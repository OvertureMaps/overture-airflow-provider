"""Tests for the development-only LOCAL Spark platform."""

import json
import logging
import os
import runpy
import socket
import subprocess
import sys
import types
from unittest.mock import MagicMock, patch

import pytest

from overture_airflow_provider import _local
from overture_airflow_provider._failures import TIMEOUT
from overture_airflow_provider._local import (
    LocalSparkJobFailed,
    LocalSparkNotAllowedError,
    build_local_command,
    cancel_local_run,
    local_setup_info_fields,
    run_local_job,
)
from overture_airflow_provider._setup import setup_spark_job
from overture_airflow_provider.config import LOCAL_SPARK_ALLOW_ENV_VAR, LocalSparkConfig
from overture_airflow_provider.render import _cli, render_spark_job
from overture_airflow_provider.runner_assets import get_runner_path
from overture_airflow_provider.setup_info import rehydrate, to_xcom
from overture_airflow_provider.spark import SparkFamily, SparkImpl
from overture_airflow_provider.spark_platform_handlers import (
    LocalPlatformHandler,
    get_platform_handler,
)


def _setup_info(config=None, **overrides):
    impl = SparkImpl.LOCAL_v3_5
    info = {
        "job_name": "pkg.mod.Job",
        "spark_impl": impl,
        "spark_impl_name": "LOCAL_v3_5",
        "spark_family": SparkFamily.LOCAL,
        "spark_family_name": "LOCAL",
        "spark_version": impl.get_spark_version(),
        "scala_version": impl.get_scala_version(),
        "sedona_version": "1.7.0",
        "run_identifier": "pkg.mod.Job_20260101000000",
        "parameters": '{"--input": "/tmp/in", "--output": "/tmp/out"}',
        "spark_jar_paths": [],
        **local_setup_info_fields(config or LocalSparkConfig(), hadoop_version="3.3.4"),
    }
    info.update(overrides)
    return info


def _build(setup_info, module_name="pkg.mod", class_name="Job", conf=None):
    return build_local_command(
        setup_info,
        module_name=module_name,
        class_name=class_name,
        merged_spark_conf=conf or {"spark.a": 1, "fs.s3a.x": "y"},
        runner_path="/runner.py",
        python_executable="/venv/bin/python",
        spark_submit="/venv/bin/spark-submit",
    )


@pytest.fixture
def allow_local(monkeypatch):
    monkeypatch.setenv(LOCAL_SPARK_ALLOW_ENV_VAR, "1")


@pytest.fixture
def deny_local(monkeypatch):
    monkeypatch.delenv(LOCAL_SPARK_ALLOW_ENV_VAR, raising=False)


def _setup(**kwargs):
    return setup_spark_job(
        spark_impl_name="LOCAL_v3_5",
        sedona_version="1.7.0",
        module_name="pkg.mod",
        class_name="Job",
        job_name="",
        parameters="{}",
        spark_jar_paths="",
        **kwargs,
    )


class TestConfig:
    def test_rejects_unknown_master(self):
        with pytest.raises(ValueError, match="master"):
            LocalSparkConfig(master="yarn")

    @pytest.mark.parametrize("master", ["local[*]", "local[4]", "spark://h:7077", "sc://h:15002"])
    def test_accepts_supported_masters(self, master):
        assert LocalSparkConfig(master=master).master == master


class TestSetupGuard:
    def test_setup_refuses_without_opt_in(self, deny_local):
        with pytest.raises(LocalSparkNotAllowedError, match=LOCAL_SPARK_ALLOW_ENV_VAR):
            _setup()

    def test_setup_refuses_non_posix_workers(self, allow_local):
        with patch.object(_local.os, "name", "nt"), pytest.raises(RuntimeError, match="POSIX"):
            _local.require_local_spark_allowed()

    def test_setup_detects_hadoop_version(self, allow_local):
        detected = {"spark_version": "3.5.3", "hadoop_version": "3.3.4"}
        with (
            patch.object(_local, "inspect_local_spark", return_value=detected),
            patch("overture_airflow_provider._setup.CodeArtifactPyPiClient") as setup_client,
        ):
            info = _setup(local_spark_config=LocalSparkConfig(aws_profile="dev"))
        setup_client.assert_not_called()
        assert info["local_hadoop_version"] == "3.3.4"
        with patch("overture_core.cloud.aws.codeartifact.CodeArtifactPyPiClient") as xcom_client:
            restored = rehydrate(to_xcom(info))
        xcom_client.assert_not_called()
        assert restored["py_pi_client"] is None
        assert restored["spark_family"] == SparkFamily.LOCAL
        assert restored["local_aws_profile"] == "dev"
        assert restored["local_hadoop_version"] == "3.3.4"

    def test_setup_rejects_spark_version_mismatch(self, allow_local):
        detected = {"spark_version": "4.0.0", "hadoop_version": "3.4.1"}
        with (
            patch.object(_local, "detect_local_spark", return_value=detected),
            pytest.raises(ValueError, match="does not match"),
        ):
            _setup()

    def test_other_platforms_skip_the_guard(self, deny_local):
        with patch("overture_airflow_provider._setup.CodeArtifactPyPiClient"):
            info = setup_spark_job(
                spark_impl_name="GLUE_v5",
                sedona_version="1.7.0",
                module_name="m",
                class_name="C",
                job_name="",
                parameters="{}",
                spark_jar_paths="",
            )
        assert info["local_hadoop_version"] == ""


class TestInstalledSpark:
    def test_accepts_matching_spark_submit_version(self):
        detected = {"spark_version": "3.5.2", "hadoop_version": "3.3.4"}
        with (
            patch.object(_local, "detect_local_spark", return_value=detected),
            patch.object(_local, "resolve_spark_submit", return_value="/spark-submit"),
            patch.object(
                subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, "", "Spark version 3.5.3"),
            ) as run,
        ):
            result = _local.inspect_local_spark(
                python_executable="/python",
                spark_submit_path="",
                master="local[*]",
                expected_version="3.5.0",
                enable_s3a=True,
            )
        assert result["spark_submit"] == "/spark-submit"
        assert result["hadoop_version"] == "3.3.4"
        assert run.call_args.args[0] == ["/spark-submit", "--version"]

    def test_checks_spark_submit_version(self):
        with (
            patch.object(_local, "detect_local_spark", return_value={"spark_version": "3.5.2"}),
            patch.object(_local, "resolve_spark_submit", return_value="/spark-submit"),
            patch.object(
                subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, "", "Spark version 4.0.0"),
            ),
            pytest.raises(ValueError, match="spark-submit 4.0.0"),
        ):
            _local.inspect_local_spark(
                python_executable="/python",
                spark_submit_path="",
                master="local[*]",
                expected_version="3.5.0",
                enable_s3a=False,
            )

    def test_fails_when_s3a_hadoop_version_is_missing(self):
        with (
            patch.object(_local, "detect_local_spark", return_value={"spark_version": "3.5.2"}),
            patch.object(_local, "resolve_spark_submit", return_value="/spark-submit"),
            patch.object(
                subprocess,
                "run",
                return_value=subprocess.CompletedProcess([], 0, "", "Spark version 3.5.2"),
            ),
            pytest.raises(RuntimeError, match="Hadoop version"),
        ):
            _local.inspect_local_spark(
                python_executable="/python",
                spark_submit_path="",
                master="local[*]",
                expected_version="3.5.0",
                enable_s3a=True,
            )

    def test_connect_does_not_require_spark_submit_or_hadoop(self):
        with (
            patch.object(
                _local,
                "detect_local_spark",
                return_value={"spark_version": "3.5.2", "hadoop_version": None},
            ),
            patch.object(_local, "resolve_spark_submit") as resolve,
        ):
            _local.inspect_local_spark(
                python_executable="/python",
                spark_submit_path="",
                master="sc://localhost:15002",
                expected_version="3.5.0",
                enable_s3a=True,
            )
        resolve.assert_not_called()


class TestBuildCommand:
    def test_pyspark_spark_submit(self):
        built = _build(_setup_info(LocalSparkConfig(extra_packages=["g:a:1"], aws_profile="p")))
        argv = built["argv"]
        assert built["mode"] == "spark-submit"
        assert argv[:3] == ["/venv/bin/spark-submit", "--master", "local[*]"]
        packages = argv[argv.index("--packages") + 1].split(",")
        assert "org.apache.hadoop:hadoop-aws:3.3.4" in packages
        assert "g:a:1" in packages
        assert any(p.startswith("org.apache.sedona:sedona-spark-shaded-3.5_2.12") for p in packages)
        assert "spark.a=1" in argv
        assert not any(a.startswith("fs.s3a.x") for a in argv)
        runner_args = argv[argv.index("/runner.py") + 1 :]
        assert runner_args[:4] == ["--module_name", "pkg.mod", "--class_name", "Job"]
        assert json.loads(runner_args[-1]) == {"spark.a": 1, "fs.s3a.x": "y"}
        assert built["env"]["PYSPARK_PYTHON"] == "/venv/bin/python"
        assert built["env"]["AWS_PROFILE"] == "p"

    def test_s3a_disabled_omits_hadoop_aws(self):
        built = _build(_setup_info(LocalSparkConfig(enable_s3a=False)))
        assert "hadoop-aws" not in " ".join(built["argv"])

    def test_scala_uses_first_jar_as_app_jar(self):
        info = _setup_info(spark_jar_paths=["/app.jar", "/dep.jar"])
        argv = _build(info, module_name="", class_name="com.example.Main")["argv"]
        assert argv[argv.index("--jars") + 1] == "/dep.jar"
        i = argv.index("--class")
        assert argv[i + 1 : i + 3] == ["com.example.Main", "/app.jar"]
        assert argv[i + 3 : i + 7] == ["--input", "/tmp/in", "--output", "/tmp/out"]
        assert "--extraSparkConf" in argv

    def test_scala_without_jar_fails(self):
        with pytest.raises(ValueError, match="application JAR"):
            _build(_setup_info(), module_name="", class_name="com.example.Main")

    def test_spark_connect_runs_runner_with_python(self):
        built = _build(_setup_info(LocalSparkConfig(master="sc://localhost:15002")))
        assert built["mode"] == "spark-connect"
        assert built["argv"][:2] == ["/venv/bin/python", "/runner.py"]
        assert "--packages" not in built["argv"]
        assert built["env"]["SPARK_REMOTE"] == "sc://localhost:15002"
        assert built["warnings"]

    def test_spark_connect_rejects_scala(self):
        info = _setup_info(LocalSparkConfig(master="sc://h:1"), spark_jar_paths=["/app.jar"])
        with pytest.raises(ValueError, match="Spark Connect"):
            _build(info, module_name="", class_name="com.example.Main")


class TestSetupCluster:
    def _merged(self, config, iceberg):
        handler = get_platform_handler(SparkFamily.LOCAL, _setup_info(config))
        return handler.setup_cluster("", "", {"spark.user": "u"}, "{}", "", "", iceberg)[
            "merged_spark_conf"
        ]

    def test_reuses_iceberg_config_by_default(self):
        merged = self._merged(LocalSparkConfig(), {"spark.sql.catalog.rest": "x"})
        assert merged["spark.sql.catalog.rest"] == "x"
        assert merged["spark.hadoop.fs.s3.impl"].endswith("S3AFileSystem")
        assert merged["spark.user"] == "u"
        assert "sedona.join.numpartition" not in merged

    def test_local_iceberg_config_replaces_shared_one(self):
        config = LocalSparkConfig(iceberg_spark_config='{"spark.sql.catalog.dev": "hadoop"}')
        merged = self._merged(config, {"spark.sql.catalog.rest": "x"})
        assert merged["spark.sql.catalog.dev"] == "hadoop"
        assert "spark.sql.catalog.rest" not in merged


def _context():
    ti = MagicMock()
    return {"ti": ti}, ti


def _run(setup_info, script, max_timeout_hours=1):
    argv = [sys.executable, "-c", script]
    built = {"argv": argv, "env": {"FOO": "bar"}, "mode": "spark-submit", "warnings": []}
    context, ti = _context()
    with (
        patch.object(_local, "build_local_command", return_value=built),
        patch.object(
            _local,
            "inspect_local_spark",
            return_value={"hadoop_version": "3.3.4", "spark_submit": "spark-submit"},
        ),
    ):
        result = run_local_job(
            setup_info=setup_info,
            module_name="pkg.mod",
            class_name="Job",
            merged_spark_conf={},
            extra_spark_env_vars='{"FOO": "bar"}',
            max_timeout_hours=max_timeout_hours,
            context=context,
        )
    return result, ti


@pytest.mark.skipif(os.name != "posix", reason="LOCAL process control requires POSIX")
class TestRunLocalJob:
    def test_success_records_run_and_returns_ui_url(self, allow_local, tmp_path):
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        result, ti = _run(info, "import os; assert os.environ['FOO'] == 'bar'; print('ok')")
        assert result["status"] == "SUCCEEDED"
        assert result["job_url"] == "http://localhost:4040"
        keys = [c.kwargs["key"] for c in ti.xcom_push.call_args_list]
        assert "spark_agnostic" in keys
        assert len(keys) == 2

    def test_local_job_does_not_inherit_spark_remote(self, allow_local, tmp_path, monkeypatch):
        monkeypatch.setenv("SPARK_REMOTE", "sc://elsewhere:15002")
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        _run(info, "import os; assert 'SPARK_REMOTE' not in os.environ")

    def test_failure_carries_log_tail(self, allow_local, tmp_path):
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        script = "import sys; print('ModuleNotFoundError: No module named x'); sys.exit(3)"
        with pytest.raises(LocalSparkJobFailed) as exc_info:
            _run(info, script)
        assert exc_info.value.returncode == 3
        assert "No module named x" in exc_info.value.log_tail
        assert not exc_info.value.timed_out

    def test_timeout_kills_the_job(self, allow_local, tmp_path):
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        with pytest.raises(LocalSparkJobFailed) as exc_info:
            _run(info, "import time; time.sleep(30)", max_timeout_hours=0.5 / 3600)
        assert exc_info.value.timed_out

    def test_timeout_escalates_when_job_ignores_sigterm(self, allow_local, tmp_path, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.1)
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        script = (
            "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "print('ready', flush=True); time.sleep(30)"
        )
        with pytest.raises(LocalSparkJobFailed) as exc_info:
            _run(info, script, max_timeout_hours=0.5 / 3600)
        assert exc_info.value.timed_out
        assert "ready" in exc_info.value.log_tail

    def test_job_command_is_not_logged(self, allow_local, tmp_path, caplog):
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        with caplog.at_level(logging.INFO):
            _run(info, "# secret-value\nprint('ok')")
        assert "secret-value" not in caplog.text

    def test_submit_refuses_without_opt_in(self, deny_local):
        with pytest.raises(LocalSparkNotAllowedError):
            _run(_setup_info(), "print('never')")


class TestDescribeFailure:
    def test_timeout_and_hint(self):
        handler = LocalPlatformHandler(_setup_info())
        error = LocalSparkJobFailed(
            None, "ModuleNotFoundError: No module named 'pkg'", timed_out=True, run_id="local-1"
        )
        info = handler.describe_failure(error=error, run_launched=True)
        assert info.state == TIMEOUT
        assert info.run_id == "local-1"
        assert info.console_url == "http://localhost:4040"
        assert "No module named" in info.root_cause
        assert "python_executable" in info.hint


@pytest.mark.skipif(os.name != "posix", reason="LOCAL process control requires POSIX")
class TestCancel:
    def test_refuses_other_host(self):
        with pytest.raises(RuntimeError, match="only be cancelled there"):
            cancel_local_run("r", {"pid": 123, "hostname": "elsewhere"})

    def test_kills_matching_process_group(self):
        marker = "local-cancel-test-marker"
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)", marker],
            start_new_session=True,
        )
        try:
            cancel_local_run(
                "r", {"pid": proc.pid, "hostname": socket.gethostname(), "marker": marker}
            )
            assert proc.wait(timeout=10) != 0
        finally:
            if proc.poll() is None:
                proc.kill()

    def test_skips_process_without_marker(self):
        proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
        try:
            cancel_local_run(
                "r", {"pid": proc.pid, "hostname": socket.gethostname(), "marker": "nope"}
            )
            assert proc.poll() is None
        finally:
            proc.kill()
            proc.wait()

    def test_cancel_escalates_when_job_ignores_sigterm(self, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.1)
        marker = "local-cancel-ignore-term"
        script = (
            "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
            "print('ready', flush=True); time.sleep(60)"
        )
        proc = subprocess.Popen(
            [sys.executable, "-c", script, marker],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            assert proc.stdout.readline().strip() == "ready"
            cancel_local_run(
                "r", {"pid": proc.pid, "hostname": socket.gethostname(), "marker": marker}
            )
            assert proc.wait(timeout=5) != 0
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.wait()
            proc.stdout.close()


def test_render_local_needs_no_opt_in(deny_local):
    result = render_spark_job(
        spark_impl_name="LOCAL_v3_5",
        module_name="pkg.mod",
        class_name="Job",
        parameters={},
    )
    assert result.platform == "local"
    assert result.operator_kwargs["argv"][0] == "spark-submit"
    assert any("spark-submit" in line for line in result.cli)


def test_render_local_includes_extra_environment(deny_local):
    result = render_spark_job(
        spark_impl_name="LOCAL_v3_5",
        module_name="pkg.mod",
        class_name="Job",
        parameters={},
        extra_spark_env_vars='{"EXAMPLE_VAR": "value"}',
    )
    assert result.operator_kwargs["env"]["EXAMPLE_VAR"] == "value"
    assert "EXAMPLE_VAR=value" in result.cli[-1]


def test_render_cli_passes_extra_environment(deny_local, tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text(
        json.dumps(
            {
                "spark_impl_name": "LOCAL_v3_5",
                "module_name": "pkg.mod",
                "class_name": "Job",
                "extra_spark_env_vars": {"EXAMPLE_VAR": "value"},
            }
        )
    )
    assert _cli(["--config", str(config)]) == 0
    result = json.loads(capsys.readouterr().out)
    assert result["operator_kwargs"]["env"]["EXAMPLE_VAR"] == "value"


def test_local_runner_does_not_silence_unexpected_conf_errors(monkeypatch):
    spark = MagicMock()
    spark.conf.set.side_effect = ValueError("invalid setting")

    class Job:
        def run(self, params, spark):
            return None

    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()
    sql.SparkSession.builder.getOrCreate.return_value = spark
    job = types.ModuleType("local_test_job")
    job.Job = Job
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "local_test_job", job)
    monkeypatch.setenv("SPARK_REMOTE", "sc://localhost:15002")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "job_runner_local.py",
            "--module_name",
            "local_test_job",
            "--class_name",
            "Job",
            "--extra_spark_conf",
            '{"spark.invalid": "x"}',
        ],
    )
    with pytest.raises(ValueError, match="invalid setting"):
        runpy.run_path(str(get_runner_path("local")))
