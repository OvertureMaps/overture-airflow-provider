"""Tests for the development-only LOCAL Spark platform."""

import json
import socket
import subprocess
import sys
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
from overture_airflow_provider.render import render_spark_job
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
    with patch("overture_airflow_provider._setup.CodeArtifactPyPiClient"):
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

    def test_setup_detects_hadoop_version(self, allow_local):
        detected = {"spark_version": "3.5.3", "hadoop_version": "3.3.4"}
        with patch.object(_local, "detect_local_spark", return_value=detected):
            info = _setup(local_spark_config=LocalSparkConfig(aws_profile="dev"))
        assert info["local_hadoop_version"] == "3.3.4"
        restored = rehydrate(to_xcom(info))
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
    built = {"argv": argv, "env": {}, "mode": "spark-submit", "warnings": []}
    context, ti = _context()
    with (
        patch.object(_local, "build_local_command", return_value=built),
        patch.object(_local, "resolve_spark_submit", return_value="spark-submit"),
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


class TestRunLocalJob:
    def test_success_records_run_and_returns_ui_url(self, allow_local, tmp_path):
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        result, ti = _run(info, "import os; assert os.environ['FOO'] == 'bar'; print('ok')")
        assert result["status"] == "SUCCEEDED"
        assert result["job_url"] == "http://localhost:4040"
        keys = [c.kwargs["key"] for c in ti.xcom_push.call_args_list]
        assert "spark_agnostic" in keys
        assert len(keys) == 2

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
