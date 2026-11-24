"""Tests for the development-only LOCAL Spark platform."""

import json
import logging
import os
import runpy
import signal
import socket
import subprocess
import sys
import time
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

    def test_rejects_invalid_environment_variable_names(self):
        with pytest.raises(ValueError, match="Invalid environment variable names"):
            build_local_command(
                _setup_info(),
                module_name="pkg.mod",
                class_name="Job",
                merged_spark_conf={},
                runner_path="/runner.py",
                python_executable="/venv/bin/python",
                spark_submit="/venv/bin/spark-submit",
                extra_spark_env_vars='{"A;touch x": "1"}',
            )

    def test_standalone_master_forwards_environment_to_executors(self):
        config = LocalSparkConfig(master="spark://h:7077", aws_profile="p")
        built = build_local_command(
            _setup_info(config),
            module_name="pkg.mod",
            class_name="Job",
            merged_spark_conf={"spark.executorEnv.AWS_PROFILE": "user"},
            runner_path="/runner.py",
            python_executable="/venv/bin/python",
            spark_submit="/venv/bin/spark-submit",
            extra_spark_env_vars='{"EXAMPLE_VAR": "v"}',
        )
        argv = built["argv"]
        names = json.loads(argv[argv.index("--executor_env_names") + 1])
        assert names == ["AWS_PROFILE", "EXAMPLE_VAR"]
        assert "spark.executorEnv.AWS_PROFILE=user" in argv
        assert not any(a.endswith("=v") or a.endswith("=p") for a in argv)

    def test_local_master_keeps_environment_out_of_argv(self):
        built = _build(_setup_info(LocalSparkConfig(aws_profile="p")))
        assert not any("executorEnv" in a for a in built["argv"])
        assert "--executor_env_names" not in built["argv"]

    def test_relative_jar_paths_become_absolute(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        info = _setup_info(spark_jar_paths=["app.jar", "s3://b/dep.jar"])
        argv = _build(info, module_name="", class_name="com.example.Main")["argv"]
        assert argv[argv.index("--jars") + 1] == "s3://b/dep.jar"
        assert argv[argv.index("--class") + 2] == str(tmp_path / "app.jar")


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

    def test_local_iceberg_override_skips_invalid_shared_config(self):
        from overture_airflow_provider.config import IcebergConfig
        from overture_airflow_provider.iceberg import resolve_iceberg_spark_config

        resolved = resolve_iceberg_spark_config(
            IcebergConfig(spark_config="not json"),
            SparkFamily.LOCAL,
            local_override={"spark.sql.catalog.dev": "hadoop"},
        )
        assert resolved == {"spark.sql.catalog.dev": "hadoop"}

    def test_local_iceberg_config_replaces_shared_one(self):
        config = LocalSparkConfig(iceberg_spark_config='{"spark.sql.catalog.dev": "hadoop"}')
        merged = self._merged(config, {"spark.sql.catalog.rest": "x"})
        assert merged["spark.sql.catalog.dev"] == "hadoop"
        assert "spark.sql.catalog.rest" not in merged


def _gone(probe, timeout=5.0) -> bool:
    # An orphan is briefly a zombie until init reaps it; macOS reports EPERM for
    # a group holding only zombies.
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            probe()
        except (ProcessLookupError, PermissionError):
            return True
        time.sleep(0.05)
    return False


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

    def test_stops_job_when_supervisor_is_sigkilled(self, allow_local, tmp_path, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.5)
        info = _setup_info(LocalSparkConfig(work_dir=str(tmp_path)))
        pid_file = tmp_path / "pid"
        # The run marker must appear in the job's command line, as it does for spark-submit.
        script = (
            f"# {info['run_identifier']}\n"
            "import os, signal, time\n"
            f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
            "os.kill(os.getppid(), signal.SIGKILL)\n"
            "time.sleep(60)\n"
        )
        with pytest.raises(LocalSparkJobFailed) as exc_info:
            _run(info, script)
        assert exc_info.value.returncode == -signal.SIGKILL
        job_pid = int(pid_file.read_text())
        assert _gone(lambda: os.kill(job_pid, 0))

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


class TestPackageCheck:
    def test_reports_missing_and_version_mismatch(self):
        missing = _local.find_missing_packages(
            sys.executable, "pytest pytest==0.0.0 surely-not-installed-pkg"
        )
        assert missing[0].startswith("pytest==0.0.0 (installed ")
        assert missing[1] == "surely-not-installed-pkg (not installed)"


@pytest.mark.skipif(os.name != "posix", reason="LOCAL process control requires POSIX")
class TestSupervisor:
    def test_stops_job_when_task_process_goes_away(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.5)
        pid_file = tmp_path / "pid"
        job = (
            "import os, sys, time; "
            f"open({str(pid_file)!r}, 'w').write(str(os.getpid())); "
            "print('ready', flush=True); time.sleep(60)"
        )
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", job]),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            assert proc.stdout.readline().strip() == "ready"
            proc.stdin.close()
            proc.wait(timeout=10)
            job_pid = int(pid_file.read_text())
            with pytest.raises(ProcessLookupError):
                os.kill(job_pid, 0)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait()
            proc.stdout.close()

    def test_stops_descendants_left_after_job_exits(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.5)
        pid_file = tmp_path / "pid"
        grandchild = (
            f"import os, time; open({str(pid_file)!r}, 'w').write(str(os.getpid())); time.sleep(60)"
        )
        job = (
            "import subprocess, sys, time, os; "
            f"subprocess.Popen([sys.executable, '-c', {grandchild!r}]); "
            f"[time.sleep(0.05) for _ in range(100) if not os.path.exists({str(pid_file)!r})]; "
            "print('ready', flush=True)"
        )
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", job]),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            out = proc.stdout.read()
            assert proc.wait(timeout=15) == 0
            assert "ready" in out
            grandchild_pid = int(pid_file.read_text())
            with pytest.raises(ProcessLookupError):
                os.kill(grandchild_pid, 0)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait()
            proc.stdin.close()
            proc.stdout.close()

    def test_needs_no_external_commands(self):
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", "pass"]),
            stdin=subprocess.PIPE,
            env={"PATH": "/nonexistent"},
            start_new_session=True,
        )
        try:
            assert proc.wait(timeout=10) == 0
        finally:
            proc.stdin.close()

    def test_job_never_runs_if_supervisor_dies_before_recording_group(self, tmp_path):
        sentinel = tmp_path / "ran"
        job = f"open({str(sentinel)!r}, 'w').close()"
        # A directory cannot be opened for writing, so the supervisor dies at the
        # point where it would record the group id.
        unwritable = tmp_path / "pgid-dir"
        unwritable.mkdir()
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", job], str(unwritable)),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            start_new_session=True,
        )
        try:
            assert proc.wait(timeout=10) != 0
        finally:
            proc.stdin.close()
            proc.stdout.close()
        time.sleep(0.5)
        assert not sentinel.exists()

    def test_returns_job_exit_code(self):
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", "import sys; sys.exit(3)"]),
            stdin=subprocess.PIPE,
            start_new_session=True,
        )
        try:
            assert proc.wait(timeout=10) == 3
        finally:
            proc.stdin.close()


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

    def test_leaves_unverified_group_of_dead_supervisor_alone(self):
        # Once the recorded pid is gone its group id may belong to anything,
        # so nothing in it is signalled.
        leader = subprocess.Popen(
            [
                sys.executable,
                "-c",
                "import subprocess, sys; "
                "p = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); "
                "print(p.pid, flush=True)",
            ],
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        survivor = int(leader.stdout.readline())
        leader.wait()
        leader.stdout.close()
        try:
            cancel_local_run(
                "r", {"pid": leader.pid, "hostname": socket.gethostname(), "marker": "m"}
            )
            os.kill(survivor, 0)
        finally:
            try:
                os.kill(survivor, 9)
            except ProcessLookupError:
                pass

    def test_live_supervisor_stops_job_group_on_cancel(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.5)
        marker = "local-cancel-supervised-marker"
        pid_file = tmp_path / "pid"
        job = (
            "import os, time; "
            f"open({str(pid_file)!r}, 'w').write(str(os.getpid())); "
            "print('ready', flush=True); time.sleep(60)"
        )
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", job, marker]),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        try:
            assert proc.stdout.readline().strip() == "ready"
            cancel_local_run(
                "r", {"pid": proc.pid, "hostname": socket.gethostname(), "marker": marker}
            )
            proc.wait(timeout=10)
            with pytest.raises(ProcessLookupError):
                os.kill(int(pid_file.read_text()), 0)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait()
            proc.stdin.close()
            proc.stdout.close()

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

    def test_stops_job_left_by_a_sigkilled_supervisor(self, tmp_path, monkeypatch):
        monkeypatch.setattr(_local, "_KILL_GRACE_SECONDS", 0.5)
        marker = "local-cancel-orphan-marker"
        pgid_file = tmp_path / "job.pgid"
        job = "import time; print('ready', flush=True); time.sleep(60)"
        proc = subprocess.Popen(
            _local.supervised_argv([sys.executable, "-c", job, marker], str(pgid_file)),
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        job_pgid = None
        try:
            assert proc.stdout.readline().strip() == "ready"
            job_pgid = int(pgid_file.read_text())
            os.killpg(proc.pid, signal.SIGKILL)
            proc.wait(timeout=10)
            os.killpg(job_pgid, 0)
            cancel_local_run(
                "r",
                {
                    "pid": proc.pid,
                    "hostname": socket.gethostname(),
                    "marker": marker,
                    "job_pgid_file": str(pgid_file),
                },
            )
            assert _gone(lambda: os.killpg(job_pgid, 0))
        finally:
            if job_pgid:
                try:
                    os.killpg(job_pgid, signal.SIGKILL)
                except OSError:
                    pass
            proc.stdin.close()
            proc.stdout.close()

    def test_leaves_job_group_without_marker_alone(self, tmp_path):
        pgid_file = tmp_path / "job.pgid"
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(60)"], start_new_session=True
        )
        pgid_file.write_text(str(proc.pid))
        try:
            _local._stop_orphaned_job(str(pgid_file), "not-this-run")
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


def test_render_quotes_environment_values(deny_local):
    result = render_spark_job(
        spark_impl_name="LOCAL_v3_5",
        module_name="pkg.mod",
        class_name="Job",
        parameters={},
        extra_spark_env_vars='{"EXAMPLE_VAR": "a $(touch x)"}',
    )
    assert result.cli[-1].startswith("env ")
    assert "'EXAMPLE_VAR=a $(touch x)'" in result.cli[-1]


def test_render_local_override_ignores_invalid_shared_iceberg_config(deny_local):
    from overture_airflow_provider.config import IcebergConfig

    result = render_spark_job(
        spark_impl_name="LOCAL_v3_5",
        module_name="pkg.mod",
        class_name="Job",
        parameters={},
        iceberg_config=IcebergConfig(spark_config="not json"),
        local_spark_config=LocalSparkConfig(
            iceberg_spark_config='{"spark.sql.catalog.dev": "hadoop"}'
        ),
    )
    assert "spark.sql.catalog.dev=hadoop" in result.operator_kwargs["argv"]


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


def _run_runner(monkeypatch, job_cls, *, connect, extra_argv=()):
    sql = types.ModuleType("pyspark.sql")
    sql.SparkSession = MagicMock()
    job = types.ModuleType("local_test_job")
    job.Job = job_cls
    if not hasattr(sys.modules.get("pyspark"), "SparkContext"):
        pyspark = types.ModuleType("pyspark")
        pyspark.SparkContext = MagicMock()
        monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "local_test_job", job)
    if connect:
        monkeypatch.setenv("SPARK_REMOTE", "sc://localhost:15002")
    else:
        monkeypatch.delenv("SPARK_REMOTE", raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "job_runner_local.py",
            "--module_name",
            "local_test_job",
            "--class_name",
            "Job",
            *extra_argv,
        ],
    )
    runpy.run_path(str(get_runner_path("local")))
    return sql.SparkSession


class _PlatformJob:
    calls: list = []

    def init_spark_for_platform(self, extra_spark_conf):
        self.calls.append("init")

    def run(self, params):
        return None


def test_local_runner_keeps_submitted_master_and_lets_job_build_session(monkeypatch):
    _PlatformJob.calls = []
    session = _run_runner(monkeypatch, _PlatformJob, connect=False)
    session.builder.getOrCreate.assert_not_called()
    builder = object()
    assert session.Builder.master(builder, "local[*]") is builder
    assert _PlatformJob.calls == ["init"]


def test_local_runner_sets_executor_env_as_jvm_properties(monkeypatch):
    _PlatformJob.calls = []
    props = {"spark.executorEnv.KEEP": "user"}
    system = MagicMock()
    system.getProperty.side_effect = props.get
    system.setProperty.side_effect = props.__setitem__
    pyspark = types.ModuleType("pyspark")
    pyspark.SparkContext = MagicMock()
    pyspark.SparkContext._jvm.java.lang.System = system
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setenv("SECRET_VAR", "s3cr3t")
    monkeypatch.setenv("KEEP", "env")
    _run_runner(
        monkeypatch,
        _PlatformJob,
        connect=False,
        extra_argv=["--executor_env_names", '["SECRET_VAR", "KEEP", "UNSET_VAR"]'],
    )
    assert props == {"spark.executorEnv.KEEP": "user", "spark.executorEnv.SECRET_VAR": "s3cr3t"}


def test_local_runner_safeguards_apply_before_job_construction(monkeypatch):
    props: dict = {}
    system = MagicMock()
    system.getProperty.side_effect = props.get
    system.setProperty.side_effect = props.__setitem__
    pyspark = types.ModuleType("pyspark")
    pyspark.SparkContext = MagicMock()
    pyspark.SparkContext._jvm.java.lang.System = system
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setenv("SECRET_VAR", "s3cr3t")
    seen = {}

    class ConstructorJob:
        def __init__(self):
            builder = object()
            master = sys.modules["pyspark.sql"].SparkSession.Builder.master
            seen["master_kept"] = master(builder, "local[*]") is builder
            seen["props"] = dict(props)

        def run(self, params):
            return None

    _run_runner(
        monkeypatch,
        ConstructorJob,
        connect=False,
        extra_argv=["--executor_env_names", '["SECRET_VAR"]'],
    )
    assert seen == {
        "master_kept": True,
        "props": {"spark.executorEnv.SECRET_VAR": "s3cr3t"},
    }


def test_local_runner_rejects_platform_jobs_over_spark_connect(monkeypatch):
    with pytest.raises(RuntimeError, match="Spark Connect"):
        _run_runner(monkeypatch, _PlatformJob, connect=True)


@pytest.mark.parametrize(
    ("impl", "config", "expected"),
    [
        ("LOCAL_v3_5", LocalSparkConfig(iceberg_spark_config='{"k": "v"}'), True),
        ("LOCAL_v3_5", LocalSparkConfig(), False),
        ("LOCAL_v3_5", None, False),
        ("GLUE_v5", LocalSparkConfig(iceberg_spark_config='{"k": "v"}'), False),
        ("{{ params.impl }}", LocalSparkConfig(iceberg_spark_config='{"k": "v"}'), None),
        ("{{ params.impl }}", LocalSparkConfig(), False),
    ],
)
def test_local_iceberg_override_active(impl, config, expected):
    from overture_airflow_provider.spark_agnostic_taskgroup import _local_iceberg_override_active

    assert _local_iceberg_override_active(impl, config) is expected
