"""Development-only ``LOCAL`` platform: run Spark jobs on a local Spark install.

Every entry point that starts work calls :func:`require_local_spark_allowed`,
so the platform refuses to run unless the worker opts in through
``OVERTURE_AIRFLOW_PROVIDER_ALLOW_LOCAL_SPARK=1``. Command building is pure so
``render`` can preview it without Airflow, pyspark or the opt-in.
"""

import collections
import datetime
import json
import logging
import os
import pathlib
import re
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time

from overture_airflow_provider.config import LOCAL_SPARK_ALLOW_ENV_VAR, coerce_config_dict
from overture_airflow_provider.spark import SparkSedona

log = logging.getLogger(__name__)

_LOG_TAIL_LINES = 200
_KILL_GRACE_SECONDS = 10
_RUNNER_MARKER = "job_runner_local.py"

_DETECT_SCRIPT = """
import json, pathlib, pyspark
jars = pathlib.Path(pyspark.__file__).parent / "jars"
hadoop = None
for p in sorted(jars.glob("hadoop-client-api-*.jar")):
    hadoop = p.name[len("hadoop-client-api-"):-len(".jar")]
print(json.dumps({"spark_version": pyspark.__version__, "hadoop_version": hadoop}))
"""

_MISSING_PACKAGES_SCRIPT = """
import importlib.metadata as m, json, sys
missing = []
for name in sys.argv[1:]:
    try:
        m.version(name)
    except m.PackageNotFoundError:
        missing.append(name)
print(json.dumps(missing))
"""

S3A_SPARK_CONF = {
    "spark.hadoop.fs.s3.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider": (
        "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    ),
}


class LocalSparkNotAllowedError(RuntimeError):
    """The worker has not opted in to the development-only ``LOCAL`` platform."""


class LocalSparkJobFailed(RuntimeError):
    """A local Spark job process exited non-zero or hit ``max_timeout_hours``."""

    def __init__(self, returncode, log_tail: str, *, timed_out: bool = False, run_id: str = ""):
        self.returncode = returncode
        self.log_tail = log_tail
        self.timed_out = timed_out
        self.run_id = run_id
        what = "timed out" if timed_out else f"exited with code {returncode}"
        super().__init__(
            f"Local Spark job {run_id} {what}" if run_id else f"Local Spark job {what}"
        )


def local_setup_info_fields(config, hadoop_version: str = "") -> dict:
    """Flatten a ``LocalSparkConfig`` into the ``local_*`` setup_info fields."""
    return {
        "local_master": config.master,
        "local_python_executable": config.python_executable,
        "local_spark_submit_path": config.spark_submit_path,
        "local_driver_memory": config.driver_memory,
        "local_enable_s3a": config.enable_s3a,
        "local_aws_profile": config.aws_profile,
        "local_iceberg_spark_config": coerce_config_dict(
            config.iceberg_spark_config, field_name="LocalSparkConfig.iceberg_spark_config"
        ),
        "local_extra_packages": list(config.extra_packages),
        "local_ui_url": config.ui_url,
        "local_work_dir": config.work_dir,
        "local_hadoop_version": hadoop_version,
    }


def require_local_spark_allowed() -> None:
    """Enforce the development opt-in and supported worker platform."""
    if os.environ.get(LOCAL_SPARK_ALLOW_ENV_VAR) != "1":
        raise LocalSparkNotAllowedError(
            f"The LOCAL Spark platform is for development only and is disabled on this "
            f"worker. Set {LOCAL_SPARK_ALLOW_ENV_VAR}=1 in the worker environment to use it."
        )
    if os.name != "posix":
        raise RuntimeError(
            "LOCAL Spark execution requires a POSIX Airflow worker (Linux or macOS)."
        )


def resolve_python_executable(python_executable: str) -> str:
    executable = python_executable or sys.executable
    if os.path.dirname(executable):
        return str(pathlib.Path(executable).absolute())
    return shutil.which(executable) or executable


def resolve_spark_submit(python_executable: str, spark_submit_path: str) -> str:
    """``spark_submit_path``, else the pyspark wheel's script next to the interpreter, else PATH."""
    if spark_submit_path:
        if os.path.dirname(spark_submit_path):
            return str(pathlib.Path(spark_submit_path).absolute())
        return shutil.which(spark_submit_path) or spark_submit_path
    candidate = pathlib.Path(python_executable).parent / "spark-submit"
    if candidate.exists():
        return str(candidate)
    found = shutil.which("spark-submit")
    if found:
        return found
    raise FileNotFoundError(
        f"spark-submit not found next to {python_executable} or on PATH. "
        "Install pyspark into that interpreter or set LocalSparkConfig.spark_submit_path."
    )


def detect_local_spark(python_executable: str) -> dict:
    """Return ``{"spark_version", "hadoop_version"}`` of pyspark in ``python_executable``."""
    proc = subprocess.run(
        [python_executable, "-c", _DETECT_SCRIPT],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(
            f"pyspark is not importable with {python_executable}: "
            f"{(proc.stderr or proc.stdout).strip()[-2000:]}"
        )
    return json.loads(proc.stdout.strip().splitlines()[-1])


def check_spark_version(expected: str, detected: str, *, component: str = "pyspark") -> None:
    """Raise when an installed Spark component's major.minor differs from the SparkImpl's."""
    if expected.split(".")[:2] != detected.split(".")[:2]:
        raise ValueError(
            f"Installed {component} {detected} does not match the LOCAL SparkImpl's Spark "
            f"{expected} (major.minor must match)."
        )


def inspect_local_spark(
    *,
    python_executable: str,
    spark_submit_path: str,
    master: str,
    expected_version: str,
    enable_s3a: bool,
) -> dict:
    """Validate the interpreter and, for spark-submit, the actual Spark executable."""
    detected = detect_local_spark(python_executable)
    check_spark_version(expected_version, detected["spark_version"])
    if is_spark_connect(master):
        return detected

    spark_submit = resolve_spark_submit(python_executable, spark_submit_path)
    proc = subprocess.run(
        [spark_submit, "--version"], capture_output=True, text=True, timeout=120, check=False
    )
    output = proc.stdout + "\n" + proc.stderr
    match = re.search(r"\bversion\s+(\d+\.\d+(?:\.\d+)?)", output, re.IGNORECASE)
    if proc.returncode != 0 or match is None:
        raise RuntimeError(
            f"Could not determine Spark version from {spark_submit}: {output[-2000:]}"
        )
    check_spark_version(expected_version, match.group(1), component="spark-submit")
    if enable_s3a and not detected.get("hadoop_version"):
        raise RuntimeError(
            f"Could not determine the Hadoop version in {python_executable}; "
            "install a complete PySpark distribution or disable LocalSparkConfig.enable_s3a."
        )
    detected["spark_submit"] = spark_submit
    return detected


def _requirement_name(requirement: str) -> str:
    return re.split(r"[<>=!~\[;@ ]", requirement.strip(), maxsplit=1)[0]


def find_missing_packages(python_executable: str, python_packages: str) -> list[str]:
    """Names from ``python_packages`` not installed in ``python_executable``."""
    names = [n for n in (_requirement_name(r) for r in python_packages.split()) if n]
    if not names:
        return []
    proc = subprocess.run(
        [python_executable, "-c", _MISSING_PACKAGES_SCRIPT, *names],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(f"Could not inspect packages in {python_executable}: {proc.stderr}")
    return json.loads(proc.stdout.strip().splitlines()[-1])


def is_spark_connect(master: str) -> bool:
    return master.startswith("sc://")


def local_maven_packages(setup_info: dict) -> list[str]:
    """Sedona, ``hadoop-aws`` (when S3A is on) and caller-supplied coordinates."""
    packages = list(
        SparkSedona.getSedonaJarPackages(
            setup_info["sedona_version"],
            setup_info["spark_version"],
            setup_info["scala_version"],
        )
    )
    hadoop_version = setup_info.get("local_hadoop_version")
    if setup_info.get("local_enable_s3a", True) and hadoop_version:
        packages.append(f"org.apache.hadoop:hadoop-aws:{hadoop_version}")
    packages.extend(setup_info.get("local_extra_packages") or [])
    return packages


def _conf_value(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _parameters_str(parameters) -> str:
    return parameters if isinstance(parameters, str) else json.dumps(parameters)


def _scala_args(parameters, merged_spark_conf: dict) -> list[str]:
    """Glue's Scala argument contract: ``--key value`` params plus ``--extraSparkConf``."""
    parsed = json.loads(parameters) if isinstance(parameters, str) else parameters
    args: list[str] = []
    if isinstance(parsed, dict):
        for k, v in parsed.items():
            if v is not None and v != "":
                args.extend([k, v if isinstance(v, str) else json.dumps(v)])
    else:
        args.extend(["--params", json.dumps(parsed) if parsed is not None else ""])
    args.extend(["--extraSparkConf", json.dumps(merged_spark_conf)])
    return args


def build_local_command(
    setup_info: dict,
    *,
    module_name: str,
    class_name: str,
    merged_spark_conf: dict,
    runner_path: str,
    python_executable: str,
    spark_submit: str,
    extra_spark_env_vars: str = "{}",
) -> dict:
    """Build the job command without touching the environment.

    Returns ``{"argv", "env", "mode", "warnings"}`` where ``env`` holds only the
    variables to add to the worker's environment and ``mode`` is
    ``"spark-submit"`` or ``"spark-connect"``.
    """
    master = setup_info.get("local_master") or "local[*]"
    jar_paths = [j for j in setup_info.get("spark_jar_paths") or [] if j]
    parameters = setup_info["parameters"]
    env = {
        "PYSPARK_PYTHON": python_executable,
        "PYSPARK_DRIVER_PYTHON": python_executable,
    }
    if setup_info.get("local_aws_profile"):
        env["AWS_PROFILE"] = setup_info["local_aws_profile"]
    env.update(
        {
            k: _conf_value(v)
            for k, v in coerce_config_dict(
                extra_spark_env_vars or "{}", field_name="extra_spark_env_vars"
            ).items()
        }
    )
    warnings: list[str] = []

    if is_spark_connect(master):
        if not module_name:
            raise ValueError("Scala jobs cannot run over Spark Connect (sc://); use local[*].")
        warnings.append(
            "Spark Connect: --packages/--jars and static Spark confs are not applied; "
            "the server must already have Sedona and any catalogs configured."
        )
        env["SPARK_REMOTE"] = master
        argv = [
            python_executable,
            runner_path,
            "--module_name",
            module_name,
            "--class_name",
            class_name,
            "--params",
            _parameters_str(parameters),
            "--extra_spark_conf",
            json.dumps(merged_spark_conf),
        ]
        return {"argv": argv, "env": env, "mode": "spark-connect", "warnings": warnings}

    argv = [
        spark_submit,
        "--master",
        master,
        "--name",
        setup_info.get("run_identifier") or setup_info.get("job_name") or "local-spark-job",
        "--driver-memory",
        setup_info.get("local_driver_memory") or "4g",
    ]
    packages = local_maven_packages(setup_info)
    if packages:
        argv.extend(["--packages", ",".join(packages)])
    for k, v in merged_spark_conf.items():
        # spark-submit ignores non-"spark." keys; the runner applies them instead.
        if k.startswith("spark."):
            argv.extend(["--conf", f"{k}={_conf_value(v)}"])

    if module_name:
        if jar_paths:
            argv.extend(["--jars", ",".join(jar_paths)])
        argv.extend(
            [
                runner_path,
                "--module_name",
                module_name,
                "--class_name",
                class_name,
                "--params",
                _parameters_str(parameters),
                "--extra_spark_conf",
                json.dumps(merged_spark_conf),
            ]
        )
    else:
        if not jar_paths:
            raise ValueError(
                "Scala jobs need the application JAR as the first spark_jar_paths entry."
            )
        app_jar, extra_jars = jar_paths[0], jar_paths[1:]
        if extra_jars:
            argv.extend(["--jars", ",".join(extra_jars)])
        argv.extend(["--class", class_name, app_jar, *_scala_args(parameters, merged_spark_conf)])
    return {"argv": argv, "env": env, "mode": "spark-submit", "warnings": warnings}


def _kill_process_group(pid: int, sig=signal.SIGTERM) -> None:
    try:
        os.killpg(pid, sig)
    except ProcessLookupError:
        pass


def _terminate(proc: subprocess.Popen) -> None:
    if proc.poll() is not None:
        return
    _kill_process_group(proc.pid)
    try:
        proc.wait(timeout=_KILL_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        _kill_process_group(proc.pid, getattr(signal, "SIGKILL", signal.SIGTERM))
        proc.wait()


def _agnostic_xcom_payload(setup_info: dict, job_url: str) -> str:
    return json.dumps(
        {
            "spark_impl": setup_info.get("spark_impl_name"),
            "spark_family": setup_info.get(
                "spark_family_name", str(setup_info.get("spark_family", ""))
            ),
            "spark_version": setup_info.get("spark_version"),
            "sedona_version": setup_info.get("sedona_version"),
            "job_url": job_url,
            "status": "RUNNING",
        }
    )


def run_local_job(
    *,
    setup_info: dict,
    module_name: str,
    class_name: str,
    merged_spark_conf: dict,
    extra_spark_env_vars: str,
    max_timeout_hours: int,
    context,
) -> dict:
    """Run the job to completion as a child process, streaming its output to the task log."""
    from overture_airflow_provider.runner_assets import get_runner_path

    require_local_spark_allowed()
    python_executable = resolve_python_executable(setup_info.get("local_python_executable", ""))
    master = setup_info.get("local_master") or "local[*]"
    detected = inspect_local_spark(
        python_executable=python_executable,
        spark_submit_path=setup_info.get("local_spark_submit_path", ""),
        master=master,
        expected_version=setup_info["spark_version"],
        enable_s3a=setup_info.get("local_enable_s3a", True),
    )
    setup_info = {**setup_info, "local_hadoop_version": detected.get("hadoop_version") or ""}
    spark_submit = "" if is_spark_connect(master) else detected["spark_submit"]
    built = build_local_command(
        setup_info,
        module_name=module_name,
        class_name=class_name,
        merged_spark_conf=merged_spark_conf,
        runner_path=str(get_runner_path("local")),
        python_executable=python_executable,
        spark_submit=spark_submit,
        extra_spark_env_vars=extra_spark_env_vars,
    )
    for warning in built["warnings"]:
        log.warning(warning)

    env = os.environ.copy()
    if not is_spark_connect(master):
        env.pop("SPARK_REMOTE", None)
    env.update(built["env"])
    work_dir = setup_info.get("local_work_dir") or tempfile.mkdtemp(prefix="overture-local-spark-")
    ui_url = setup_info.get("local_ui_url") or ""
    return _run_process(built, env, work_dir, ui_url, setup_info, max_timeout_hours, context)


def _run_process(built, env, work_dir, ui_url, setup_info, max_timeout_hours, context) -> dict:
    from overture_airflow_provider._retry_guard import record_launched_run

    log.info("Running local Spark job (%s) in %s", built["mode"], work_dir)

    proc = subprocess.Popen(
        built["argv"],
        cwd=work_dir,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        start_new_session=True,
    )
    run_id = f"local-{proc.pid}-{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}"
    timed_out = threading.Event()
    finished = threading.Event()

    def _on_timeout():
        timed_out.set()
        _kill_process_group(proc.pid)
        if not finished.wait(_KILL_GRACE_SECONDS):
            _kill_process_group(proc.pid, getattr(signal, "SIGKILL", signal.SIGTERM))

    timer = threading.Timer(max_timeout_hours * 3600, _on_timeout)
    timer.daemon = True
    tail: collections.deque[str] = collections.deque(maxlen=_LOG_TAIL_LINES)
    try:
        record_launched_run(
            context,
            platform="local",
            run_id=run_id,
            extra={
                "pid": proc.pid,
                "hostname": socket.gethostname(),
                "marker": _process_marker(setup_info, built["mode"]),
            },
        )
        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is not None and callable(getattr(ti, "xcom_push", None)):
            ti.xcom_push(key="spark_agnostic", value=_agnostic_xcom_payload(setup_info, ui_url))
        timer.start()
        for line in proc.stdout:
            line = line.rstrip("\n")
            tail.append(line)
            log.info("%s", line)
        returncode = proc.wait()
    finally:
        finished.set()
        timer.cancel()
        _terminate(proc)

    if returncode != 0 or timed_out.is_set():
        raise LocalSparkJobFailed(
            returncode, "\n".join(tail), timed_out=timed_out.is_set(), run_id=run_id
        )
    return {"job_url": ui_url, "status": "SUCCEEDED", "run_id": run_id}


def _process_marker(setup_info: dict, mode: str) -> str:
    """Text present in the job process's command line, to avoid killing a reused pid."""
    if mode == "spark-submit" and setup_info.get("run_identifier"):
        return setup_info["run_identifier"]
    return _RUNNER_MARKER


def _is_local_run_process(pid: int, marker: str) -> bool:
    proc = subprocess.run(
        ["ps", "-o", "command=", "-p", str(pid)],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode == 0 and marker in proc.stdout


def cancel_local_run(run_id: str, extra: dict | None = None) -> None:
    """Stop a local run left over from a killed try, only on the host that started it."""
    extra = extra or {}
    pid = extra.get("pid")
    hostname = extra.get("hostname")
    if not pid:
        raise ValueError(f"No pid recorded for local run {run_id}")
    if hostname != socket.gethostname():
        raise RuntimeError(
            f"Local run {run_id} was started on {hostname}, not {socket.gethostname()}; "
            "it can only be cancelled there."
        )
    pid = int(pid)
    marker = extra.get("marker") or _RUNNER_MARKER
    if not _is_local_run_process(pid, marker):
        log.info("Local run %s (pid %s) is no longer running", run_id, pid)
        return
    _kill_process_group(pid)
    deadline = time.monotonic() + _KILL_GRACE_SECONDS
    while time.monotonic() < deadline:
        if not _is_local_run_process(pid, marker):
            return
        time.sleep(0.1)
    if _is_local_run_process(pid, marker):
        _kill_process_group(pid, getattr(signal, "SIGKILL", signal.SIGTERM))
