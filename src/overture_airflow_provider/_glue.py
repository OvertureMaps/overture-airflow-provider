"""AWS Glue execution: Python package + JAR caching, job submission."""

import json
import logging
import shutil
import time
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import boto3

from overture_airflow_provider._retry_guard import record_launched_run, task_instance_key
from overture_airflow_provider.cluster_sizing import AwsGlueClusterSize
from overture_airflow_provider.spark import SparkSedona
from overture_airflow_provider.spark_agnostic_helpers import SparkAgnosticHelper

_log = logging.getLogger(__name__)

#: Per-run Glue argument stamped with ``_retry_guard.task_instance_key`` so a
#: later try of the same task instance can find (and stop) this run via
#: ``get_job_runs``. Unknown ``--`` arguments are harmless to both runners:
#: ``getResolvedOptions`` parses with ``parse_known_args``, and Glue already
#: hands its own system arguments to a Scala ``main`` unfiltered.
TASK_INSTANCE_ARG = "--airflow_task_instance"

#: Glue ``JobRunState`` values that mean a run is (or may still become) a
#: live writer. ``WAITING`` is a queued run (``JobRunQueuingEnabled``);
#: ``STOPPING`` is already on its way out but must still be waited on.
_ACTIVE_GLUE_RUN_STATES = frozenset({"STARTING", "RUNNING", "STOPPING", "WAITING"})
#: Slack added to the job's own ``Timeout`` to bound how far back the
#: pre-submit scan walks a job's run history; covers queued waits and a
#: timeout that was lowered between tries. See ``_stale_run_lookback``.
_STALE_RUN_LOOKBACK_SLACK = timedelta(hours=16)
#: Hard cap on ``get_job_runs`` pages (200 runs each) per scan.
_STALE_RUN_MAX_PAGES = 10
#: Bounds on how long ``stop_stale_glue_runs`` waits for a stopped run to
#: actually reach a terminal state before the new run is submitted.
_STALE_RUN_STOP_TIMEOUT_SECONDS = 15 * 60
_STALE_RUN_POLL_INTERVAL_SECONDS = 10

# Keys excluded from the Glue Scala --conf DefaultArgument.
# spark.jars.packages: Glue can't resolve Maven coords at runtime; JARs are pre-staged via --extra-jars.
# spark.driver/executor.extraJavaOptions: already set via --driver-java-options / --executor-java-options;
#   duplicating them in --conf would override those args and lose the sedona charset setting.
_GLUE_SCALA_CONF_EXCLUDE = frozenset(
    {
        "spark.jars.packages",
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
    }
)


def _conf_default_arg(spark_conf_dict: dict) -> str | None:
    """Build Glue's native ``--conf`` DefaultArgument string from a Spark conf dict.

    Excludes keys Glue can't honor at session-creation time (Maven coords, java
    options). Returns ``None`` when nothing is left to inject. Used by both the
    Scala and PySpark paths so catalogs/extensions register identically at
    SparkSession bootstrap.
    """
    filtered = {k: v for k, v in spark_conf_dict.items() if k not in _GLUE_SCALA_CONF_EXCLUDE}
    if not filtered:
        return None
    return " --conf ".join(f"{k}={v}" for k, v in filtered.items())


def _build_agnostic_xcom_payload(setup_info: dict, *, job_url: str) -> str:
    return json.dumps(
        {
            "spark_impl": setup_info.get("spark_impl_name"),
            "spark_family": setup_info.get(
                "spark_family_name",
                str(setup_info.get("spark_family", "")),
            ),
            "spark_version": setup_info.get("spark_version"),
            "sedona_version": setup_info.get("sedona_version"),
            "job_url": job_url,
            "status": "RUNNING",
        }
    )


def download_python_packages_glue(
    setup_info: dict,
    python_packages: str,
) -> dict:
    """Download Python packages from the registry and cache in S3 for Glue."""
    helper = SparkAgnosticHelper(
        job_name=setup_info["job_name"],
        run_identifier=setup_info["run_identifier"],
        s3_bucket=setup_info["s3_assets_bucket"],
        s3_root=setup_info["s3_assets_root"],
        force_pip_packages=setup_info.get("force_pip_packages", []),
    )

    packages_to_download = python_packages.split()
    packages_to_download.append(f"apache-sedona=={setup_info['sedona_version']}")

    py_files, _job_runner_whl, tmp_folder_pypi, native_packages = (
        helper.download_and_cache_python_packages(
            py_pi_client=setup_info["py_pi_client"],
            packages=packages_to_download,
            python_version=setup_info["python_version"],
            job_runner_wheel_prefix=None,  # runners are now bundled in the provider
        )
    )

    s3_bucket = setup_info["s3_assets_bucket"]
    s3_root = setup_info["s3_assets_root"]

    from overture_airflow_provider.runner_assets import upload_runners_to_s3

    runner_uris = upload_runners_to_s3(
        helper.s3_client,
        s3_bucket,
        s3_root,
        overrides=setup_info.get("runner_script_overrides"),
        platforms=["glue", "glue_scala"],
    )
    script_location = runner_uris["glue"]
    scala_script_location = runner_uris["glue_scala"]

    shutil.rmtree(tmp_folder_pypi)

    return {
        "py_files": py_files,
        "script_location": script_location,
        "scala_script_location": scala_script_location,
        "s3_bucket": s3_bucket,
        "s3_prefix": helper.s3_prefix,
        "native_packages": native_packages,
    }


def download_jars_glue(
    setup_info: dict,
    spark_jar_paths: list[str],
) -> dict:
    """Download JARs from registry/Maven and cache in S3 for Glue."""
    from overture_airflow_provider._airflow_compat import AirflowException

    helper = SparkAgnosticHelper(
        job_name=setup_info["job_name"],
        run_identifier=setup_info["run_identifier"],
        s3_bucket=setup_info["s3_assets_bucket"],
        s3_root=setup_info["s3_assets_root"],
        force_pip_packages=setup_info.get("force_pip_packages", []),
    )

    codeartifact_maven_repo = helper.get_codeartifact_maven_repo(
        domain=setup_info["codeartifact_domain"],
        domain_owner=setup_info["codeartifact_domain_owner"],
        region=setup_info["codeartifact_region"],
        repository_path=setup_info["codeartifact_maven_repository_path"],
    )

    if not codeartifact_maven_repo:
        raise AirflowException(
            "CodeArtifact Maven repo URL is empty; cannot resolve Sedona/GeoTools "
            "JARs. Configure PackageRegistryConfig.maven_repository (and "
            "maven_repository_path if non-default) on the task group."
        )

    sedona_jars_http = [
        (
            f"{codeartifact_maven_repo}/org/apache/sedona/"
            f"sedona-spark-shaded-{setup_info['spark_version_for_sedona']}_"
            f"{setup_info['scala_version']}/{setup_info['sedona_version']}/"
            f"sedona-spark-shaded-{setup_info['spark_version_for_sedona']}_"
            f"{setup_info['scala_version']}-{setup_info['sedona_version']}.jar"
        ),
        (
            f"{codeartifact_maven_repo}/org/datasyslab/geotools-wrapper/"
            f"{setup_info['sedona_version']}-{setup_info['geotools_wrapper_version']}/"
            f"geotools-wrapper-{setup_info['sedona_version']}-"
            f"{setup_info['geotools_wrapper_version']}.jar"
        ),
    ]

    pre_provisioned_s3_paths: list[str] = []
    jar_urls_to_download: list[str] = []

    for spark_jar_path in spark_jar_paths:
        if not spark_jar_path or spark_jar_path.strip() == "":
            continue
        if spark_jar_path.startswith("s3://"):
            pre_provisioned_s3_paths.append(spark_jar_path)
        elif spark_jar_path.startswith("https://"):
            jar_urls_to_download.append(spark_jar_path)
        else:
            s3_jar_path = f"s3://{setup_info['s3_assets_bucket']}/scala_jars/{spark_jar_path}"
            pre_provisioned_s3_paths.append(s3_jar_path)

    all_jar_urls = sedona_jars_http + jar_urls_to_download
    jars_s3 = helper.download_and_cache_jars(
        jar_urls=all_jar_urls,
        pre_provisioned_jars=pre_provisioned_s3_paths,
    )

    sedona_packages = ",".join(
        SparkSedona.getSedonaJarPackages(
            sedona_version=setup_info["sedona_version"],
            py_spark_version=setup_info["spark_version"],
            scala_version=setup_info["scala_version"],
        )
    )

    sedona_module = f"apache-sedona=={setup_info['sedona_version']}"

    return {
        "jars_s3": jars_s3,
        "sedona_packages": sedona_packages,
        "sedona_module": sedona_module,
    }


def build_glue_operator_kwargs(
    setup_info: dict,
    package_info: dict,
    jar_info: dict,
    module_name: str,
    class_name: str,
    extra_spark_conf: dict,
    spark_cluster_desired_worker_cores: str,
    spark_cluster_desired_workers: str,
    max_timeout_hours: str,
    iam_role_name: str,
    task_id: str,
    dag_id: str = "",
    execution_class: str = "STANDARD",
    verbose: bool = True,
    task_instance_key: str | None = None,
) -> dict:
    """Pure-Python assembly of GlueJobOperator kwargs.

    Returns ``{"operator_kwargs", "create_job_kwargs", "script_args",
    "script_location", "tags"}``.

    Side-effect-free: does NOT instantiate any operator, call boto3, or
    invoke ``.execute()``. Used by both ``submit_glue_job`` (real submit)
    and ``overture_airflow_provider.render`` (Airflow-free preview).

    ``task_instance_key`` (see ``_retry_guard.task_instance_key``), when
    given, is stamped onto the per-run ``script_args`` as ``TASK_INSTANCE_ARG``
    so ``stop_stale_glue_runs`` can later recognise this run as belonging to
    the same task instance. It goes on the run ``Arguments``, not the job's
    ``DefaultArguments``: ``get_job_runs`` only reports the former per run.
    """
    if module_name:
        script_location = package_info["script_location"]
    else:
        script_location = package_info["scala_script_location"]

    native_packages = package_info.get("native_packages", [])
    sedona_module = jar_info.get("sedona_module")
    additional_modules = [sedona_module] if sedona_module else []
    if native_packages:
        for pkg in native_packages:
            if "apache-sedona" not in pkg:
                additional_modules.append(pkg)

    glue_job_default_args = {
        "--extra-jars": jar_info["jars_s3"],
        "--enable-glue-datacatalog": "true",
        "--enable-auto-scaling": "true",
        "--enable-spark-ui": "true",
        "--spark-event-logs-path": (
            f"s3://{package_info['s3_bucket']}/{package_info['s3_prefix']}/sparkHistoryLogs/"
        ),
        "--enable-metrics": "true",
        "--extra-py-files": package_info["py_files"],
        "--datalake-formats": "iceberg",
    }

    if additional_modules:
        glue_job_default_args["--additional-python-modules"] = ", ".join(additional_modules)

    spark_conf_dict = {
        "spark.jars.packages": jar_info["sedona_packages"],
        **extra_spark_conf,
    }

    if module_name:
        # Inject Iceberg / Spark conf into DefaultArguments as Glue's native --conf so the
        # catalogs/extensions register at SparkSession bootstrap, before user code runs.
        # Glue (not the runner) builds the session for PySpark jobs too, so legacy
        # SparkSedonaJob.run() implementations that don't accept a `spark` kwarg still get
        # the named catalogs. --extra_spark_conf is kept as the documented runner contract.
        conf_arg = _conf_default_arg(spark_conf_dict)
        if conf_arg:
            glue_job_default_args["--conf"] = conf_arg
        script_args = {
            "--module_name": module_name,
            "--class_name": class_name,
            "--params": setup_info["parameters"],
            "--extra_spark_conf": json.dumps(spark_conf_dict),
        }
    else:
        glue_job_default_args["--class"] = class_name
        glue_job_default_args["--job-language"] = "scala"
        glue_job_default_args["--driver-java-options"] = (
            "-Djts.overlay=ng -Dsedona.global.charset=utf8"
        )
        glue_job_default_args["--executor-java-options"] = (
            "-Djts.overlay=ng -Dsedona.global.charset=utf8"
        )
        glue_job_default_args = {
            k: v for k, v in glue_job_default_args.items() if v is not None and v != ""
        }
        # Inject Iceberg / Spark conf into DefaultArguments as Glue's native --conf mechanism.
        # Glue applies this at session-creation time, before any user code runs, so the catalog
        # is registered even though the real entry point is the caller's --class in --extra-jars.
        # Format: "k1=v1 --conf k2=v2 ..." (combined with the "--conf" key itself by Glue).
        conf_arg = _conf_default_arg(spark_conf_dict)
        if conf_arg:
            glue_job_default_args["--conf"] = conf_arg
        parsed_params = (
            json.loads(setup_info["parameters"])
            if isinstance(setup_info["parameters"], str)
            else setup_info["parameters"]
        )
        if isinstance(parsed_params, dict):
            base_params = {k: v for k, v in parsed_params.items() if v is not None and v != ""}
        else:
            base_params = {
                "--params": (json.dumps(parsed_params) if parsed_params is not None else "")
            }
        script_args = {
            **base_params,
            "--extra-jars": jar_info["jars_s3"],
            "--extraSparkConf": json.dumps(spark_conf_dict),
            "--user-jars-first": "true",
        }

    if task_instance_key:
        script_args[TASK_INSTANCE_ARG] = task_instance_key

    cluster_size_kwargs = AwsGlueClusterSize.from_desired_cores(
        int(spark_cluster_desired_worker_cores),
        int(spark_cluster_desired_workers) if spark_cluster_desired_workers else 1,
    )
    create_job_kwargs = {
        **cluster_size_kwargs,
        "GlueVersion": setup_info["spark_impl"].get_native_version(),
        "DefaultArguments": glue_job_default_args,
        "ExecutionProperty": {"MaxConcurrentRuns": 100},
        "ExecutionClass": AwsGlueClusterSize.resolve_execution_class(
            execution_class, cluster_size_kwargs["WorkerType"]
        ),
        "Command": {
            "Name": "glueetl",
            "ScriptLocation": script_location,
        },
        "Timeout": 60 * int(max_timeout_hours),
    }

    tags = {
        "airflow_dag": dag_id,
        "airflow_task": task_id,
        "job_name": setup_info["job_name"],
    }

    operator_kwargs = {
        "task_id": task_id,
        "job_name": setup_info["job_name"],
        "script_location": script_location,
        "script_args": script_args,
        "s3_bucket": package_info["s3_bucket"],
        "iam_role_name": iam_role_name,
        "region_name": setup_info["aws_region"],
        "update_config": True,
        "create_job_kwargs": create_job_kwargs,
        "run_job_kwargs": {"JobRunQueuingEnabled": True},
        "verbose": verbose,
        "deferrable": True,
    }

    return {
        "operator_kwargs": operator_kwargs,
        "create_job_kwargs": create_job_kwargs,
        "script_args": script_args,
        "script_location": script_location,
        "tags": tags,
    }


def _glue_console_url(region: str, job_name: str, run_id: str) -> str:
    """Build the AWS Glue Studio console URL for a job run."""
    return (
        f"https://{region}.console.aws.amazon.com/gluestudio/home?region={region}"
        f"#/job/{job_name}/run/{run_id}"
    )


def _run_started_on(run: dict) -> datetime | None:
    started = run.get("StartedOn")
    if not isinstance(started, datetime):
        return None
    return started if started.tzinfo else started.replace(tzinfo=UTC)


def _stale_run_lookback(max_timeout_hours: int | str) -> timedelta:
    """How far back the pre-submit scan walks ``job_name``'s run history.

    Every run the provider starts is capped by the job's ``Timeout``
    (``60 * max_timeout_hours`` minutes), so a run that started more than
    that plus ``_STALE_RUN_LOOKBACK_SLACK`` ago is necessarily terminal.
    """
    return timedelta(hours=int(max_timeout_hours)) + _STALE_RUN_LOOKBACK_SLACK


def find_active_glue_runs(
    glue_client,
    job_name: str,
    task_instance_key: str,
    *,
    lookback: timedelta,
    now: datetime | None = None,
    max_pages: int = _STALE_RUN_MAX_PAGES,
) -> list[dict]:
    """Return this task instance's still-active runs of ``job_name``.

    Walks ``get_job_runs`` (newest first) and keeps runs whose ``Arguments``
    carry ``task_instance_key`` under ``TASK_INSTANCE_ARG`` and whose state is
    in ``_ACTIVE_GLUE_RUN_STATES``. Stops once it reaches a run that started
    before ``now - lookback`` (see ``_stale_run_lookback``: anything older is
    past the job's own ``Timeout``) or after ``max_pages`` pages, whichever
    comes first.
    """
    now = now or datetime.now(UTC)
    cutoff = now - lookback
    matches: list[dict] = []
    next_token: str | None = None
    for _ in range(max_pages):
        kwargs = {"JobName": job_name, "MaxResults": 200}
        if next_token:
            kwargs["NextToken"] = next_token
        response = glue_client.get_job_runs(**kwargs)
        runs = response.get("JobRuns") or []
        for run in runs:
            started = _run_started_on(run)
            if started is not None and started < cutoff:
                return matches
            if run.get("JobRunState") not in _ACTIVE_GLUE_RUN_STATES:
                continue
            if (run.get("Arguments") or {}).get(TASK_INSTANCE_ARG) != task_instance_key:
                continue
            matches.append(run)
        next_token = response.get("NextToken")
        if not isinstance(next_token, str) or not next_token:
            return matches
    _log.warning(
        "Stale-run scan for Glue job %s stopped after %d pages without reaching the "
        "%s lookback boundary; an older run of this task instance could be missed.",
        job_name,
        max_pages,
        lookback,
    )
    return matches


def stop_stale_glue_runs(
    glue_client,
    job_name: str,
    task_instance_key: str,
    *,
    max_timeout_hours: int | str,
    sleep: Callable[[float], None] = time.sleep,
    timeout_seconds: float = _STALE_RUN_STOP_TIMEOUT_SECONDS,
    poll_interval_seconds: float = _STALE_RUN_POLL_INTERVAL_SECONDS,
) -> list[str]:
    """Stop any still-active run this task instance launched earlier, and wait.

    Called right before a new run is submitted. Any match here can only be a
    leftover from an earlier try -- a zombie kill, or a *clear* of a deferred
    task, where no callback ever fired and the XCom-recorded run id is already
    gone -- so it's stopped via ``batch_stop_job_run`` and then polled until
    Glue reports a terminal state. Submitting before that would put two
    writers on the same output, which is exactly the race this prevents.

    ``max_timeout_hours`` is the job's configured run timeout; it bounds how
    far back the run history is scanned (``_stale_run_lookback``).

    Returns the ids of the runs that were stopped. Raises ``AirflowException``
    if a run can't be stopped or doesn't reach a terminal state within
    ``timeout_seconds``; the caller hasn't launched anything yet at that
    point, so the failure classifies as never-launched and stays retryable.
    """
    from overture_airflow_provider._airflow_compat import AirflowException

    active = find_active_glue_runs(
        glue_client,
        job_name,
        task_instance_key,
        lookback=_stale_run_lookback(max_timeout_hours),
    )
    if not active:
        return []

    run_ids = [run["Id"] for run in active]
    _log.warning(
        "Found %d still-active Glue run(s) of job %s launched by an earlier try of "
        "this task instance (%s); stopping before submitting a new run: %s",
        len(run_ids),
        job_name,
        task_instance_key,
        ", ".join(run_ids),
    )

    to_stop = [run["Id"] for run in active if run.get("JobRunState") != "STOPPING"]
    if to_stop:
        response = glue_client.batch_stop_job_run(JobName=job_name, JobRunIds=to_stop)
        for error in response.get("Errors") or []:
            run_id = error.get("JobRunId")
            # A run can finish between the scan and the stop; only a still-active
            # one that refused to stop is a problem.
            state = glue_client.get_job_run(JobName=job_name, RunId=run_id)["JobRun"].get(
                "JobRunState"
            )
            if state in _ACTIVE_GLUE_RUN_STATES:
                detail = error.get("ErrorDetail") or {}
                raise AirflowException(
                    f"Could not stop still-active Glue run {run_id} of job {job_name} left "
                    f"over from an earlier try of this task instance ({state}): "
                    f"{detail.get('ErrorCode')}: {detail.get('ErrorMessage')}. Refusing to "
                    "submit a second run against the same output."
                )

    pending = set(run_ids)
    deadline = time.monotonic() + timeout_seconds
    while pending:
        for run_id in sorted(pending):
            state = glue_client.get_job_run(JobName=job_name, RunId=run_id)["JobRun"].get(
                "JobRunState"
            )
            if state not in _ACTIVE_GLUE_RUN_STATES:
                _log.info("Stale Glue run %s of job %s is now %s", run_id, job_name, state)
                pending.discard(run_id)
        if not pending:
            break
        if time.monotonic() >= deadline:
            raise AirflowException(
                f"Glue run(s) {', '.join(sorted(pending))} of job {job_name} left over from "
                f"an earlier try of this task instance did not stop within "
                f"{int(timeout_seconds)}s. Refusing to submit a second run against the same "
                "output."
            )
        sleep(poll_interval_seconds)
    return run_ids


def submit_glue_job(
    setup_info: dict,
    package_info: dict,
    jar_info: dict,
    module_name: str,
    class_name: str,
    extra_spark_conf: dict,
    spark_cluster_desired_worker_cores: str,
    spark_cluster_desired_workers: str,
    max_timeout_hours: str,
    iam_role_name: str,
    task_id: str,
    context: dict,
    execution_class: str = "STANDARD",
    verbose: bool = True,
) -> dict:
    """Submit a Glue job (non-blocking) and return a trigger to defer on.

    Builds the upstream ``GlueJobOperator`` with ``deferrable=True`` and calls
    its ``execute()``. In deferrable mode the operator submits the run and then
    raises ``TaskDeferred`` carrying a ``GlueJobCompleteTrigger`` it constructs
    itself — so the trigger is always built with the kwargs the *installed*
    amazon provider expects (older versions don't accept ``region_name``). We
    catch that exception and hand the trigger back to our operator's
    ``execute_complete`` instead of the inner operator's. The early
    ``spark_agnostic`` XCom is pushed here so ``SparkJobLink`` works while the
    task is deferred.

    Before submitting, any still-active run an earlier try of this same task
    instance launched is stopped and waited on (``stop_stale_glue_runs``), so
    a cleared or zombie-killed try can't keep writing alongside the new run.
    The new run is stamped with the task instance's key so the next try can
    do the same.
    """
    from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

    from overture_airflow_provider._airflow_compat import TaskDeferred

    ti_key = task_instance_key(context)

    if not module_name:
        # Scala job: ensure placeholder script exists.
        scala_script = package_info["scala_script_location"]
        s3_client = boto3.client("s3")
        bucket = scala_script.replace("s3://", "").split("/")[0]
        scala_script_key = "/".join(scala_script.replace("s3://", "").split("/")[1:])

        try:
            s3_client.head_object(Bucket=bucket, Key=scala_script_key)
        except s3_client.exceptions.ClientError as exc:
            if exc.response["Error"]["Code"] == "404":
                placeholder = (
                    "// Placeholder Scala script for Glue job\n"
                    "// The actual job logic is in the JAR specified via --extra-jars\n"
                    "// This file is required by AWS Glue for Scala jobs but is not executed\n"
                )
                s3_client.put_object(
                    Bucket=bucket,
                    Key=scala_script_key,
                    Body=placeholder.encode("utf-8"),
                )
            else:
                raise

    built = build_glue_operator_kwargs(
        setup_info=setup_info,
        package_info=package_info,
        jar_info=jar_info,
        module_name=module_name,
        class_name=class_name,
        extra_spark_conf=extra_spark_conf,
        spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
        spark_cluster_desired_workers=spark_cluster_desired_workers,
        max_timeout_hours=max_timeout_hours,
        iam_role_name=iam_role_name,
        task_id=task_id,
        dag_id=context["dag"].dag_id if "dag" in context else "",
        execution_class=execution_class,
        verbose=verbose,
        task_instance_key=ti_key,
    )

    platform_operator = GlueJobOperator(**built["operator_kwargs"])

    # Tags only apply on job creation.
    glue_client = boto3.client("glue", region_name=setup_info["aws_region"])
    try:
        glue_client.get_job(JobName=setup_info["job_name"])
    except glue_client.exceptions.EntityNotFoundException:
        platform_operator.create_job_kwargs = {
            **built["create_job_kwargs"],
            "Tags": built["tags"],
        }
    else:
        # The job exists, so an earlier try of this task instance may have left
        # a run of it going. A brand-new job has no runs to check.
        if ti_key:
            stop_stale_glue_runs(
                glue_client,
                setup_info["job_name"],
                ti_key,
                max_timeout_hours=max_timeout_hours,
            )

    # deferrable=True -> execute() submits the run, then raises TaskDeferred with
    # the provider's own GlueJobCompleteTrigger. We reuse that trigger.
    try:
        platform_operator.execute(context)
    except TaskDeferred as deferred:
        trigger = deferred.trigger
    else:  # pragma: no cover - deferrable execute always defers
        raise RuntimeError("GlueJobOperator did not defer; expected deferrable=True to raise.")

    run_id = getattr(platform_operator, "_job_run_id", None) or getattr(trigger, "run_id", None)
    region = setup_info["aws_region"]
    job_name = setup_info["job_name"]
    job_url = _glue_console_url(region, job_name, run_id)

    ti = context.get("ti") if hasattr(context, "get") else None
    if ti is not None and callable(getattr(ti, "xcom_push", None)):
        ti.xcom_push(
            key="spark_agnostic",
            value=_build_agnostic_xcom_payload(setup_info, job_url=job_url),
        )

    # Recorded the moment the run id is known -- before any polling/deferral --
    # so a zombie kill anywhere after this point still leaves a trail this
    # task instance's on_failure_callback can cancel instead of a retry
    # racing a second run against the same output.
    record_launched_run(
        context,
        platform="glue",
        run_id=run_id,
        extra={"job_name": job_name, "region": region},
    )

    return {
        "trigger": trigger,
        "run_id": run_id,
        "platform_operator": platform_operator,
    }


def cancel_glue_run(run_id: str, extra: dict | None = None) -> None:
    """Best-effort stop of a Glue job run left over from a killed or zombie try."""
    extra = extra or {}
    glue_client = boto3.client("glue", region_name=extra.get("region"))
    glue_client.batch_stop_job_run(JobName=extra.get("job_name"), JobRunIds=[run_id])


#: AWS's own default continuous-logging output log group; see GlueConfig
#: .output_log_group in config.py for how a consumer overrides this.
_GLUE_OUTPUT_LOG_GROUP = "/aws-glue/jobs/output"
#: Glue's driver prints this immediately before exiting, on every job run
#: regardless of the job's own code. Its presence means the stream has
#: nothing left to deliver.
_GLUE_SHUTDOWN_MARKER = "Running autoDebugger shutdown hook."
#: Bounds on how long the CloudWatch catch-up poll below can run.
_LOG_TAIL_POLL_ATTEMPTS = 6
_LOG_TAIL_POLL_INTERVAL_SECONDS = 4
#: Logged with each diagnostic line below so a task log shows which version
#: of this function's fetch/sort logic actually ran.
_LOG_TAIL_FETCH_VERSION = "sort-v1"


def _fetch_glue_output_log_tail(
    region: str,
    run_id: str | None,
    max_events: int = 1000,
    poll: bool = False,
    sleep: Callable[[float], None] = time.sleep,
    log_group: str = _GLUE_OUTPUT_LOG_GROUP,
) -> str | None:
    """Best-effort fetch of a Glue job run's driver stdout tail.

    Glue's own ``JobRun.LogTail`` is empty for GlueVersion 5.0 Spark jobs, so
    a job's own diagnostics (e.g. a validation job's error report) live only
    in its CloudWatch output log stream, named after the run id. ``log_group``
    defaults to Glue's own log group but should be set from the consumer's
    ``GlueConfig.output_log_group`` when a job writes continuous logging
    output elsewhere.

    ``GetLogEvents`` can return an incomplete or out-of-order view for a
    while after a run finishes, even once the stream is done. ``poll=True``
    retries until ``_GLUE_SHUTDOWN_MARKER`` shows up, proving the stream is
    actually done; events are sorted by timestamp before joining regardless,
    since order isn't guaranteed even once everything's arrived. Omitting
    ``poll`` does a single fetch.

    Returns ``None`` on any failure, so a fetch problem here can't break
    failure reporting itself.
    """
    if not run_id:
        return None
    try:
        logs_client = boto3.client("logs", region_name=region)
    except Exception:
        return None

    attempts = _LOG_TAIL_POLL_ATTEMPTS if poll else 1
    events: list[dict] = []
    for attempt in range(attempts):
        try:
            response = logs_client.get_log_events(
                logGroupName=log_group,
                logStreamName=run_id,
                startFromHead=False,
                limit=max_events,
            )
        except Exception as exc:  # noqa: BLE001
            _log.warning(
                "log-tail fetch attempt %d/%d for %s raised: %s",
                attempt + 1,
                attempts,
                run_id,
                exc,
            )
            break
        events = sorted(response.get("events", []), key=lambda e: e.get("timestamp", 0))
        complete = any(_GLUE_SHUTDOWN_MARKER in e.get("message", "") for e in events)
        _log.debug(
            "log-tail fetch [%s] attempt %d/%d for %s: %d events "
            "(first_ts=%s, last_ts=%s), shutdown marker %s",
            _LOG_TAIL_FETCH_VERSION,
            attempt + 1,
            attempts,
            run_id,
            len(events),
            events[0].get("timestamp") if events else None,
            events[-1].get("timestamp") if events else None,
            "found" if complete else "not found",
        )
        if complete or attempt == attempts - 1:
            break
        sleep(_LOG_TAIL_POLL_INTERVAL_SECONDS)
    tail = "\n".join(e.get("message", "") for e in events) or None
    _log.debug(
        "log-tail fetch [%s] returning %d chars for %s",
        _LOG_TAIL_FETCH_VERSION,
        len(tail) if tail else 0,
        run_id,
    )
    return tail


def complete_glue_job(setup_info: dict, run_id: str, context: dict, handler=None) -> dict:
    """Resolve a completed Glue run into the final result dict.

    Called from the deferrable operator's ``execute_complete`` after the
    ``GlueJobCompleteTrigger`` reports the run reached a terminal state. On a
    non-success state, ``handler`` (when supplied) is used to raise a classified,
    de-noised failure naming the Glue ``ErrorMessage`` as the root cause, with the
    run's own stdout tail attached when Glue's ``LogTail`` field is empty.
    """
    from overture_airflow_provider._airflow_compat import AirflowException

    region = setup_info["aws_region"]
    job_name = setup_info["job_name"]
    glue_client = boto3.client("glue", region_name=region)

    job_status = glue_client.get_job_run(JobName=job_name, RunId=run_id)
    job_run = job_status["JobRun"]
    job_state = job_run["JobRunState"]
    if job_state != "SUCCEEDED":
        if handler is not None:
            from overture_airflow_provider._failures import format_failure

            if not job_run.get("LogTail"):
                log_group = setup_info.get("glue_output_log_group", _GLUE_OUTPUT_LOG_GROUP)
                output_tail = _fetch_glue_output_log_tail(
                    region, run_id, poll=True, log_group=log_group
                )
                if output_tail:
                    job_run = {**job_run, "LogTail": output_tail}

            failure = handler.describe_failure(
                payload=job_run,
                run_id=run_id,
                run_launched=True,
                console_url=_glue_console_url(region, job_name, run_id),
            )
            raise AirflowException(format_failure(failure)) from None
        msg = f"Glue job {job_name} (run {run_id}) did not succeed. Final state: {job_state}"
        print(f"ERROR: {msg}")
        raise AirflowException(msg)

    return {
        "job_url": _glue_console_url(region, job_name, run_id),
        "status": job_status["JobRun"],
    }
