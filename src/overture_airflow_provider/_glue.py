"""AWS Glue execution: Python package + JAR caching, job submission."""

import json
import shutil

import boto3
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator

from overture_airflow_provider._airflow_compat import AirflowException
from overture_airflow_provider.cluster_sizing import AwsGlueClusterSize
from overture_airflow_provider.spark import SparkSedona
from overture_airflow_provider.spark_agnostic_helpers import SparkAgnosticHelper

MAX_TIMEOUT_HOURS = 8

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


def _get_glue_job_url_and_status(
    platform_operator: GlueJobOperator,
    context: dict,
    setup_info: dict,
) -> dict:
    """Return console URL + job state; raise on terminal failure."""
    glue_client = boto3.client("glue", region_name=setup_info["aws_region"])

    glue_job_run_details = platform_operator.xcom_pull(
        context, task_ids=context["ti"].task_id, key="glue_job_run_details"
    )

    if isinstance(glue_job_run_details, dict):
        region = glue_job_run_details["region_name"]
        domain = glue_job_run_details["aws_domain"]
        job_name = glue_job_run_details["job_name"]
        job_run_id = glue_job_run_details["job_run_id"]
    else:
        details = list(glue_job_run_details)
        region = details[0]["region_name"]
        domain = details[0]["aws_domain"]
        job_name = details[0]["job_name"]
        job_run_id = details[0]["job_run_id"]

    job_url = (
        f"https://{region}.console.{domain}/gluestudio/home?region={region}"
        f"#/job/{job_name}/run/{job_run_id}"
    )

    job_status = glue_client.get_job_run(JobName=job_name, RunId=job_run_id)
    job_state = job_status["JobRun"]["JobRunState"]
    if job_state not in ["SUCCEEDED"]:
        msg = f"Glue job {job_name} (run {job_run_id}) did not succeed. Final state: {job_state}"
        if job_state in ["STOPPED", "FAILED", "TIMEOUT", "ERROR"]:
            print(f"ERROR: {msg}")
            raise AirflowException(msg)
        print(
            f"WARNING: Glue job {job_name} (run {job_run_id}) is in unexpected state: {job_state}"
        )

    return {
        "job_url": job_url,
        "status": job_status["JobRun"],
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
    iam_role_name: str,
    task_id: str,
    dag_id: str = "",
    execution_class: str = "STANDARD",
) -> dict:
    """Pure-Python assembly of GlueJobOperator kwargs.

    Returns ``{"operator_kwargs", "create_job_kwargs", "script_args",
    "script_location", "tags"}``.

    Side-effect-free: does NOT instantiate any operator, call boto3, or
    invoke ``.execute()``. Used by both ``execute_glue_job`` (real submit)
    and ``overture_airflow_provider.render`` (Airflow-free preview).
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
        "Timeout": 60 * MAX_TIMEOUT_HOURS,
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
        "verbose": True,
        "deferrable": True,
    }

    return {
        "operator_kwargs": operator_kwargs,
        "create_job_kwargs": create_job_kwargs,
        "script_args": script_args,
        "script_location": script_location,
        "tags": tags,
    }


def execute_glue_job(
    setup_info: dict,
    package_info: dict,
    jar_info: dict,
    module_name: str,
    class_name: str,
    extra_spark_conf: dict,
    spark_cluster_desired_worker_cores: str,
    spark_cluster_desired_workers: str,
    iam_role_name: str,
    task_id: str,
    context: dict,
    execution_class: str = "STANDARD",
) -> dict:
    """Submit and wait for an AWS Glue job."""
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
        iam_role_name=iam_role_name,
        task_id=task_id,
        dag_id=context["dag"].dag_id if "dag" in context else "",
        execution_class=execution_class,
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
    ti = context.get("ti") if hasattr(context, "get") else None
    original_xcom_push = getattr(ti, "xcom_push", None)
    early_xcom_pushed = False

    if callable(original_xcom_push):

        def _xcom_push_with_early_agnostic(*args, **kwargs):
            nonlocal early_xcom_pushed
            result = original_xcom_push(*args, **kwargs)
            key = kwargs.get("key") if "key" in kwargs else (args[0] if args else None)
            value = (
                kwargs.get("value") if "value" in kwargs else (args[1] if len(args) > 1 else None)
            )
            if key == "glue_job_run_id" and value and not early_xcom_pushed:
                region = setup_info["aws_region"]
                job_name = setup_info["job_name"]
                job_url = (
                    f"https://{region}.console.aws.amazon.com/gluestudio/home?region={region}"
                    f"#/job/{job_name}/run/{value}"
                )
                original_xcom_push(
                    key="spark_agnostic",
                    value=_build_agnostic_xcom_payload(setup_info, job_url=job_url),
                )
                early_xcom_pushed = True
            return result

        ti.xcom_push = _xcom_push_with_early_agnostic

    try:
        platform_operator.execute(context)
    finally:
        if callable(original_xcom_push):
            ti.xcom_push = original_xcom_push

    job_result = _get_glue_job_url_and_status(platform_operator, context, setup_info)

    return {
        **job_result,
        "platform_operator": platform_operator,
    }
