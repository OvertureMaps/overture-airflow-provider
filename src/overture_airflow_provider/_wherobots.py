"""Wherobots execution: Python package + JAR caching, job submission."""

import json
import re
import shutil
from typing import Any

from overture_airflow_provider._airflow_compat import AirflowException, BaseHook
from overture_airflow_provider.cluster_sizing import WherobotsClusterSize
from overture_airflow_provider.spark_agnostic_helpers import SparkAgnosticHelper

# Optional dependency: Wherobots SDK isn't installed in every environment.
try:
    from airflow_providers_wherobots.operators.run import WherobotsRunOperator
    from wherobots.db import Region

    WHEROBOTS_AVAILABLE = True
except ImportError:
    WHEROBOTS_AVAILABLE = False

MAX_TIMEOUT_HOURS = 8
WHEROBOTS_PROVIDER = "com.wherobots.awssdk.auth.WherobotsAssumeRoleCredentialsProvider"
_API_SUBDOMAIN_PREFIX = "api."


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


def _build_wherobots_run_url(platform_operator: Any, run_id: str) -> str | None:
    conn = BaseHook.get_connection(platform_operator.wherobots_conn_id)
    host = (conn.host or "").strip()
    if not host:
        return None
    host = host.removeprefix("https://").removeprefix("http://").rstrip("/")
    if host.startswith(_API_SUBDOMAIN_PREFIX):
        host = host[len(_API_SUBDOMAIN_PREFIX) :]
    return f"https://{host}/runs/{run_id}"


def _resolve_wherobots_region(aws_region: str):
    """Map an AWS region (e.g. ``us-west-2``) to the matching ``Region`` enum.

    Falls back to the first member whose name ends with the upper-cased
    underscore-form of the region. Raises ``AirflowException`` on miss.
    """
    target = aws_region.upper().replace("-", "_")
    for member in Region:
        if member.name.endswith(target):
            return member
    raise AirflowException(f"No Wherobots Region enum value matches AWS region '{aws_region}'")


def download_python_packages_wherobots(
    setup_info: dict,
    python_packages: str,
) -> dict:
    """Download Python packages for Wherobots, cache in S3, extract job runner."""
    helper = SparkAgnosticHelper(
        job_name=setup_info["job_name"],
        run_identifier=setup_info["run_identifier"],
        s3_bucket=setup_info["s3_assets_bucket"],
        s3_root=setup_info["s3_assets_root"],
        force_pip_packages=setup_info.get("force_pip_packages", []),
    )

    # Wherobots handles native packages differently, so the native_packages
    # list is reused below to build the dependencies spec rather than passed
    # to a separate install mechanism.
    packages_to_download = python_packages.split()
    packages_to_download.append(f"apache-sedona=={setup_info['sedona_version']}")

    py_files_str, _job_runner_whl, tmp_folder_pypi, native_packages = (
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
        platforms=["wherobots"],
    )
    script_location = runner_uris["wherobots"]

    shutil.rmtree(tmp_folder_pypi)

    py_files = py_files_str.split(",") if py_files_str else []

    # Wherobots dependencies list (filter out aarch64 wheels).
    python_packages_or_jars_list = []
    for package in py_files:
        if "aarch64" in package:
            print(f"Filtering out aarch64 whl: {package}")
            continue
        python_packages_or_jars_list.append(
            {
                "sourceType": "FILE",
                "filePath": package,
            }
        )

    for pkg in native_packages:
        if "sedona" in pkg:
            continue
        if "==" not in pkg:
            raise ValueError(
                "Wherobots execution requires specific library version, please "
                f"pin a version with '==' for: {pkg}"
            )
        name, version = pkg.split("==")
        python_packages_or_jars_list.append(
            {
                "sourceType": "PYPI",
                "libraryName": name,
                "libraryVersion": version,
            }
        )

    return {
        "py_files": py_files,
        "script_location": script_location,
        "python_packages_or_jars_list": python_packages_or_jars_list,
    }


def download_jars_wherobots(
    setup_info: dict,
    spark_jar_paths: list,
) -> dict:
    """Download JARs for Wherobots and cache in S3."""
    helper = SparkAgnosticHelper(
        job_name=setup_info["job_name"],
        run_identifier=setup_info["run_identifier"],
        s3_bucket=setup_info["s3_assets_bucket"],
        s3_root=setup_info["s3_assets_root"],
        force_pip_packages=setup_info.get("force_pip_packages", []),
    )

    print("Downloading spark jars from CodeArtifact/Maven and uploading to S3")

    pre_provisioned_s3_paths = []
    jar_urls_to_download = []

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

    jars_s3_str = helper.download_and_cache_jars(
        jar_urls=jar_urls_to_download,
        pre_provisioned_jars=pre_provisioned_s3_paths,
    )

    jars_s3 = jars_s3_str.split(",") if jars_s3_str else []

    return {
        "jars_s3": jars_s3,
    }


def build_wherobots_operator_kwargs(
    setup_info: dict,
    package_info: dict,
    jar_info: dict,
    module_name: str,
    class_name: str,
    extra_spark_conf: dict,
    spark_cluster_size: str,
    spark_cluster_desired_worker_cores: str,
    spark_cluster_desired_workers: str,
    wherobots_role_arn: str,
    task_id: str,
    version: str = "preview",
    resolve_region: bool = True,
) -> dict:
    """Pure-Python assembly of WherobotsRunOperator kwargs.

    Side-effect-free apart from optionally resolving the Wherobots ``Region``
    enum (set ``resolve_region=False`` to skip when the SDK isn't installed,
    in which case the raw AWS region string is returned in ``region``).

    Returns ``{"operator_kwargs", "submit_payload"}``. ``submit_payload`` is
    the JSON-serialisable equivalent used by the Wherobots REST API / CLI.
    """
    my_parameters = setup_info["parameters"]

    python_packages_or_jars_list = list(package_info["python_packages_or_jars_list"])

    for jar_path in jar_info["jars_s3"]:
        if re.search(r"hadoop-azure-\d+(\.\d+)*$", jar_path):
            continue
        python_packages_or_jars_list.append({"sourceType": "FILE", "filePath": jar_path})

    is_scala_job = not module_name

    if is_scala_job:
        parsed_params = (
            json.loads(my_parameters) if isinstance(my_parameters, str) else my_parameters
        )
        args_list: list = []
        for key, value in parsed_params.items():
            args_list.append(key if key.startswith("--") else f"--{key}")
            args_list.append(str(value))
    else:
        args_list = [
            "--module_name",
            module_name,
            "--class_name",
            class_name,
            "--params",
            my_parameters,
        ]

    if isinstance(extra_spark_conf, str):
        spark_configs = json.loads(extra_spark_conf)
    else:
        spark_configs = dict(extra_spark_conf)

    spark_configs = {k: str(v) for k, v in spark_configs.items()}

    for cfg in (
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
        "sedona.join.numpartition",
        "spark.kryoserializer.buffer",
        "spark.driver.maxResultSize",
        "spark.sql.sources.partitionOverwriteMode",
    ):
        spark_configs.pop(cfg, None)

    if "spark.sql.defaultCatalog" in spark_configs:
        if not wherobots_role_arn:
            raise ValueError(
                "WherobotsConfig.role_arn is required when using Iceberg with Wherobots. "
                'Set it via: WherobotsConfig(role_arn="arn:aws:iam::<account>:role/<role-name>", ...)'
            )
        catalog_name = spark_configs["spark.sql.defaultCatalog"]
        spark_configs.update(
            {
                f"spark.sql.catalog.{catalog_name}.client.factory": "com.wherobots.iceberg.aws.WherobotsStIntCredentialsFactory",
                f"spark.sql.catalog.{catalog_name}.client.assume-role.arn": wherobots_role_arn,
                f"spark.sql.catalog.{catalog_name}.client.assume-role.region": setup_info[
                    "aws_region"
                ],
                f"spark.sql.catalog.{catalog_name}.client.credentials-provider": WHEROBOTS_PROVIDER,
                f"spark.sql.catalog.{catalog_name}.client.credentials-provider.role-arn": wherobots_role_arn,
                f"spark.sql.catalog.{catalog_name}.client.credentials-provider.external-id": setup_info[
                    "wherobots_external_id"
                ],
                f"spark.sql.catalog.{catalog_name}.client.assume-role.external-id": setup_info[
                    "wherobots_external_id"
                ],
            }
        )

    runtime_name = ""
    if spark_cluster_size:
        runtime_name = WherobotsClusterSize.from_cluster_size(spark_cluster_size)
    elif spark_cluster_desired_worker_cores:
        runtime_name = WherobotsClusterSize.from_desired_cores(
            int(spark_cluster_desired_worker_cores)
        )

    if is_scala_job:
        jar_uri = None
        for dep in python_packages_or_jars_list:
            if dep.get("sourceType") == "FILE" and dep.get("filePath", "").endswith(".jar"):
                jar_path = dep.get("filePath", "")
                if (
                    "hadoop-azure" not in jar_path
                    and "sedona" not in jar_path
                    and "geotools" not in jar_path
                ):
                    jar_uri = jar_path
                    break
        if not jar_uri:
            for dep in python_packages_or_jars_list:
                if dep.get("sourceType") == "FILE" and dep.get("filePath", "").endswith(".jar"):
                    jar_uri = dep.get("filePath")
                    break
        if not jar_uri:
            raise ValueError(
                f"No JAR file found in dependencies for Scala job with class {class_name}"
            )

        run_jar = {"uri": jar_uri, "mainClass": class_name, "args": args_list}
        run_python = None
        name = class_name
        poll_logs = False
        polling_interval = 30
    else:
        script_location = package_info["script_location"]
        if isinstance(script_location, list):
            script_location = script_location[0] if script_location else ""
        run_python = {"uri": script_location, "args": args_list}
        run_jar = None
        name = f"{module_name}.{class_name}"
        poll_logs = True
        polling_interval = 10

    environment = {
        "sparkConfigs": spark_configs,
        "dependencies": python_packages_or_jars_list,
    }

    region_val = setup_info["aws_region"]
    if resolve_region and WHEROBOTS_AVAILABLE:
        region_val = _resolve_wherobots_region(setup_info["aws_region"])

    operator_kwargs = {
        "task_id": task_id,
        "name": name,
        "runtime": runtime_name,
        "version": version,
        "poll_logs": poll_logs,
        "polling_interval": polling_interval,
        "timeout_seconds": (3600 * MAX_TIMEOUT_HOURS),
        "region": region_val,
        "environment": environment,
    }
    if run_jar is not None:
        operator_kwargs["run_jar"] = run_jar
    if run_python is not None:
        operator_kwargs["run_python"] = run_python

    # JSON-serialisable submit payload (region as string, no SDK enum).
    submit_payload = {
        "name": name,
        "runtime": runtime_name,
        "version": version,
        "region": setup_info["aws_region"],
        "environment": environment,
    }
    if run_jar is not None:
        submit_payload["run_jar"] = run_jar
    if run_python is not None:
        submit_payload["run_python"] = run_python

    return {
        "operator_kwargs": operator_kwargs,
        "submit_payload": submit_payload,
    }


def execute_wherobots_job(
    setup_info: dict,
    package_info: dict,
    jar_info: dict,
    module_name: str,
    class_name: str,
    extra_spark_conf: dict,
    spark_cluster_size: str,
    spark_cluster_desired_worker_cores: str,
    spark_cluster_desired_workers: str,
    wherobots_role_arn: str,
    task_id: str,
    context,
    version: str = "preview",
) -> dict:
    """Submit and wait for a Wherobots job."""
    if not WHEROBOTS_AVAILABLE:
        raise ImportError("Wherobots dependencies are not installed")

    built = build_wherobots_operator_kwargs(
        setup_info=setup_info,
        package_info=package_info,
        jar_info=jar_info,
        module_name=module_name,
        class_name=class_name,
        extra_spark_conf=extra_spark_conf,
        spark_cluster_size=spark_cluster_size,
        spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
        spark_cluster_desired_workers=spark_cluster_desired_workers,
        wherobots_role_arn=wherobots_role_arn,
        task_id=task_id,
        version=version,
        resolve_region=True,
    )

    platform_operator = WherobotsRunOperator(**built["operator_kwargs"])
    ti = context.get("ti") if hasattr(context, "get") else None
    original_xcom_push = getattr(ti, "xcom_push", None)
    early_xcom_pushed = False
    captured_job_url = None

    if callable(original_xcom_push):

        def _xcom_push_with_early_agnostic(*args, **kwargs):
            nonlocal early_xcom_pushed, captured_job_url
            result = original_xcom_push(*args, **kwargs)
            key = kwargs.get("key") if "key" in kwargs else (args[0] if args else None)
            value = (
                kwargs.get("value") if "value" in kwargs else (args[1] if len(args) > 1 else None)
            )
            if key == "run_id" and value and not early_xcom_pushed:
                job_url = _build_wherobots_run_url(platform_operator, str(value))
                if job_url:
                    original_xcom_push(
                        key="spark_agnostic",
                        value=_build_agnostic_xcom_payload(setup_info, job_url=job_url),
                    )
                    early_xcom_pushed = True
                    captured_job_url = job_url
            return result

        ti.xcom_push = _xcom_push_with_early_agnostic

    try:
        platform_operator.execute(context)
    finally:
        if callable(original_xcom_push):
            ti.xcom_push = original_xcom_push

    result = {"platform_operator": platform_operator}
    if captured_job_url:
        result["job_url"] = captured_job_url
    return result
