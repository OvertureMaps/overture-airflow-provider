"""Databricks execution: cluster setup + job submission."""

import json

from overture_airflow_provider.cluster_sizing import DatabricksClusterSize

# Default DBFS prefix used when callers pass bare jar filenames (no scheme).
_DEFAULT_DBFS_JAR_PREFIX = "dbfs:/FileStore/deploy/"

# See https://iceberg.apache.org/releases
_SPARK_TO_ICEBERG_VERSION_MAP = {
    "3.3": "1.8.1",
    "3.4": "1.10.2",
    "3.5": "1.10.2",
}

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


def _databricks_jar_libraries(spark_jar_paths: str) -> list:
    """Parse caller-supplied JAR paths into Databricks library entries.

    ``spark_jar_paths`` is comma-separated. Entries already carrying a URI
    scheme (``dbfs:``, ``s3://``, ``s3a://``, ``http(s)://``, ``abfss://``,
    ``gs://``) are passed through verbatim; bare filenames are prefixed with
    the default DBFS deploy path for backwards compatibility.
    """
    if not spark_jar_paths:
        return []
    libraries = []
    for raw in spark_jar_paths.split(","):
        jar = raw.strip()
        if not jar:
            continue
        if "://" in jar or jar.startswith("dbfs:"):
            libraries.append({"jar": jar})
        else:
            libraries.append({"jar": _DEFAULT_DBFS_JAR_PREFIX + jar})
    return libraries


def setup_databricks_cluster(
    setup_info: dict,
    python_packages: str,
    spark_jar_paths: str,
    extra_spark_conf: dict,
    extra_spark_env_vars: str | dict,
    spark_cluster_desired_worker_cores: str,
    spark_cluster_desired_workers: str,
) -> dict:
    """Build the Databricks ``new_cluster`` spec, libraries, and conn config."""
    spark_impl = setup_info["spark_impl"]
    sedona_version = setup_info["sedona_version"]
    scala_version = setup_info["scala_version"]
    geotools_wrapper_version = setup_info["geotools_wrapper_version"]
    run_identifier = setup_info["run_identifier"]
    spark_version_for_sedona = setup_info["spark_version_for_sedona"]
    spark_major_minor_version = ".".join(setup_info["spark_version"].split(".")[:2])
    iceberg_version = _SPARK_TO_ICEBERG_VERSION_MAP[spark_major_minor_version]

    if isinstance(extra_spark_env_vars, str):
        extra_spark_env_vars = json.loads(extra_spark_env_vars)

    py_pi_client = setup_info["py_pi_client"]

    # DBFS layout (templates caller-supplied to keep platform paths configurable).
    dbfs_root = setup_info["databricks_dbfs_root_template"].format(
        s3_assets_root=setup_info["s3_assets_root"]
    )
    dbfs_prefix = f"{dbfs_root}/{run_identifier}"
    cluster_logs_path = f"{dbfs_prefix}/sparkLogs"

    print(f"Databricks logs_path: {cluster_logs_path}")

    # extra_libraries lets the caller pin transitive deps (e.g. numpy/geopandas)
    # without the provider hardcoding versions.
    extra_libraries = setup_info.get("databricks_extra_libraries", []) or []
    libraries = (
        [
            {"pypi": {"package": package, "repo": py_pi_client.get_url()}}
            for package in python_packages.split()
        ]
        + [
            {"pypi": {"package": "databricks-sdk"}},
            {"pypi": {"package": f"apache-sedona=={sedona_version}"}},
        ]
        + [
        # Iceberg Spark catalog plugin (SparkCatalog / RESTCatalog) + SigV4 support
        {
            "maven": {
                "coordinates": f"org.apache.iceberg:iceberg-spark-runtime-{spark_major_minor_version}_{scala_version}:{iceberg_version}"
            }
        },
        {
            "maven": {
                "coordinates": f"org.apache.iceberg:iceberg-aws-bundle:{spark_major_minor_version}"
            }
        },
        ]
        + list(extra_libraries)
        + _databricks_jar_libraries(spark_jar_paths)
        
    )

    databricks_conf = setup_info["databricks_conf"]
    databricks_deployed_scripts_path = setup_info[
        "databricks_workspace_scripts_path_template"
    ].format(s3_assets_root=setup_info["s3_assets_root"])
    cluster_init_script_name = setup_info["databricks_cluster_init_script_name"]
    custom_tags = dict(setup_info.get("databricks_custom_tags", {}) or {})

    print(f"spark_jar_paths: {spark_jar_paths}")
    print(f"sedona_version: {sedona_version}")
    print(f"spark_version_for_sedona: {spark_version_for_sedona}")
    print(f"geotools_wrapper_version: {geotools_wrapper_version}")
    print(f"scala_version: {scala_version}")
    print(f"extra_spark_env_vars: {extra_spark_env_vars}")

    new_cluster = {
        **DatabricksClusterSize.from_desired_cores(
            int(spark_cluster_desired_worker_cores),
            (int(spark_cluster_desired_workers) if spark_cluster_desired_workers else None),
        ),
        "spark_version": spark_impl.get_native_version(),
        "spark_conf": {
            "parquet.enable.summary-metadata": "false",
            "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
            "spark.kryo.registrator": "org.apache.sedona.core.serde.SedonaKryoRegistrator",
            "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
            "spark.sql.sources.commitProtocolClass": (
                "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
            ),
            "spark.sql.extensions": (
                "org.apache.sedona.viz.sql.SedonaVizExtensions,"
                "org.apache.sedona.sql.SedonaSqlExtensions"
            ),
            "spark.databricks.io.directoryCommit.createSuccessFile": "false",
            **extra_spark_conf,
        },
        "azure_attributes": {
            "first_on_demand": 1,
            "availability": "ON_DEMAND_AZURE",
            "spot_bid_max_price": -1,
        },
        "spark_env_vars": {
            "PIP_PRE": "true",
            "SPARK_JAR_PATHS": spark_jar_paths or "",
            "SEDONA_VERSION": sedona_version,
            "SPARK_VERSION": spark_version_for_sedona,
            "GEOTOOLS_VERSION": geotools_wrapper_version,
            "SCALA_VERSION": scala_version,
            **extra_spark_env_vars,
        },
        "runtime_engine": "STANDARD",
        "data_security_mode": "NONE",
        "init_scripts": [
            {
                "workspace": {
                    "destination": (
                        f"{databricks_deployed_scripts_path}/{cluster_init_script_name}"
                    ),
                }
            }
        ],
        "cluster_log_conf": {"dbfs": {"destination": cluster_logs_path}},
        "custom_tags": custom_tags,
    }

    return {
        "new_cluster": new_cluster,
        "libraries": libraries,
        "databricks_conf": databricks_conf,
        "databricks_deployed_scripts_path": databricks_deployed_scripts_path,
    }


def build_databricks_operator_kwargs(
    setup_info: dict,
    cluster_info: dict,
    module_name: str,
    class_name: str,
    task_id: str,
) -> dict:
    """Pure-Python assembly of DatabricksSubmitRunOperator kwargs.

    Side-effect-free; returns ``{"operator_kwargs", "notebook_task",
    "spark_jar_task", "submit_payload"}``. ``submit_payload`` is the JSON body
    equivalent to ``databricks jobs submit --json``.
    """
    my_parameters = setup_info["parameters"]

    if module_name:
        notebook_task = {
            "notebook_path": (
                f"{cluster_info['databricks_deployed_scripts_path']}/job_runner_databricks"
            ),
            "base_parameters": {
                "module_name": module_name,
                "class_name": class_name,
                "params": my_parameters,
            },
        }
        spark_jar_task = None
    else:
        notebook_task = None
        jar_parameters = (
            my_parameters
            if isinstance(my_parameters, str)
            else [f"{key}={value}" for key, value in my_parameters.items()]
        )
        spark_jar_task = {
            "main_class_name": class_name,
            "parameters": jar_parameters,
        }

    operator_kwargs = {
        "task_id": task_id,
        "databricks_conn_id": cluster_info["databricks_conf"].get(
            "databricks_conn_id", "databricks_default"
        ),
        "new_cluster": cluster_info["new_cluster"],
        "notebook_task": notebook_task,
        "spark_jar_task": spark_jar_task,
        "libraries": cluster_info["libraries"],
        "run_name": setup_info["run_identifier"],
    }

    # Equivalent payload for `databricks jobs submit --json @file.json`.
    submit_payload = {
        "run_name": setup_info["run_identifier"],
        "new_cluster": cluster_info["new_cluster"],
        "libraries": cluster_info["libraries"],
    }
    if notebook_task is not None:
        submit_payload["notebook_task"] = notebook_task
    if spark_jar_task is not None:
        submit_payload["spark_jar_task"] = spark_jar_task

    return {
        "operator_kwargs": operator_kwargs,
        "notebook_task": notebook_task,
        "spark_jar_task": spark_jar_task,
        "submit_payload": submit_payload,
    }


def execute_databricks_job(
    setup_info: dict,
    cluster_info: dict,
    module_name: str,
    class_name: str,
    parameters: str,
    task_id: str,
    context,
) -> dict:
    """Submit and wait for a Databricks job."""
    # Lazy imports so the builder + render path work without the
    # [databricks] extra installed.
    from airflow.providers.databricks.hooks.databricks import DatabricksHook
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )

    built = build_databricks_operator_kwargs(
        setup_info=setup_info,
        cluster_info=cluster_info,
        module_name=module_name,
        class_name=class_name,
        task_id=task_id,
    )

    print(f"Databricks cluster config: {cluster_info['new_cluster']}")

    platform_operator = DatabricksSubmitRunOperator(**built["operator_kwargs"])
    ti = context.get("ti") if isinstance(context, dict) else None
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
            if key == "run_page_url" and value and not early_xcom_pushed:
                original_xcom_push(
                    key="spark_agnostic",
                    value=_build_agnostic_xcom_payload(setup_info, job_url=value),
                )
                early_xcom_pushed = True
            return result

        ti.xcom_push = _xcom_push_with_early_agnostic

    try:
        platform_operator.execute(context)
    finally:
        if callable(original_xcom_push):
            ti.xcom_push = original_xcom_push

    job_url = platform_operator.xcom_pull(context, key="run_page_url")

    run_id = platform_operator.xcom_pull(context, key="run_id")
    hook = DatabricksHook(databricks_conn_id=cluster_info["databricks_conf"]["databricks_conn_id"])
    status = hook.get_run(run_id)

    return {
        "job_url": job_url,
        "status": status,
        "platform_operator": platform_operator,
    }
