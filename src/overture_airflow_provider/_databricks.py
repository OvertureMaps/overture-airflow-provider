"""Databricks execution: cluster setup + job submission."""

import json

import requests

from overture_airflow_provider.cluster_sizing import DatabricksClusterSize

# Default DBFS prefix used when callers pass bare jar filenames (no scheme).
_DEFAULT_DBFS_JAR_PREFIX = "dbfs:/FileStore/deploy/"


def discover_gpu_cluster_options(
    databricks_conn_id: str, *, need_nodes: bool = True, need_runtime: bool = True
) -> dict:
    """Discover GPU node types and/or a GPU ML runtime from the workspace.

    Queries the connected Databricks workspace via the ``databricks-sdk`` so
    callers don't have to hand-maintain cloud-specific GPU SKUs. Builds a single
    ``WorkspaceClient`` (one connection lookup) and makes only the lookups the
    caller asks for, so a pinned field never triggers a needless API round trip.

    Args:
        databricks_conn_id: Airflow connection ID for the workspace.
        need_nodes: When ``True``, call ``list_node_types`` and return
            ``worker_instance_types`` (``{node_type_id: cores}`` for every
            non-deprecated GPU node) plus ``driver_node_type`` (smallest GPU
            node, since a GPU runtime needs a GPU-capable driver).
        need_runtime: When ``True``, call ``select_spark_version`` and return the
            latest LTS GPU ML runtime as ``spark_version``.

    Returns a dict with keys ``worker_instance_types``, ``driver_node_type`` and
    ``spark_version``; entries not requested are ``None``. Raises ``ValueError``
    if node types are requested but the workspace exposes no usable GPU nodes
    (no silent CPU fallback).
    """
    # Lazy import: the databricks-sdk lives in the optional [databricks] extra.
    from databricks.sdk import WorkspaceClient

    from overture_airflow_provider._airflow_compat import BaseHook

    conn = BaseHook.get_connection(databricks_conn_id)
    client = WorkspaceClient(host=conn.host, token=conn.password)

    result = {"worker_instance_types": None, "driver_node_type": None, "spark_version": None}

    if need_nodes:
        node_types = getattr(client.clusters.list_node_types(), "node_types", None) or []
        worker_instance_types = {}
        for node in node_types:
            if (getattr(node, "num_gpus", 0) or 0) < 1:
                continue
            if getattr(node, "is_deprecated", False):
                continue
            cores = getattr(node, "num_cores", None)
            node_id = getattr(node, "node_type_id", None)
            if node_id and cores and int(cores) > 0:
                worker_instance_types[node_id] = int(cores)

        if not worker_instance_types:
            raise ValueError(
                f"Databricks workspace for connection {databricks_conn_id!r} exposes no "
                "GPU-capable node types; cannot satisfy gpu=True"
            )

        result["worker_instance_types"] = worker_instance_types
        result["driver_node_type"] = min(worker_instance_types, key=worker_instance_types.get)

    if need_runtime:
        result["spark_version"] = client.clusters.select_spark_version(
            long_term_support=True, ml=True, gpu=True
        )

    return result


def _resolve_databricks_node_config(setup_info: dict) -> dict:
    """Merge explicit Databricks node overrides with optional GPU discovery.

    Explicit fields always win per-field; GPU discovery only fills the gaps.
    Workspace lookups are lazy: node types are fetched only when the worker
    catalog or driver is missing, the runtime only when ``spark_version`` is
    missing, and nothing is fetched when all three are pinned.
    """
    worker_instance_types = setup_info.get("databricks_worker_instance_types") or None
    driver_node_type = setup_info.get("databricks_driver_node_type") or None
    spark_version = setup_info.get("databricks_spark_version") or None

    if setup_info.get("databricks_gpu"):
        need_nodes = not (worker_instance_types and driver_node_type)
        need_runtime = not spark_version

        if need_nodes or need_runtime:
            conn_id = setup_info["databricks_conf"].get("databricks_conn_id", "databricks_default")
            discovered = discover_gpu_cluster_options(
                conn_id, need_nodes=need_nodes, need_runtime=need_runtime
            )
            if need_nodes:
                worker_instance_types = worker_instance_types or discovered["worker_instance_types"]
                driver_node_type = driver_node_type or discovered["driver_node_type"]
            if need_runtime:
                spark_version = spark_version or discovered["spark_version"]

        if "gpu" not in spark_version.lower():
            print(
                f"[Databricks] WARNING: gpu=True but spark_version {spark_version!r} "
                "does not look GPU-enabled; the cluster may not expose GPUs"
            )

    return {
        "worker_instance_types": worker_instance_types,
        "driver_node_type": driver_node_type,
        "spark_version": spark_version,
    }


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

    node_config = _resolve_databricks_node_config(setup_info)

    new_cluster = {
        **DatabricksClusterSize.from_desired_cores(
            int(spark_cluster_desired_worker_cores),
            (int(spark_cluster_desired_workers) if spark_cluster_desired_workers else None),
            instance_types=node_config["worker_instance_types"],
            driver_node_type=node_config["driver_node_type"],
        ),
        "spark_version": (node_config["spark_version"] or spark_impl.get_native_version()),
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
            "SPARK_JAR_PATHS": spark_jar_paths,
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
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )

    from overture_airflow_provider._airflow_compat import BaseHook

    built = build_databricks_operator_kwargs(
        setup_info=setup_info,
        cluster_info=cluster_info,
        module_name=module_name,
        class_name=class_name,
        task_id=task_id,
    )

    print(f"Databricks cluster config: {cluster_info['new_cluster']}")

    platform_operator = DatabricksSubmitRunOperator(**built["operator_kwargs"])

    platform_operator.execute(context)

    job_url = platform_operator.xcom_pull(context, key="run_page_url")

    conn = BaseHook.get_connection(cluster_info["databricks_conf"]["databricks_conn_id"])
    databricks_host = conn.host
    databricks_token = conn.password
    headers = {"Authorization": f"Bearer {databricks_token}"}
    get_run_url = f"{databricks_host}/api/2.0/jobs/runs/get"
    params = {"run_id": platform_operator.xcom_pull(context, key="run_id")}

    status = requests.get(get_run_url, headers=headers, params=params, timeout=30).json()

    return {
        "job_url": job_url,
        "status": status,
        "platform_operator": platform_operator,
    }
