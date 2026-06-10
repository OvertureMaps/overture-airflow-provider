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


def _normalize_workspace_path(path: str) -> str:
    """Return a bare Databricks workspace path, stripping the ``/Workspace`` prefix.

    ``/Workspace/...`` is the cluster FUSE-mount convention; the Workspace REST
    API (``2.0/workspace/get-status``), notebook ``notebook_path`` and workspace
    ``init_scripts`` destinations all address objects by their bare path
    (``/Shared/...``, ``/Users/...``). Stripping the prefix keeps preflight, the
    notebook task and the init-script reference consistent and API-addressable.
    """
    if path == "/Workspace":
        return "/"
    if path.startswith("/Workspace/"):
        return path[len("/Workspace") :]
    return path


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
            non-deprecated GPU node) plus ``driver_node_type`` (the cheapest CPU
            node, since the driver doesn't need a GPU — compute runs on the
            workers; falls back to the smallest GPU node only if the workspace
            exposes no CPU node).
        need_runtime: When ``True``, call ``select_spark_version`` and return the
            latest LTS GPU ML runtime as ``spark_version``.

    Returns a dict with keys ``worker_instance_types``, ``driver_node_type`` and
    ``spark_version``; entries not requested are ``None``. Raises ``ValueError``
    if node types are requested but the workspace exposes no usable GPU nodes
    (no silent CPU fallback).
    """
    # Lazy import: the hook pulls in databricks-sdk / Airflow, both optional at
    # package-import time.
    from overture_airflow_provider.hooks import DatabricksSdkHook

    result = {"worker_instance_types": None, "driver_node_type": None, "spark_version": None}

    # Single client/connection lookup; the SDK's unified auth (via the hook)
    # supports PAT, OAuth M2M, Azure, and federated service principals.
    with DatabricksSdkHook(databricks_conn_id).get_workspace_client() as client:
        if need_nodes:
            node_types = getattr(client.clusters.list_node_types(), "node_types", None) or []
            worker_instance_types = {}
            cpu_node_cores = {}
            for node in node_types:
                if getattr(node, "is_deprecated", False):
                    continue
                cores = getattr(node, "num_cores", None)
                node_id = getattr(node, "node_type_id", None)
                if not (node_id and cores and int(cores) > 0):
                    continue
                if (getattr(node, "num_gpus", 0) or 0) >= 1:
                    worker_instance_types[node_id] = int(cores)
                else:
                    cpu_node_cores[node_id] = int(cores)

            if not worker_instance_types:
                raise ValueError(
                    f"Databricks workspace for connection {databricks_conn_id!r} exposes no "
                    "GPU-capable node types; cannot satisfy gpu=True"
                )

            result["worker_instance_types"] = worker_instance_types
            # The driver doesn't need a GPU (compute runs on the workers), so
            # default it to the cheapest CPU node and avoid wasting a GPU on the
            # driver. Fall back to the smallest GPU node only if the workspace
            # exposes no CPU node.
            driver_pool = cpu_node_cores or worker_instance_types
            result["driver_node_type"] = min(driver_pool, key=driver_pool.get)

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
            {"maven": {"coordinates": f"org.apache.iceberg:iceberg-aws-bundle:{iceberg_version}"}},
        ]
        + list(extra_libraries)
        + _databricks_jar_libraries(spark_jar_paths)
    )

    databricks_conf = setup_info["databricks_conf"]
    databricks_deployed_scripts_path = _normalize_workspace_path(
        setup_info["databricks_workspace_scripts_path_template"].format(
            s3_assets_root=setup_info["s3_assets_root"]
        )
    )
    cluster_init_script_name = setup_info["databricks_cluster_init_script_name"]
    custom_tags = dict(setup_info.get("databricks_custom_tags", {}) or {})

    print(f"spark_jar_paths: {spark_jar_paths}")
    print(f"sedona_version: {sedona_version}")
    print(f"spark_version_for_sedona: {spark_version_for_sedona}")
    print(f"geotools_wrapper_version: {geotools_wrapper_version}")
    print(f"scala_version: {scala_version}")
    print(f"extra_spark_env_vars: {extra_spark_env_vars}")

    node_config = _resolve_databricks_node_config(setup_info)

    databricks_spark_conf = setup_info.get("databricks_spark_conf", {}) or {}
    databricks_spark_env_vars = setup_info.get("databricks_spark_env_vars", {}) or {}

    if setup_info.get("databricks_gpu") and not spark_cluster_desired_workers:
        print(
            "[Databricks] WARNING: gpu=True sized by worker cores only; "
            "core-based sizing is an indirect proxy for GPU count and assumes a "
            "fixed cores-per-GPU node shape. Prefer pinning "
            "spark_cluster_desired_workers (explicit node/GPU count) for GPU runs."
        )

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
            **databricks_spark_conf,
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
            # Use Java 17 (Zulu JDK) so Iceberg 1.5+ JARs (compiled for
            # Java 11, class file version 55) can run. DBR defaults to
            # Java 8; JNAME overrides the JVM before the cluster starts.
            "JNAME": "zulu17-ca-amd64",
            **databricks_spark_env_vars,
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


def _is_runner_not_found(exc: Exception) -> bool:
    """Identify a Databricks workspace "object does not exist" error.

    The workspace ``get-status`` REST endpoint returns HTTP 404 when the path
    is absent (matching how the official ``DatabricksHook.get_repo_by_path``
    treats a missing object).
    """
    response = getattr(exc, "response", None)
    return getattr(response, "status_code", None) == 404


def _workspace_object_exists(hook, path: str) -> bool:
    """Return True if a Databricks workspace object exists, False if absent (404).

    Re-raises any non-404 error so the caller can decide whether to warn-and-skip.
    """
    try:
        hook._do_api_call(
            ("GET", "2.0/workspace/get-status"),
            {"path": path},
            wrap_http_errors=False,
        )
        return True
    except Exception as exc:
        if _is_runner_not_found(exc):
            return False
        raise


def preflight_databricks_runner(setup_info: dict, cluster_info: dict) -> None:
    """Advisory check for required Databricks workspace assets.

    Unlike Glue/Wherobots — whose runners auto-upload to S3 during setup — the
    Databricks job depends on workspace assets that must be staged out-of-band
    (CI/CD or :func:`runner_assets.upload_databricks_runner_to_workspace`):

    - the runner **notebook** (``job_runner_databricks``), and
    - the cluster **init script** (``databricks_cluster_init_script_name``),
      which is wired into ``new_cluster.init_scripts`` and is *not* bundled with
      the provider.

    If either is missing the submit otherwise fails opaquely mid-run (the init
    script as a cluster-launch failure). This pre-checks the resolved workspace
    paths and prints an actionable warning so the cause is obvious up front.

    **Best-effort and never fatal.** ``2.0/workspace/get-status`` returns HTTP
    404 for *both* a truly absent object *and* a permission-denied read, so a
    principal that can *run* the notebook but lacks workspace *read* (a common,
    already-working prod setup) would otherwise be falsely blocked. Because a 404
    is ambiguous, a missing-asset result is downgraded to a loud warning and the
    run proceeds — the real submit surfaces a genuinely missing asset via
    Databricks' own run error, which a permissions gap cannot fake. Any other
    error (auth, transient HTTP, SDK/provider mismatch) is likewise warned and
    skipped, so a working deployment is never turned into a hard failure.

    The check reuses the official ``DatabricksHook`` and the same
    ``databricks_conn_id`` the submit uses — the same endpoint the provider's own
    ``get_repo_by_path`` uses.
    """
    scripts_path = cluster_info["databricks_deployed_scripts_path"]
    conn_id = cluster_info["databricks_conf"].get("databricks_conn_id", "databricks_default")

    # (workspace path, human label, remediation hint) per required asset.
    required_assets = [
        (
            f"{scripts_path}/job_runner_databricks",
            "runner notebook",
            "Deploy it via your CI/CD pipeline or overture_airflow_provider."
            "runner_assets.upload_databricks_runner_to_workspace(...).",
        ),
    ]
    init_script_name = setup_info.get("databricks_cluster_init_script_name")
    if init_script_name:
        required_assets.append(
            (
                f"{scripts_path}/{init_script_name}",
                "cluster init script",
                "Deploy it to the workspace scripts path via your CI/CD pipeline; "
                "it is not bundled with the provider.",
            )
        )

    try:
        from airflow.providers.databricks.hooks.databricks import DatabricksHook

        hook = DatabricksHook(databricks_conn_id=conn_id)
    except Exception as exc:
        print(
            f"[Databricks] WARNING: could not construct hook to verify runner assets "
            f"({type(exc).__name__}: {exc}); proceeding without preflight"
        )
        return

    for path, label, hint in required_assets:
        try:
            exists = _workspace_object_exists(hook, path)
        except Exception as exc:
            print(
                f"[Databricks] WARNING: could not verify {label} at {path} "
                f"({type(exc).__name__}: {exc}); proceeding without preflight"
            )
            continue
        if not exists:
            print(
                f"[Databricks] WARNING: {label} not found at {path} via workspace "
                "get-status. Note Databricks returns HTTP 404 for BOTH a missing "
                "object AND a permission-denied read, so this may be a false alarm "
                "when the job's principal can run but not read the asset. Proceeding; "
                f"if it is genuinely missing the run will fail with a clear error. {hint} "
                "See the README 'Databricks runner deployment' section."
            )


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
        # Databricks runs synchronously: with wait_for_termination=True and no
        # deferrable flag, execute() blocks until the run terminates, returning
        # on success or raising on failure. submit_databricks_job consumes that
        # result and returns trigger=None, so the operator finalizes the run the
        # same way it does for Wherobots. (Only Glue defers.)
        "wait_for_termination": True,
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


def submit_databricks_job(
    setup_info: dict,
    cluster_info: dict,
    module_name: str,
    class_name: str,
    parameters: str,
    task_id: str,
    context,
) -> dict:
    """Submit a Databricks run and return its result (synchronous).

    Databricks runs synchronously: the upstream ``DatabricksSubmitRunOperator``
    is built with ``wait_for_termination=True`` and no ``deferrable`` flag (see
    ``build_databricks_operator_kwargs``), so ``execute()`` blocks until the run
    terminates — it returns on success or raises ``AirflowException`` on failure.
    Like Wherobots, this returns ``trigger=None`` so the operator finalizes
    immediately rather than deferring (only Glue defers). The early
    ``spark_agnostic`` XCom is pushed here so ``SparkJobLink`` works.
    """
    # Lazy imports so the builder + render path work without the
    # [databricks] extra installed.
    from airflow.providers.databricks.hooks.databricks import DatabricksHook
    from airflow.providers.databricks.operators.databricks import (
        DatabricksSubmitRunOperator,
    )

    # Notebook jobs (module_name set) require the bundled runner notebook to be
    # pre-deployed to the workspace; fail fast with guidance if it's missing.
    if module_name:
        preflight_databricks_runner(setup_info, cluster_info)

    built = build_databricks_operator_kwargs(
        setup_info=setup_info,
        cluster_info=cluster_info,
        module_name=module_name,
        class_name=class_name,
        task_id=task_id,
    )

    print(f"Databricks cluster config: {cluster_info['new_cluster']}")

    platform_operator = DatabricksSubmitRunOperator(**built["operator_kwargs"])
    # Synchronous: wait_for_termination=True with no deferrable flag means
    # execute() blocks until the run terminates — it returns on success or
    # raises AirflowException on failure.
    platform_operator.execute(context)

    conn_id = cluster_info["databricks_conf"]["databricks_conn_id"]
    run_id = platform_operator.run_id
    hook = DatabricksHook(databricks_conn_id=conn_id)
    run_page_url = hook.get_run_page_url(run_id)

    ti = context.get("ti") if hasattr(context, "get") else None
    if ti is not None and callable(getattr(ti, "xcom_push", None)):
        ti.xcom_push(
            key="spark_agnostic",
            value=_build_agnostic_xcom_payload(setup_info, job_url=run_page_url),
        )

    result = {"job_url": run_page_url, "status": hook.get_run(run_id)}

    return {
        "trigger": None,
        "run_id": run_id,
        "run_page_url": run_page_url,
        "result": result,
        "platform_operator": platform_operator,
    }


def complete_databricks_job(
    setup_info: dict,
    cluster_info: dict,
    event: dict,
    context,
) -> dict:
    """Resolve a completed Databricks run into the final result dict.

    Databricks runs synchronously, so this is not invoked on the live path —
    like Wherobots, ``submit_databricks_job`` returns ``trigger=None`` and the
    operator finalizes without deferring. It is retained for
    ``SparkPlatformHandler`` interface parity and as the resume handler should
    Databricks deferral be enabled: it parses the ``DatabricksExecutionTrigger``
    event (``run_state`` via ``RunState`` + ``run_page_url``) and raises on a
    failed run.
    """
    from airflow.providers.databricks.hooks.databricks import DatabricksHook, RunState

    from overture_airflow_provider._airflow_compat import AirflowException

    run_id = event["run_id"]
    run_page_url = event.get("run_page_url")
    run_state = RunState.from_json(event["run_state"])
    if not run_state.is_successful:
        raise AirflowException(
            f"Databricks run {run_id} failed with state {run_state}; errors: {event.get('errors')}"
        )

    hook = DatabricksHook(databricks_conn_id=cluster_info["databricks_conf"]["databricks_conn_id"])
    status = hook.get_run(run_id)

    return {
        "job_url": run_page_url,
        "status": status,
    }
