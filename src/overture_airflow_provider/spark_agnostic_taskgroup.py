"""Spark-agnostic TaskGroup factories.

Two public entry points:

- ``spark_agnostic_task_group`` — single TaskGroup that runs one Spark job on
  a caller-selected platform (Glue, Databricks, or Wherobots).
- ``spark_agnostic_mapped_task_group`` — same TaskGroup, dynamically mapped
  over a list of parameter sets so one DAG can fan out N parallel Spark jobs.

Both expand into the same five-task pipeline in the Airflow UI::

    setup -> [download_python_packages, download_jars, setup_cluster] -> execute_spark_job

Platform selection happens at runtime inside ``setup`` based on the resolved
``spark_impl_name``. Only the config objects relevant to the chosen platform
need to be passed; the others are ignored.

Spark configuration merge order (later entries win)::

    platform_defaults -> iceberg_config -> extra_spark_conf

**Jinja templating**

All string parameters (``spark_impl_name``, ``sedona_version``,
``module_name``, ``class_name``, ``parameters``, ``python_packages``,
``extra_spark_conf``, ``extra_spark_env_vars``, cluster sizing strings, etc.)
are passed as explicit ``op_kwargs`` to every ``@task`` that uses them.
Airflow's ``PythonOperator`` renders all ``op_kwargs`` before calling the
function, so standard Jinja syntax works without any manual rendering::

    spark_agnostic_task_group(
        group_id="my_job",
        spark_impl_name="{{ params.SparkImpl }}",
        parameters='{"run_date": "{{ ds }}"}',
        ...
    )

Config dataclasses (``IcebergConfig``, ``WherobotsConfig``, etc.) are Python
objects and are not Jinja-templatable; populate their fields from Airflow
Variables or environment variables before constructing them.

See ``overture_airflow_provider.config`` for the dataclasses callers should
construct.
"""

import datetime
import json
from typing import Any

from overture_airflow_provider._airflow_compat import task, task_group
from overture_airflow_provider._setup import setup_spark_job
from overture_airflow_provider.config import (
    ArtifactStoreConfig,
    DatabricksConfig,
    GlueConfig,
    IcebergConfig,
    PackageRegistryConfig,
    WherobotsConfig,
)
from overture_airflow_provider.setup_info import rehydrate, to_xcom
from overture_airflow_provider.spark_platform_handlers import get_platform_handler

# =============================================================================
# Public API
# =============================================================================


def spark_agnostic_task_group(
    group_id: str,
    *,
    spark_impl_name: str,
    sedona_version: str,
    job_name: str = "",
    module_name: str = "",
    class_name: str = "",
    python_packages: str = "",
    spark_jar_paths: str = "",
    spark_cluster_desired_worker_cores: str = "",
    spark_cluster_size_name: str = "",
    spark_cluster_desired_workers: str = "",
    extra_spark_conf: str = "{}",
    extra_spark_env_vars: str = "{}",
    parameters: str = "{}",
    pool: str = "default_pool",
    retries: int = 1,
    iceberg_config: IcebergConfig | None = None,
    python_download_pool: str | None = None,
    scala_download_pool: str | None = None,
    package_registry: PackageRegistryConfig | None = None,
    artifact_store: ArtifactStoreConfig | None = None,
    glue_config: GlueConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    wherobots_config: WherobotsConfig | None = None,
):
    """Create a TaskGroup that runs one Spark job on the platform selected at
    runtime by ``spark_impl_name``.

    Args:
        group_id: TaskGroup ID shown in the Airflow UI.
        spark_impl_name: ``SparkImpl`` enum name, e.g. ``"GLUE_v5"``. Accepts
            Jinja templates (``"{{ params.SparkImpl }}"``).
        sedona_version: Apache Sedona version (e.g. ``"1.7.0"``).
        job_name: Optional display-name suffix appended to ``module_name.class_name``.
        module_name: Python module path for PySpark jobs. Empty for Scala jobs.
        class_name: Scala main class or PySpark entry-point class name.
        python_packages: Space-separated package specs to download from the
            registry (e.g. ``"my-spark-pkg==1.0 numba"``).
        spark_jar_paths: Comma-separated JAR paths or S3/DBFS URIs.
        spark_cluster_desired_worker_cores: Total desired worker vCPUs.
        spark_cluster_size_name: Named cluster size (Wherobots only).
        spark_cluster_desired_workers: Explicit worker count.
        extra_spark_conf: JSON string of Spark config overrides (merged on top
            of platform defaults and ``iceberg_config``).
        extra_spark_env_vars: JSON string of extra env vars for driver/executor.
        parameters: Job parameters as a JSON string passed to the entry point.
        pool: Airflow pool for the ``execute_spark_job`` task.
        retries: Retry count for the ``execute_spark_job`` task.
        iceberg_config: Iceberg Spark config for both Glue/Databricks and
            Wherobots; the right variant is selected at runtime. Pass ``None``
            for jobs that don't use Iceberg.
        python_download_pool: Pool for the ``download_python_packages`` task.
        scala_download_pool: Pool for the ``download_jars`` task.
        package_registry: Private PyPI-compatible registry credentials.
            Required for Glue and Wherobots runs (apache-sedona is always
            downloaded from this registry); for Databricks runs, only required
            when ``python_packages`` is non-empty.
        artifact_store: S3 bucket and path settings for caching wheels/JARs/scripts.
            Required for Glue and Wherobots.
        glue_config: AWS Glue settings (IAM role, execution class).
        databricks_config: Databricks cluster settings (connection id, custom
            tags, init-script/workspace paths, GPU node overrides). Optional: when
            omitted, a default ``DatabricksConfig()`` is used, which connects via
            the ``databricks_default`` connection and the default workspace paths.
        wherobots_config: Wherobots execution settings (role ARN, external ID,
            AWS region). Required for Wherobots runs that use Iceberg.

    Returns:
        TaskGroup containing five tasks: ``setup``, ``download_python_packages``,
        ``download_jars``, ``setup_cluster``, ``execute_spark_job``.
    """
    return _spark_agnostic_task_group.override(group_id=group_id)(
        spark_impl_name=spark_impl_name,
        sedona_version=sedona_version,
        job_name=job_name,
        module_name=module_name,
        class_name=class_name,
        python_packages=python_packages,
        spark_jar_paths=spark_jar_paths,
        spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
        spark_cluster_size_name=spark_cluster_size_name,
        spark_cluster_desired_workers=spark_cluster_desired_workers,
        extra_spark_conf=extra_spark_conf,
        extra_spark_env_vars=extra_spark_env_vars,
        parameters=parameters,
        pool=pool,
        retries=retries,
        iceberg_config=iceberg_config,
        python_download_pool=python_download_pool,
        scala_download_pool=scala_download_pool,
        package_registry=package_registry,
        artifact_store=artifact_store,
        glue_config=glue_config,
        databricks_config=databricks_config,
        wherobots_config=wherobots_config,
    )


def spark_agnostic_mapped_task_group(
    group_id: str,
    parameters_list,
    *,
    max_active_tis_per_dagrun: int | None = None,
    **kwargs,
):
    """Dynamically map ``spark_agnostic_task_group`` over parameter sets.

    One TaskGroup instance is created per element in ``parameters_list``. All
    other arguments (forwarded as ``**kwargs``) are constant across mapped
    instances. Accepts every keyword argument that
    ``spark_agnostic_task_group`` does, except ``parameters`` (the mapped
    value) and ``group_id`` (positional).

    Args:
        group_id: TaskGroup ID shown in the Airflow UI.
        parameters_list: ``XComArg`` or list of JSON strings, one per instance.
        max_active_tis_per_dagrun: Cap concurrent mapped instances per DAG run.
        **kwargs: Forwarded to ``spark_agnostic_task_group``.

    Returns:
        Mapped TaskGroup (one instance per element in ``parameters_list``).
    """
    return (
        _spark_agnostic_task_group.override(group_id=group_id)
        .partial(max_active_tis_per_dagrun=max_active_tis_per_dagrun, **kwargs)
        .expand(parameters=parameters_list)
    )


# =============================================================================
# Internal implementation
# =============================================================================


def _parse_json_or_dict(value: Any) -> dict:
    """Accept either a parsed dict or a JSON string and return a dict."""
    if isinstance(value, dict):
        return value
    if value and value != "{}":
        return json.loads(value)
    return {}


def _select_iceberg_conf(iceberg_config: IcebergConfig | None, spark_family_name: str) -> dict:
    """Pick the right Iceberg config variant for the resolved platform family."""
    if iceberg_config is None:
        return {}
    raw = (
        iceberg_config.wherobots_spark_config
        if spark_family_name == "WHEROBOTS"
        else iceberg_config.spark_config
    )
    return json.loads(raw) if raw and raw != "{}" else {}


def _xcom_datetime_default(obj):
    """JSON serializer that handles datetime objects pushed through XCom."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


@task_group
def _spark_agnostic_task_group(
    *,
    spark_impl_name: str,
    sedona_version: str,
    job_name: str = "",
    module_name: str = "",
    class_name: str = "",
    python_packages: str = "",
    spark_jar_paths: str = "",
    spark_cluster_desired_worker_cores: str = "",
    spark_cluster_size_name: str = "",
    spark_cluster_desired_workers: str = "",
    extra_spark_conf: str = "{}",
    extra_spark_env_vars: str = "{}",
    parameters: str = "{}",
    pool: str = "default_pool",
    retries: int = 1,
    max_active_tis_per_dagrun: int | None = None,
    iceberg_config: IcebergConfig | None = None,
    python_download_pool: str | None = None,
    scala_download_pool: str | None = None,
    package_registry: PackageRegistryConfig | None = None,
    artifact_store: ArtifactStoreConfig | None = None,
    glue_config: GlueConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    wherobots_config: WherobotsConfig | None = None,
):
    """Internal task-group implementation. See ``spark_agnostic_task_group``."""

    execute_kwargs = {"pool": pool, "retries": retries}
    if max_active_tis_per_dagrun is not None:
        execute_kwargs["max_active_tis_per_dagrun"] = max_active_tis_per_dagrun

    @task(task_id="setup")
    def setup_task(
        spark_impl_name: str,
        sedona_version: str,
        module_name: str,
        class_name: str,
        job_name: str,
        parameters: str,
        spark_jar_paths: str,
    ):
        """Resolve versions, build run identifier, project setup_info to XCom.

        All string args are Jinja-rendered by Airflow before this function
        runs (they are ``op_kwargs`` on the underlying PythonOperator).
        """
        setup_info = setup_spark_job(
            spark_impl_name=spark_impl_name,
            sedona_version=sedona_version,
            module_name=module_name,
            class_name=class_name,
            job_name=job_name,
            parameters=parameters,
            spark_jar_paths=spark_jar_paths,
            package_registry=package_registry,
            artifact_store=artifact_store,
            glue_config=glue_config,
            databricks_config=databricks_config,
            wherobots_config=wherobots_config,
        )
        print(f"Platform: {setup_info['spark_family'].name}")
        print(f"Spark / Sedona: {setup_info['spark_version']} / {setup_info['sedona_version']}")
        return to_xcom(setup_info)

    @task(task_id="download_python_packages", pool=python_download_pool)
    def download_packages_task(setup_info: dict, python_packages: str = ""):
        full = rehydrate(setup_info)
        return get_platform_handler(full["spark_family"], full).download_python_packages(
            python_packages or ""
        )

    @task(task_id="download_jars", pool=scala_download_pool)
    def download_jars_task(setup_info: dict):
        full = rehydrate(setup_info)
        return get_platform_handler(full["spark_family"], full).download_jars()

    @task(task_id="setup_cluster")
    def setup_cluster_task(
        setup_info: dict,
        extra_spark_conf: str = "{}",
        extra_spark_env_vars: str = "{}",
        python_packages: str = "",
        spark_jar_paths: str = "",
        spark_cluster_desired_worker_cores: str = "",
        spark_cluster_desired_workers: str = "",
    ):
        """Compute merged Spark config and (for Databricks) the cluster spec."""
        full = rehydrate(setup_info)
        return get_platform_handler(full["spark_family"], full).setup_cluster(
            python_packages=python_packages,
            spark_jar_paths=spark_jar_paths,
            extra_spark_conf=_parse_json_or_dict(extra_spark_conf),
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            iceberg_spark_config=_select_iceberg_conf(
                iceberg_config, setup_info["spark_family_name"]
            ),
        )

    @task(task_id="execute_spark_job", **execute_kwargs)
    def execute_job_task(
        setup_info: dict,
        package_info: dict,
        jar_info: dict,
        cluster_info: dict,
        module_name: str = "",
        class_name: str = "",
        parameters: str = "{}",
        extra_spark_env_vars: str = "{}",
        spark_cluster_size_name: str = "",
        spark_cluster_desired_worker_cores: str = "",
        spark_cluster_desired_workers: str = "",
        **context,
    ):
        """Submit the Spark job to the resolved platform and wait for completion."""
        full = rehydrate(setup_info)
        merged_spark_conf = (cluster_info or {}).get("merged_spark_conf", {})

        def _int_or_none(value: str):
            return int(value) if value else None

        execution_result = get_platform_handler(full["spark_family"], full).execute_job(
            package_info=package_info,
            jar_info=jar_info,
            cluster_info=cluster_info,
            module_name=module_name,
            class_name=class_name,
            parameters=parameters,
            extra_spark_conf=merged_spark_conf,
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_size_name=spark_cluster_size_name,
            spark_cluster_desired_worker_cores=_int_or_none(spark_cluster_desired_worker_cores),
            spark_cluster_desired_workers=_int_or_none(spark_cluster_desired_workers),
            iam_role_name=setup_info.get("iam_role_name", "AWSGlueServiceRole"),
            wherobots_role_arn=setup_info.get("wherobots_role_arn", ""),
            task_id="execute_spark_job",
            context=context,
        )

        # Cross-platform XCom payload that downstream tasks rely on.
        agnostic_xcom = {
            "spark_impl": setup_info["spark_impl_name"],
            "spark_family": setup_info["spark_family_name"],
            "spark_version": setup_info["spark_version"],
            "sedona_version": setup_info["sedona_version"],
        }
        for key in ("job_url", "status"):
            if key in execution_result:
                agnostic_xcom[key] = execution_result[key]

        operator = execution_result.get("platform_operator")
        if operator and context:
            operator.xcom_push(
                context,
                "spark_agnostic",
                json.dumps(agnostic_xcom, default=_xcom_datetime_default),
            )
        return agnostic_xcom

    setup_result = setup_task(
        spark_impl_name=spark_impl_name,
        sedona_version=sedona_version,
        module_name=module_name,
        class_name=class_name,
        job_name=job_name,
        parameters=parameters,
        spark_jar_paths=spark_jar_paths,
    )
    package_result = download_packages_task(
        setup_info=setup_result,
        python_packages=python_packages,
    )
    jar_result = download_jars_task(setup_info=setup_result)
    cluster_result = setup_cluster_task(
        setup_info=setup_result,
        extra_spark_conf=extra_spark_conf,
        extra_spark_env_vars=extra_spark_env_vars,
        python_packages=python_packages,
        spark_jar_paths=spark_jar_paths,
        spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
        spark_cluster_desired_workers=spark_cluster_desired_workers,
    )

    execute_job_task(
        setup_info=setup_result,
        package_info=package_result,
        jar_info=jar_result,
        cluster_info=cluster_result,
        module_name=module_name,
        class_name=class_name,
        parameters=parameters,
        extra_spark_env_vars=extra_spark_env_vars,
        spark_cluster_size_name=spark_cluster_size_name,
        spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
        spark_cluster_desired_workers=spark_cluster_desired_workers,
    )
