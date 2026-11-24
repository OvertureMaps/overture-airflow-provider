"""Spark-agnostic TaskGroup factories.

Two public entry points:

- ``spark_agnostic_task_group`` — single TaskGroup that runs one Spark job on
  a caller-selected platform (Glue, Databricks, Wherobots, or the
  development-only LOCAL platform).
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
        max_timeout_hours="{{ params.timeout_hours }}",
        ...
    )

Config dataclasses (``WherobotsConfig``, ``GlueConfig``, etc.) are Python
objects and are not Jinja-templatable; populate their fields from Airflow
Variables or environment variables before constructing them. ``IcebergConfig``
is the exception: its JSON config fields are forwarded as ``op_kwargs`` strings,
so Jinja in them (e.g. ``{{ var.value.managed_bucket_iceberg }}``) renders at
task execution time.

See ``overture_airflow_provider.config`` for the dataclasses callers should
construct.
"""

import dataclasses

from overture_airflow_provider._airflow_compat import get_current_context, task, task_group
from overture_airflow_provider._operator import SparkAgnosticExecuteOperator
from overture_airflow_provider._setup import setup_spark_job
from overture_airflow_provider.config import (
    DEFAULT_MAX_TIMEOUT_HOURS,
    ArtifactStoreConfig,
    DatabricksConfig,
    GlueConfig,
    IcebergConfig,
    LocalSparkConfig,
    PackageRegistryConfig,
    ReportIssueConfig,
    WherobotsConfig,
    coerce_config_dict,
)
from overture_airflow_provider.iceberg import resolve_iceberg_spark_config
from overture_airflow_provider.setup_info import rehydrate, to_xcom
from overture_airflow_provider.spark import SparkFamily, SparkImpl
from overture_airflow_provider.spark_platform_handlers import get_platform_handler

# Task Documentation for these two: see test_task_doc_md.py for why doc_md is
# passed explicitly here instead of relying on @task's docstring auto-populate.
_SETUP_TASK_DOC = """\
Resolve versions, build run identifier, project setup_info to XCom.

All string args are Jinja-rendered by Airflow before this function runs
(they are `op_kwargs` on the underlying PythonOperator).
"""

_SETUP_CLUSTER_TASK_DOC = """\
Compute merged Spark config and (for Databricks) the cluster spec.

On Databricks this task also stages the bundled runner notebook and cluster
init script to content-hash-keyed workspace paths (unless
`DatabricksConfig.stage_workspace_assets=False`).

The four `iceberg_*_config` JSON strings are the `IcebergConfig` fields
passed as `op_kwargs` (not the dataclass itself), so Airflow renders any
Jinja in them before this task runs. They are reassembled into an
`IcebergConfig` and the platform variant is selected here.
"""

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
    retries: int = 0,
    iceberg_config: IcebergConfig | None = None,
    python_download_pool: str | None = None,
    scala_download_pool: str | None = None,
    package_registry: PackageRegistryConfig | None = None,
    artifact_store: ArtifactStoreConfig | None = None,
    glue_config: GlueConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    wherobots_config: WherobotsConfig | None = None,
    local_spark_config: LocalSparkConfig | None = None,
    max_timeout_hours: str = DEFAULT_MAX_TIMEOUT_HOURS,
    report_issue_config: ReportIssueConfig | None = None,
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
        retries: Retry count for the ``execute_spark_job`` task (default 0).
            Safe to raise: a failure that never reached the platform (submit
            or config fault), or a Glue run rejected for capacity before any
            job code ran, raises a retryable ``AirflowException``; a failure
            after the job ran raises ``AirflowFailException``, which Airflow
            never retries regardless of this setting. The setup tasks
            (``setup``, ``download_python_packages``, ``download_jars``,
            ``setup_cluster``) always retry twice.
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
        local_spark_config: Development-only ``LOCAL`` platform settings (master,
            interpreter, S3A, Iceberg override). ``LOCAL`` runs refuse to start
            unless the worker sets ``OVERTURE_AIRFLOW_PROVIDER_ALLOW_LOCAL_SPARK=1``.
        max_timeout_hours: Job run timeout, in hours. Defaults to ``"8"``.

        report_issue_config: Opt-in "Report Issue" operator link. Off by
            default; when enabled it adds a link on ``execute_spark_job`` that
            opens a pre-filled "create issue" form on the configured tracker
            (GitHub built in; pluggable). Requires a target (e.g. ``"owner/repo"``).

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
        local_spark_config=local_spark_config,
        max_timeout_hours=max_timeout_hours,
        report_issue_config=report_issue_config,
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


def _local_iceberg_override_active(
    spark_impl_name: str, local_spark_config: LocalSparkConfig | None
) -> bool | None:
    """Whether ``LocalSparkConfig.iceberg_spark_config`` replaces the shared config.

    ``None`` when that depends on a templated ``spark_impl_name``.
    """
    if local_spark_config is None:
        return False
    try:
        has_override = bool(
            coerce_config_dict(
                local_spark_config.iceberg_spark_config,
                field_name="LocalSparkConfig.iceberg_spark_config",
            )
        )
    except ValueError:
        return False
    if not has_override:
        return False
    if spark_impl_name not in SparkImpl.__members__:
        return None
    return SparkImpl[spark_impl_name].get_family() == SparkFamily.LOCAL


_RAW_START, _RAW_END = "{% raw %}", "{% endraw %}"


def _defer_templating(value):
    """Shield ``value`` from task templating so it is rendered only if used."""
    return f"{_RAW_START}{value}{_RAW_END}" if isinstance(value, str) else value


def _render_deferred(value, context):
    return context["task"].render_template(value, context)


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
    retries: int = 0,
    max_active_tis_per_dagrun: int | None = None,
    iceberg_config: IcebergConfig | None = None,
    python_download_pool: str | None = None,
    scala_download_pool: str | None = None,
    package_registry: PackageRegistryConfig | None = None,
    artifact_store: ArtifactStoreConfig | None = None,
    glue_config: GlueConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    wherobots_config: WherobotsConfig | None = None,
    local_spark_config: LocalSparkConfig | None = None,
    max_timeout_hours: str = DEFAULT_MAX_TIMEOUT_HOURS,
    report_issue_config: ReportIssueConfig | None = None,
):
    """Internal task-group implementation. See ``spark_agnostic_task_group``."""

    execute_kwargs = {"pool": pool, "retries": retries}
    if max_active_tis_per_dagrun is not None:
        execute_kwargs["max_active_tis_per_dagrun"] = max_active_tis_per_dagrun

    report_issue_payload = (
        report_issue_config.to_operator_payload()
        if report_issue_config is not None and report_issue_config.active
        else None
    )

    @task(task_id="setup", retries=2, doc_md=_SETUP_TASK_DOC)
    def setup_task(
        spark_impl_name: str,
        sedona_version: str,
        module_name: str,
        class_name: str,
        job_name: str,
        parameters: str,
        spark_jar_paths: str,
    ):
        """Resolve versions, build run identifier, project setup_info to XCom."""
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
            local_spark_config=local_spark_config,
        )
        print(f"Platform: {setup_info['spark_family'].name}")
        print(f"Spark / Sedona: {setup_info['spark_version']} / {setup_info['sedona_version']}")
        return to_xcom(setup_info)

    @task(task_id="download_python_packages", pool=python_download_pool, retries=2)
    def download_packages_task(setup_info: dict, python_packages: str = ""):
        full = rehydrate(setup_info)
        return get_platform_handler(full["spark_family"], full).download_python_packages(
            python_packages or ""
        )

    @task(task_id="download_jars", pool=scala_download_pool, retries=2)
    def download_jars_task(setup_info: dict):
        full = rehydrate(setup_info)
        return get_platform_handler(full["spark_family"], full).download_jars()

    @task(task_id="setup_cluster", retries=2, doc_md=_SETUP_CLUSTER_TASK_DOC)
    def setup_cluster_task(
        setup_info: dict,
        extra_spark_conf: str = "{}",
        extra_spark_env_vars: str = "{}",
        python_packages: str = "",
        spark_jar_paths: str = "",
        spark_cluster_desired_worker_cores: str = "",
        spark_cluster_desired_workers: str = "",
        iceberg_primary_config: str = "{}",
        iceberg_wherobots_config: str = "{}",
        iceberg_s3tables_config: str = "{}",
        iceberg_wherobots_s3tables_config: str = "{}",
        iceberg_templating_deferred: bool = False,
    ):
        """Compute merged Spark config and (for Databricks) the cluster spec."""
        full = rehydrate(setup_info)
        iceberg_fields = {
            "spark_config": iceberg_primary_config,
            "wherobots_spark_config": iceberg_wherobots_config,
            "s3tables_spark_config": iceberg_s3tables_config,
            "wherobots_s3tables_spark_config": iceberg_wherobots_s3tables_config,
        }
        local_override = full.get("local_iceberg_spark_config")
        if iceberg_templating_deferred:
            if full["spark_family"] == SparkFamily.LOCAL and local_override:
                iceberg_fields = {}
            else:
                context = get_current_context()
                iceberg_fields = {
                    k: _render_deferred(v, context) for k, v in iceberg_fields.items()
                }
        iceberg_config = IcebergConfig(**iceberg_fields)
        return get_platform_handler(full["spark_family"], full).setup_cluster(
            python_packages=python_packages,
            spark_jar_paths=spark_jar_paths,
            extra_spark_conf=coerce_config_dict(extra_spark_conf, field_name="extra_spark_conf"),
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            iceberg_spark_config=resolve_iceberg_spark_config(
                iceberg_config,
                setup_info["spark_family_name"],
                local_override=local_override,
            ),
        )

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
    iceberg_config = iceberg_config or IcebergConfig()
    local_override_active = _local_iceberg_override_active(spark_impl_name, local_spark_config)
    if local_override_active is None and any(
        "endraw" in (value or "") for value in dataclasses.astuple(iceberg_config)
    ):
        # Cannot be shielded safely; template eagerly as before.
        local_override_active = False
    if local_override_active:
        # The shared config is unused, so do not template or parse it.
        iceberg_config = IcebergConfig()
    elif local_override_active is None:
        iceberg_config = IcebergConfig(
            spark_config=_defer_templating(iceberg_config.spark_config),
            wherobots_spark_config=_defer_templating(iceberg_config.wherobots_spark_config),
            s3tables_spark_config=_defer_templating(iceberg_config.s3tables_spark_config),
            wherobots_s3tables_spark_config=_defer_templating(
                iceberg_config.wherobots_s3tables_spark_config
            ),
        )
    cluster_result = setup_cluster_task(
        setup_info=setup_result,
        extra_spark_conf=extra_spark_conf,
        extra_spark_env_vars=extra_spark_env_vars,
        python_packages=python_packages,
        spark_jar_paths=spark_jar_paths,
        spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
        spark_cluster_desired_workers=spark_cluster_desired_workers,
        iceberg_primary_config=iceberg_config.spark_config,
        iceberg_wherobots_config=iceberg_config.wherobots_spark_config,
        iceberg_s3tables_config=iceberg_config.s3tables_spark_config,
        iceberg_wherobots_s3tables_config=iceberg_config.wherobots_s3tables_spark_config,
        iceberg_templating_deferred=local_override_active is None,
    )

    SparkAgnosticExecuteOperator(
        task_id="execute_spark_job",
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
        max_timeout_hours=max_timeout_hours,
        report_issue_config=report_issue_payload,
        **execute_kwargs,
    )
