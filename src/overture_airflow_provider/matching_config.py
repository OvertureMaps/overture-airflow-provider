"""Caller-supplied execution and Corpus transport settings for matching."""

from dataclasses import dataclass

from overture_airflow_provider.config import (
    ArtifactStoreConfig,
    GlueConfig,
    IcebergConfig,
    PackageRegistryConfig,
    ReportIssueConfig,
    _require_non_empty,
)


@dataclass(frozen=True)
class GlueMatchingConfig:
    """Run the published Scala matcher through the existing Spark task group.

    Artifact paths and versions may be templated strings. Registry, storage and
    platform config objects have the same templating rules as the Spark API.
    Theme-specific JARs, files and tuning are supplied by the caller.
    """

    spark_jar_paths: str
    sedona_version: str
    package_registry: PackageRegistryConfig
    artifact_store: ArtifactStoreConfig
    glue_config: GlueConfig
    spark_impl_name: str = "GLUE_v5"
    extra_spark_conf: str = "{}"
    extra_spark_env_vars: str = "{}"
    extra_files: str = ""
    pool: str = "default_pool"
    python_download_pool: str | None = None
    scala_download_pool: str | None = None
    retries: int = 0
    max_timeout_hours: str = "8"
    iceberg_config: IcebergConfig | None = None
    report_issue_config: ReportIssueConfig | None = None

    def __post_init__(self):
        _require_non_empty(
            type(self).__name__,
            spark_jar_paths=self.spark_jar_paths,
            sedona_version=self.sedona_version,
        )
        if not self.spark_impl_name.startswith("GLUE_"):
            raise ValueError("GlueMatchingConfig supports only explicit GLUE_* implementations")


@dataclass(frozen=True)
class DatabricksMatchingConfig:
    """Invoke an existing multi-task Databricks matching job.

    The job must return ``resultDataPaths`` from exactly one task whose key
    contains ``output_task_key_contains``. No connection or job name is inferred
    from the theme or environment. Polling occupies an Airflow worker, matching
    the existing named-job integration.
    """

    databricks_conn_id: str
    job_name: str
    output_task_key_contains: str = "assign"
    timeout_seconds: int = 43200
    poll_interval_seconds: int = 30
    pool: str = "default_pool"

    def __post_init__(self):
        _require_non_empty(
            type(self).__name__,
            databricks_conn_id=self.databricks_conn_id,
            job_name=self.job_name,
            output_task_key_contains=self.output_task_key_contains,
        )
        if self.timeout_seconds <= 0 or self.poll_interval_seconds <= 0:
            raise ValueError("Databricks matching timeout and poll interval must be positive")


@dataclass(frozen=True)
class CorpusDagConfig:
    """Transport contract for caller-deployed Corpus DAGs.

    Child runs are waited on and failures propagate. Stable child run IDs make
    clearing a trigger fail rather than blindly replay an already committed
    load; inspect/recover the child run before retrying it.

    Non-default branches are treated as existing unless ``create_branch`` is
    enabled explicitly. Branch creation failures are never ignored.
    """

    export_dag_id: str
    load_dag_id: str
    export_output_task_id: str
    export_output_key: str = "return_value"
    create_branch_dag_id: str | None = None
    create_branch: bool = False
    default_branch: str = "main"
    export_format: str = "parquet"
    combine_output: bool = True
    spark_impl_name: str | None = None
    export_pool: str = "default_pool"
    load_pool: str = "default_pool"
    deferrable: bool = True

    def __post_init__(self):
        _require_non_empty(
            type(self).__name__,
            export_dag_id=self.export_dag_id,
            load_dag_id=self.load_dag_id,
            export_output_task_id=self.export_output_task_id,
            default_branch=self.default_branch,
            export_format=self.export_format,
        )
        if self.create_branch and not self.create_branch_dag_id:
            raise ValueError("create_branch requires create_branch_dag_id")
