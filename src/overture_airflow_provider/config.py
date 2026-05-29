"""Platform configuration dataclasses for ``spark_agnostic_task_group``.

Callers construct only the config objects relevant to their target platform
and pass them as single arguments rather than scattering many keyword args
across the call site.

Example::

    from overture_airflow_provider.config import (
        PackageRegistryConfig,
        ArtifactStoreConfig,
        GlueConfig,
        IcebergConfig,
    )

    spark_agnostic_task_group(
        group_id="my_job",
        spark_impl_name="{{ params.SparkImpl }}",
        sedona_version=SEDONA_VERSION,
        parameters=json.dumps(job_params),
        package_registry=PackageRegistryConfig(
            domain_owner="123456789012",
            domain="my-pypi",
            repository="my-repo",
            region="us-east-1",
            maven_repository="my-maven",
            maven_repository_path="maven/my-maven",
        ),
        artifact_store=ArtifactStoreConfig(
            s3_bucket="my-glue-assets-bucket",
            force_pip_packages=["sentence-transformers"],
        ),
        glue_config=GlueConfig(),
        iceberg_config=IcebergConfig(spark_config="{}"),
    )
"""

from dataclasses import dataclass, field
from typing import Any


@dataclass
class PackageRegistryConfig:
    """Pip-compatible private package registry (and optional Maven mirror).

    Field names map directly to AWS CodeArtifact but are generic enough to
    work with any PyPI-compatible private index.

    Args:
        domain_owner: Account ID (or tenant identifier) that owns the registry.
        domain: Registry domain name.
        repository: Repository name within the domain.
        region: Region hosting the registry. Defaults to ``"us-east-1"``.
        maven_repository: Optional Maven repository within the same domain.
            When set, JAR URLs for Sedona/GeoTools are built against it.
            Leave empty to disable registry-backed Maven downloads.
        maven_repository_path: URL path segment for the Maven repository
            (e.g. ``"maven/my-maven"``). Combined with the registry host to
            form the base Maven URL. Defaults to ``"maven/" + maven_repository``
            when empty.
    """

    domain_owner: str
    domain: str
    repository: str
    region: str = "us-east-1"
    maven_repository: str = ""
    maven_repository_path: str = ""


@dataclass
class ArtifactStoreConfig:
    """S3 location used to cache Spark job assets (wheels, JARs, scripts).

    Args:
        s3_bucket: S3 bucket name.
        s3_root: Key prefix within the bucket. All provider-managed objects
            are written under ``s3://<s3_bucket>/<s3_root>/``. Defaults to
            ``"spark-agnostic-operator"``.
        job_runner_wheel_prefix: Filename prefix of the job-runner wheel. The
            provider locates a wheel whose filename starts with this prefix
            and extracts platform-specific runner scripts from it. Pass
            ``None`` to skip extraction.
        force_pip_packages: Package-name substrings that must be installed via
            pip on the cluster instead of being uploaded as wheels (e.g.
            native packages needing platform-specific resolution at runtime).
    """

    s3_bucket: str
    s3_root: str = "spark-agnostic-operator"
    job_runner_wheel_prefix: str | None = None
    """Deprecated. Runner scripts are now bundled in the provider package.
    This field is kept for backward compatibility but has no effect when
    ``runner_script_overrides`` is not set."""
    force_pip_packages: list[str] = field(default_factory=list)
    runner_script_overrides: dict[str, str] = field(default_factory=dict)
    """Optional per-platform S3 URIs that replace the provider-bundled runners.

    Keys are platform names: ``"glue"``, ``"glue_scala"``, ``"databricks"``,
    ``"wherobots"``.  When a platform is present here its URI is used as-is
    and the bundled script is not uploaded.  Useful for pinning a custom runner
    version or pointing at a pre-deployed workspace notebook.

    Example::

        ArtifactStoreConfig(
            s3_bucket="my-bucket",
            runner_script_overrides={
                "glue": "s3://my-bucket/runners/custom_glue_runner.py",
            },
        )
    """


@dataclass
class IcebergConfig:
    """Iceberg Spark configuration for all platforms.

    The two fields hold structurally different configs because each platform
    family talks to the Iceberg catalog differently:

    - ``spark_config`` — for Glue and Databricks. Typically a ``RESTCatalog``
      configuration using native AWS credentials.
    - ``wherobots_spark_config`` — for Wherobots. Typically a ``GlueCatalog``
      configuration accessed via cross-account credential delegation.

    The task group selects the right variant at runtime based on the resolved
    platform family. Pass only the variants you need; omit ``iceberg_config``
    entirely for jobs that do not use Iceberg.

    Args:
        spark_config: Iceberg Spark config JSON string for Glue/Databricks.
            Defaults to ``"{}"`` (no Iceberg).
        wherobots_spark_config: Iceberg Spark config JSON string for Wherobots.
            Defaults to ``"{}"`` (no Iceberg).
    """

    spark_config: str = "{}"
    wherobots_spark_config: str = "{}"


@dataclass
class GlueConfig:
    """AWS Glue-specific job settings.

    Args:
        iam_role_name: IAM role name attached to the Glue job.
            Defaults to ``"AWSGlueServiceRole"``.
        execution_class: Glue execution class. ``"STANDARD"`` (default) uses
            standard workers; ``"FLEX"`` uses spot-backed workers at lower cost.
    """

    iam_role_name: str = "AWSGlueServiceRole"
    execution_class: str = "STANDARD"


@dataclass
class DatabricksConfig:
    """Databricks-specific cluster and submission settings.

    Args:
        cluster_conf: Raw Databricks cluster configuration dict passed to
            ``DatabricksSubmitRunOperator``. Must contain ``databricks_conn_id``
            at minimum.
        extra_libraries: Extra ``{"pypi": {"package": "..."}}`` library entries
            appended to the auto-generated cluster libraries list. Use for
            pinning environment-specific transitive deps.
        dbfs_root_template: Template for the DBFS root used to stage job assets.
            ``{s3_assets_root}`` is substituted at runtime.
        workspace_scripts_path_template: Template for the workspace path
            holding init scripts. ``{s3_assets_root}`` is substituted at runtime.
        cluster_init_script_name: Filename of the cluster init script located
            under ``workspace_scripts_path_template``.
        custom_tags: Cluster ``custom_tags`` dict applied to every cluster the
            provider launches.
        spark_conf: Databricks-only Spark configuration merged into the cluster's
            ``spark_conf`` between the provider's base defaults and the caller's
            platform-agnostic ``extra_spark_conf`` (so ``extra_spark_conf`` still
            wins). Applied only on Databricks runs, so platform-specific
            credentials/extensions never leak onto Glue or Wherobots.
        spark_env_vars: Databricks-only environment variables merged into the
            cluster's ``spark_env_vars`` between the provider's base defaults and
            the caller's platform-agnostic ``extra_spark_env_vars`` (so
            ``extra_spark_env_vars`` still wins). Applied only on Databricks runs.
        worker_instance_types: Optional caller-supplied catalog mapping
            Databricks node type IDs to their core count (e.g.
            ``{"Standard_NC8as_T4_v3": 8}``). When non-empty it replaces the
            provider's built-in CPU node catalog for worker sizing, so the same
            core-based sizing logic picks from these node types instead. Use
            this to pin specific SKUs or to size GPU clusters offline (without a
            workspace lookup). Worker count is still derived from
            ``spark_cluster_desired_worker_cores`` / ``spark_cluster_desired_workers``;
            for GPU-count-sensitive jobs, pin the worker count via
            ``spark_cluster_desired_workers``.
        driver_node_type: Optional driver node type ID. Overrides the default
            driver SKU. Leave empty to keep the provider default.
        spark_version: Optional Databricks runtime version ID. Overrides the
            runtime derived from the Spark implementation. Required for GPU runs,
            which need a GPU-enabled runtime (e.g. ``"15.4.x-gpu-ml-scala2.12"``).
            Leave empty to keep the implementation's native version.
        gpu: When ``True``, the provider discovers GPU-capable node types and a
            GPU-enabled ML runtime from the connected workspace (via the
            ``databricks-sdk``) and sizes the cluster from them — no need to
            hand-maintain cloud-specific SKUs. Discovery only fills the gaps:
            any of ``worker_instance_types`` / ``driver_node_type`` /
            ``spark_version`` you set explicitly takes precedence, and when all
            three are set discovery is skipped entirely (no API call). Requires
            the ``[databricks]`` extra and a reachable workspace connection at
            setup time. Raises if the workspace exposes no GPU node types. The
            driver defaults to the cheapest discovered CPU node (the driver
            doesn't need a GPU); override with ``driver_node_type``.
            For GPU runs prefer sizing by ``spark_cluster_desired_workers``
            (explicit node/GPU count) over ``spark_cluster_desired_worker_cores``;
            core-based sizing is an indirect proxy for GPUs and assumes a fixed
            cores-per-GPU node shape.
    """

    cluster_conf: dict[str, Any] = field(default_factory=dict)
    extra_libraries: list[dict[str, Any]] = field(default_factory=list)
    dbfs_root_template: str = "dbfs:/FileStore/deploy/{s3_assets_root}"
    workspace_scripts_path_template: str = "/Workspace/Shared/{s3_assets_root}"
    cluster_init_script_name: str = "agnostic_operator_cluster_init_databricks.sh"
    custom_tags: dict[str, str] = field(default_factory=dict)
    spark_conf: dict[str, Any] = field(default_factory=dict)
    spark_env_vars: dict[str, Any] = field(default_factory=dict)
    worker_instance_types: dict[str, int] = field(default_factory=dict)
    driver_node_type: str = ""
    spark_version: str = ""
    gpu: bool = False


@dataclass
class WherobotsConfig:
    """Wherobots-specific execution settings.

    Args:
        role_arn: IAM role ARN granting Wherobots cross-account access to S3
            and Glue (e.g. ``"arn:aws:iam::123456789012:role/wherobots-access"``).
            Required when using Iceberg with Wherobots; ignored otherwise.
        external_id: External ID for the cross-account assume-role call.
        aws_region: AWS region used for Iceberg credential config and for
            resolving the Wherobots run region.
    """

    role_arn: str = ""
    external_id: str = ""
    aws_region: str = "us-east-1"
