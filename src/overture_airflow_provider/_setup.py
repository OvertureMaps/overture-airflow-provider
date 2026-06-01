"""Setup phase for Spark jobs.

``setup_spark_job`` resolves platform/version metadata, builds the run
identifier, and returns the flat ``setup_info`` dict that downstream platform
tasks consume via XCom.
"""

import datetime
import json

from overture_airflow_provider.config import (
    ArtifactStoreConfig,
    DatabricksConfig,
    GlueConfig,
    PackageRegistryConfig,
    WherobotsConfig,
)
from overture_airflow_provider.python_package_utils import CodeArtifactPyPiClient
from overture_airflow_provider.spark import SparkImpl, SparkSedona


def setup_spark_job(
    spark_impl_name: str,
    sedona_version: str,
    module_name: str,
    class_name: str,
    job_name: str,
    parameters,
    spark_jar_paths: str,
    package_registry: PackageRegistryConfig | None = None,
    artifact_store: ArtifactStoreConfig | None = None,
    glue_config: GlueConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    wherobots_config: WherobotsConfig | None = None,
) -> dict:
    """Initialize Spark job environment and return a flat ``setup_info`` dict.

    The returned dict contains both serializable values (everything in
    ``setup_info.SERIALIZABLE_KEYS``) and non-serializable in-process values
    (``spark_impl``, ``spark_family``, ``py_pi_client``) which are stripped
    before XCom push and rehydrated downstream.
    """
    package_registry = package_registry or PackageRegistryConfig(
        domain_owner="", domain="", repository=""
    )
    artifact_store = artifact_store or ArtifactStoreConfig(s3_bucket="")
    glue_config = glue_config or GlueConfig()
    databricks_config = databricks_config or DatabricksConfig()
    wherobots_config = wherobots_config or WherobotsConfig()

    # Always join all non-empty parts so Scala jobs (module_name="") still keep
    # class_name in the run identifier; fall back to bare job_name only when
    # nothing else is supplied.
    full_job_name = (
        ".".join(part for part in (module_name, class_name, job_name) if part) or job_name
    )
    print(f"job name: [{full_job_name}]")

    spark_impl = SparkImpl.from_str(spark_impl_name)
    spark_family = spark_impl.get_family()

    spark_version = spark_impl.get_spark_version()
    scala_version = spark_impl.get_scala_version()
    python_version = spark_impl.get_python_version()
    spark_version_for_sedona = SparkSedona.getSparkVersionForSedona(spark_version, sedona_version)
    geotools_wrapper_version = SparkSedona.getGeotoolsWrapperVersion(sedona_version)

    timestamp = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    run_identifier = f"{full_job_name}_{timestamp}"
    print(f"run_identifier: {run_identifier}")

    py_pi_client = CodeArtifactPyPiClient(
        domain_owner=package_registry.domain_owner,
        domain=package_registry.domain,
        repository=package_registry.repository,
        region_name=package_registry.region,
    )
    print("Initialized private PyPI client")

    if isinstance(parameters, str):
        resolved_parameters = parameters
    elif isinstance(parameters, list):
        resolved_parameters = {
            split_param[0]: split_param[1].strip()
            for split_param in [param.split("=", 1) for param in parameters]
            if split_param[1].strip()
        }
    else:
        resolved_parameters = json.dumps(parameters)
    print(f"Using parameters: {resolved_parameters}")

    spark_jar_paths_list = spark_jar_paths.split(",") if spark_jar_paths else []
    print(f"Using spark jar paths: {spark_jar_paths_list}")

    return {
        "job_name": full_job_name,
        "spark_impl": spark_impl,
        "spark_impl_name": spark_impl_name,
        "spark_family": spark_family,
        "spark_version": spark_version,
        "scala_version": scala_version,
        "python_version": python_version,
        "sedona_version": sedona_version,
        "spark_version_for_sedona": spark_version_for_sedona,
        "geotools_wrapper_version": geotools_wrapper_version,
        "run_identifier": run_identifier,
        "py_pi_client": py_pi_client,
        "parameters": resolved_parameters,
        "spark_jar_paths": spark_jar_paths_list,
        # Flattened from config objects for XCom-safe downstream access
        "s3_assets_bucket": artifact_store.s3_bucket,
        "s3_assets_root": artifact_store.s3_root,
        "job_runner_wheel_prefix": artifact_store.job_runner_wheel_prefix,
        "force_pip_packages": list(artifact_store.force_pip_packages),
        "runner_script_overrides": dict(artifact_store.runner_script_overrides),
        "wherobots_external_id": wherobots_config.external_id,
        "wherobots_role_arn": wherobots_config.role_arn,
        "aws_region": wherobots_config.aws_region,
        "databricks_conf": databricks_config.cluster_conf,
        "databricks_extra_libraries": list(databricks_config.extra_libraries),
        "databricks_dbfs_root_template": databricks_config.dbfs_root_template,
        "databricks_workspace_scripts_path_template": (
            databricks_config.workspace_scripts_path_template
        ),
        "databricks_cluster_init_script_name": (databricks_config.cluster_init_script_name),
        "databricks_custom_tags": dict(databricks_config.custom_tags),
        "databricks_spark_conf": dict(databricks_config.spark_conf),
        "databricks_spark_env_vars": dict(databricks_config.spark_env_vars),
        "databricks_worker_instance_types": dict(databricks_config.worker_instance_types),
        "databricks_driver_node_type": databricks_config.driver_node_type,
        "databricks_spark_version": databricks_config.spark_version,
        "databricks_gpu": databricks_config.gpu,
        "glue_execution_class": glue_config.execution_class,
        "iam_role_name": glue_config.iam_role_name,
        # Registry params stored for client reconstruction after XCom round-trip
        "codeartifact_domain_owner": package_registry.domain_owner,
        "codeartifact_domain": package_registry.domain,
        "codeartifact_repository": package_registry.repository,
        "codeartifact_region": package_registry.region,
        "codeartifact_maven_repository": package_registry.maven_repository,
        "codeartifact_maven_repository_path": (
            package_registry.maven_repository_path
            or (
                f"maven/{package_registry.maven_repository}"
                if package_registry.maven_repository
                else ""
            )
        ),
    }
