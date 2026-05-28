"""XCom serialization contract for the ``setup_info`` dict.

``setup_info`` is the single contract between ``setup_spark_job`` (which
builds it) and every downstream task / platform handler (which reads it).

Most fields are JSON-serializable and survive XCom round-trips unchanged. A
few are not (enums, the PyPI client) and must be re-derived in the consuming
task.

This module owns:

- ``SERIALIZABLE_KEYS`` — explicit, audited list of fields that go through
  XCom. Adding a new field to ``setup_spark_job`` requires adding it here too,
  which is the deliberate friction that prevents accidental leakage of
  non-serializable objects.
- ``to_xcom(setup_info)`` — project the full dict down to its serializable subset.
- ``rehydrate(serialized)`` — re-create non-serializable objects from the
  serialized dict.
"""

from overture_airflow_provider.python_package_utils import CodeArtifactPyPiClient
from overture_airflow_provider.spark import SparkFamily, SparkImpl

# Every key returned by setup_spark_job that is safe to push to XCom.
# Keep aligned with the keys produced by setup_spark_job in _setup.py.
SERIALIZABLE_KEYS = (
    "job_name",
    "spark_impl_name",
    "spark_version",
    "scala_version",
    "python_version",
    "sedona_version",
    "spark_version_for_sedona",
    "geotools_wrapper_version",
    "run_identifier",
    "parameters",
    "spark_jar_paths",
    "s3_assets_bucket",
    "s3_assets_root",
    "job_runner_wheel_prefix",
    "force_pip_packages",
    "runner_script_overrides",
    "wherobots_external_id",
    "wherobots_role_arn",
    "aws_region",
    "databricks_conf",
    "databricks_extra_libraries",
    "databricks_dbfs_root_template",
    "databricks_workspace_scripts_path_template",
    "databricks_cluster_init_script_name",
    "databricks_custom_tags",
    "glue_execution_class",
    "iam_role_name",
    "codeartifact_domain_owner",
    "codeartifact_domain",
    "codeartifact_repository",
    "codeartifact_region",
    "codeartifact_maven_repository",
    "codeartifact_maven_repository_path",
)


def to_xcom(setup_info: dict) -> dict:
    """Project ``setup_info`` down to its XCom-safe subset.

    Also serializes ``spark_family`` as ``spark_family_name`` because the
    ``SparkFamily`` enum doesn't round-trip through XCom cleanly.
    """
    out = {key: setup_info[key] for key in SERIALIZABLE_KEYS if key in setup_info}
    out["spark_family_name"] = setup_info["spark_family"].name
    return out


def rehydrate(serialized: dict) -> dict:
    """Reconstruct a full ``setup_info`` dict from its XCom-serialized form.

    Re-derives the non-serializable fields (``spark_impl``, ``spark_family``,
    ``py_pi_client``) from the serialized values.
    """
    full = dict(serialized)
    full["spark_impl"] = SparkImpl.from_str(serialized["spark_impl_name"])
    full["spark_family"] = SparkFamily[serialized["spark_family_name"]]
    full["py_pi_client"] = CodeArtifactPyPiClient(
        domain_owner=serialized["codeartifact_domain_owner"],
        domain=serialized["codeartifact_domain"],
        repository=serialized["codeartifact_repository"],
        region_name=serialized["codeartifact_region"],
    )
    return full
