"""Single source of truth for picking and merging Iceberg Spark configs.

``IcebergConfig`` (see ``config.py``) carries four JSON-string fields because
each platform family talks to Iceberg differently, and an S3 Tables catalog
can optionally coexist alongside the primary one:

- ``spark_config`` / ``wherobots_spark_config`` — the primary catalog, Glue
  and Databricks vs. Wherobots respectively.
- ``s3tables_spark_config`` / ``wherobots_s3tables_spark_config`` — the
  optional secondary S3 Tables catalog for the same two platform groups.

``resolve_iceberg_spark_config`` is the *only* place that picks the right pair
for a resolved platform family and merges them. It used to be duplicated
(nearly verbatim) between ``spark_agnostic_taskgroup._select_iceberg_conf``
(the live DAG path) and ``render._select_iceberg_spark_config`` (the
Airflow-free render/test path). Two copies meant a fix to one could silently
miss the other; both now delegate here.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from overture_airflow_provider.config import IcebergConfig, coerce_config_dict

if TYPE_CHECKING:
    from overture_airflow_provider.spark import SparkFamily

WHEROBOTS_FAMILY_NAME = "WHEROBOTS"


def _family_name(spark_family: SparkFamily | str) -> str:
    """Normalize a ``SparkFamily`` enum member or a bare family name string."""
    return spark_family.name if hasattr(spark_family, "name") else str(spark_family)


def resolve_iceberg_spark_config(
    iceberg_config: IcebergConfig | None,
    spark_family: SparkFamily | str,
) -> dict[str, Any]:
    """Pick the right Iceberg config variants for ``spark_family`` and merge them.

    Merges the primary catalog config with the S3 Tables catalog config (when
    present) into a single dict. S3 Tables keys are namespaced under a separate
    catalog alias (e.g. ``spark.sql.catalog.s3tables_catalog.*``) so they
    coexist with the primary catalog without conflicting; S3 Tables values win
    over primary-catalog values on key collision.

    Args:
        iceberg_config: The caller's ``IcebergConfig``, or ``None`` if the job
            doesn't use Iceberg.
        spark_family: The resolved platform family, as a ``SparkFamily`` enum
            member or its bare name (e.g. ``"WHEROBOTS"``) — both call sites
            (the live DAG path and the Airflow-free render path) resolve the
            family at a different point in their pipeline, so both shapes are
            accepted rather than forcing one to convert.

    Returns:
        The merged Spark config dict. Empty when ``iceberg_config`` is
        ``None`` or all its relevant fields are unset.
    """
    if iceberg_config is None:
        return {}

    if _family_name(spark_family) == WHEROBOTS_FAMILY_NAME:
        primary = coerce_config_dict(
            iceberg_config.wherobots_spark_config,
            field_name="IcebergConfig.wherobots_spark_config",
        )
        s3tables = coerce_config_dict(
            iceberg_config.wherobots_s3tables_spark_config,
            field_name="IcebergConfig.wherobots_s3tables_spark_config",
        )
    else:
        primary = coerce_config_dict(
            iceberg_config.spark_config,
            field_name="IcebergConfig.spark_config",
        )
        s3tables = coerce_config_dict(
            iceberg_config.s3tables_spark_config,
            field_name="IcebergConfig.s3tables_spark_config",
        )

    return {**primary, **s3tables}
