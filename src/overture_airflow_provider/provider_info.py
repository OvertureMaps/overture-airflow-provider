"""Airflow provider metadata.

The ``apache_airflow_provider`` entry point in ``pyproject.toml`` points here.
Airflow calls ``get_provider_info()`` at startup to register the provider,
populate ``airflow providers list`` output, and discover extra-links.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version


def _package_version() -> str:
    """Resolve the installed package version, falling back when uninstalled."""
    try:
        return version("airflow-provider-overture")
    except PackageNotFoundError:  # package not installed (e.g. source checkout)
        return "0.0.0"


def get_provider_info() -> dict:
    """Return Airflow provider metadata conforming to provider_info.schema.json."""
    return {
        "package-name": "airflow-provider-overture",
        "name": "Overture Maps Spark-agnostic Provider",
        "description": (
            "Spark-agnostic Airflow TaskGroup for submitting PySpark and Scala jobs to "
            "AWS Glue, Databricks, or Wherobots Cloud from a single platform-agnostic DAG entry point."
        ),
        "versions": [_package_version()],
        "operators": [
            {
                "integration-name": "Spark (Glue / Databricks / Wherobots)",
                "python-modules": [
                    "overture_airflow_provider.spark_agnostic_taskgroup",
                ],
            }
        ],
        "extra-links": [
            "overture_airflow_provider.links.SparkJobLink",
        ],
        "integrations": [
            {
                "integration-name": "AWS Glue",
                "external-doc-url": "https://docs.aws.amazon.com/glue/",
                "tags": ["aws", "spark"],
            },
            {
                "integration-name": "Databricks",
                "external-doc-url": "https://docs.databricks.com/",
                "tags": ["databricks", "spark"],
            },
            {
                "integration-name": "Wherobots Cloud",
                "external-doc-url": "https://docs.wherobots.com/",
                "tags": ["wherobots", "spark", "sedona"],
            },
        ],
    }
