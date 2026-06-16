"""Overture Maps Airflow provider for Spark-agnostic job orchestration.

Public surface (lazily loaded — see ``__getattr__``):
    spark_agnostic_task_group         – main TaskGroup factory
    spark_agnostic_mapped_task_group  – dynamically-mapped variant
    render_spark_job, RenderResult    – Airflow-free payload renderer
    SparkImpl, SparkFamily, SparkSedona  – platform/version enums
    AwsGlueClusterSize, DatabricksClusterSize, WherobotsClusterSize
    PackageRegistryConfig, ArtifactStoreConfig, IcebergConfig,
    GlueConfig, DatabricksConfig, WherobotsConfig  – caller-facing config

Symbols are imported lazily so that ``import overture_airflow_provider`` does
not pull in ``airflow.models`` (and its platform-specific deps) unless the
caller actually touches a symbol that needs it.
"""

from importlib import import_module
from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING, Any

try:
    __version__ = version("airflow-provider-overture")
except PackageNotFoundError:  # package not installed (e.g. source checkout)
    __version__ = "0.0.0"

# name -> (submodule, attribute)
_LAZY_IMPORTS = {
    "ArtifactStoreConfig": ("config", "ArtifactStoreConfig"),
    "AwsGlueClusterSize": ("cluster_sizing", "AwsGlueClusterSize"),
    "DatabricksClusterSize": ("cluster_sizing", "DatabricksClusterSize"),
    "DatabricksConfig": ("config", "DatabricksConfig"),
    "GlueConfig": ("config", "GlueConfig"),
    "IcebergConfig": ("config", "IcebergConfig"),
    "PackageRegistryConfig": ("config", "PackageRegistryConfig"),
    "RenderResult": ("render", "RenderResult"),
    "ReportIssueConfig": ("config", "ReportIssueConfig"),
    "SparkJobLink": ("links", "SparkJobLink"),
    "SparkImpl": ("spark", "SparkImpl"),
    "SparkSedona": ("spark", "SparkSedona"),
    "WherobotsClusterSize": ("cluster_sizing", "WherobotsClusterSize"),
    "WherobotsConfig": ("config", "WherobotsConfig"),
    "render_spark_job": ("render", "render_spark_job"),
    "spark_agnostic_mapped_task_group": (
        "spark_agnostic_taskgroup",
        "spark_agnostic_mapped_task_group",
    ),
    "spark_agnostic_task_group": (
        "spark_agnostic_taskgroup",
        "spark_agnostic_task_group",
    ),
}

__all__ = [*sorted(_LAZY_IMPORTS), "__version__"]


def __getattr__(name: str) -> Any:
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY_IMPORTS[name]
    module = import_module(f"{__name__}.{submodule}")
    value = getattr(module, attr)
    globals()[name] = value
    return value


if TYPE_CHECKING:
    from overture_airflow_provider.cluster_sizing import (  # noqa: F401
        AwsGlueClusterSize,
        DatabricksClusterSize,
        WherobotsClusterSize,
    )
    from overture_airflow_provider.config import (  # noqa: F401
        ArtifactStoreConfig,
        DatabricksConfig,
        GlueConfig,
        IcebergConfig,
        PackageRegistryConfig,
        ReportIssueConfig,
        WherobotsConfig,
    )
    from overture_airflow_provider.links import SparkJobLink  # noqa: F401
    from overture_airflow_provider.render import (  # noqa: F401
        RenderResult,
        render_spark_job,
    )
    from overture_airflow_provider.spark import (  # noqa: F401
        SparkFamily,
        SparkImpl,
        SparkSedona,
    )
    from overture_airflow_provider.spark_agnostic_taskgroup import (  # noqa: F401
        spark_agnostic_mapped_task_group,
        spark_agnostic_task_group,
    )
