"""Backwards-compatible facade for per-platform execution modules.

The implementation is split by platform into ``_setup``, ``_glue``,
``_databricks``, and ``_wherobots``. This facade re-exports the public symbols
so that ``spark_platform_handlers`` and any existing tests/callers can keep
importing from ``overture_airflow_provider.spark_execution_logic`` unchanged.

Re-exports are loaded **lazily** so importing this module does not pull in
optional platform SDKs (e.g. ``apache-airflow-providers-databricks``) for
callers who only run Glue jobs.

New code should import directly from the platform modules.
"""

from importlib import import_module
from typing import Any

# name -> (submodule, attribute)
_LAZY_IMPORTS = {
    "MAX_TIMEOUT_HOURS": ("_glue", "MAX_TIMEOUT_HOURS"),
    "WHEROBOTS_AVAILABLE": ("_wherobots", "WHEROBOTS_AVAILABLE"),
    "WHEROBOTS_PROVIDER": ("_wherobots", "WHEROBOTS_PROVIDER"),
    "_get_glue_job_url_and_status": ("_glue", "_get_glue_job_url_and_status"),
    "_resolve_wherobots_region": ("_wherobots", "_resolve_wherobots_region"),
    "download_jars_glue": ("_glue", "download_jars_glue"),
    "download_jars_wherobots": ("_wherobots", "download_jars_wherobots"),
    "download_python_packages_glue": ("_glue", "download_python_packages_glue"),
    "download_python_packages_wherobots": (
        "_wherobots",
        "download_python_packages_wherobots",
    ),
    "execute_databricks_job": ("_databricks", "execute_databricks_job"),
    "execute_glue_job": ("_glue", "execute_glue_job"),
    "execute_wherobots_job": ("_wherobots", "execute_wherobots_job"),
    "setup_databricks_cluster": ("_databricks", "setup_databricks_cluster"),
    "setup_spark_job": ("_setup", "setup_spark_job"),
}

__all__ = sorted(_LAZY_IMPORTS)


def __getattr__(name: str) -> Any:
    if name not in _LAZY_IMPORTS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    submodule, attr = _LAZY_IMPORTS[name]
    module = import_module(f"overture_airflow_provider.{submodule}")
    value = getattr(module, attr)
    globals()[name] = value
    return value
