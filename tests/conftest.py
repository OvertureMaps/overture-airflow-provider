"""Conftest for overture_airflow_provider tests.

Installs sys.modules stubs for optional dependencies (databricks SDK,
wherobots SDK, sh) so the suite runs without them installed. Stubs are
only inserted when the real package cannot be imported, so test runs
that have the extras installed keep the real implementations.
"""

import importlib
import sys
from unittest.mock import MagicMock

_OPTIONAL_MODULES = (
    "sh",
    "databricks",
    "databricks.sdk",
    "airflow.providers.databricks",
    "airflow.providers.databricks.hooks",
    "airflow.providers.databricks.hooks.databricks",
    "airflow.providers.databricks.operators",
    "airflow.providers.databricks.operators.databricks",
    "airflow_providers_wherobots",
    "airflow_providers_wherobots.operators",
    "airflow_providers_wherobots.operators.run",
    "wherobots",
    "wherobots.db",
)

for _mod in _OPTIONAL_MODULES:
    if _mod in sys.modules:
        continue
    try:
        importlib.import_module(_mod)
    except Exception:
        sys.modules[_mod] = MagicMock()
