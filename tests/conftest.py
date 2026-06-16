"""Conftest for overture_airflow_provider tests.

Installs sys.modules stubs for optional dependencies (databricks SDK,
wherobots SDK, sh) so the suite runs without them installed. Stubs are
only inserted when the real package cannot be imported, so test runs
that have the extras installed keep the real implementations.

The ``tests/e2e`` suite needs a real Airflow CLI and only runs inside the e2e
container, which sets ``RUN_E2E=1``. Everywhere else (host dev, the unit CI
legs) it is excluded from collection so it never fails for lack of Airflow.
"""

import importlib
import os
import sys
import types
from unittest.mock import MagicMock

# Skip the container-only e2e suite unless explicitly opted in (see tests/e2e).
if not os.environ.get("RUN_E2E"):
    collect_ignore = ["e2e"]

_OPTIONAL_MODULES = (
    "sh",
    "databricks",
    "databricks.sdk",
    "airflow.providers.databricks",
    "airflow.providers.databricks.hooks",
    "airflow.providers.databricks.hooks.databricks",
    "airflow.providers.databricks.operators",
    "airflow.providers.databricks.operators.databricks",
    "airflow.providers.databricks.triggers",
    "airflow.providers.databricks.triggers.databricks",
    "airflow_providers_wherobots",
    "airflow_providers_wherobots.operators",
    "airflow_providers_wherobots.operators.run",
    "wherobots",
    "wherobots.db",
)


def _stub_module(name: str):
    module = types.ModuleType(name)
    module.__getattr__ = lambda attr: MagicMock(name=f"{name}.{attr}")
    sys.modules[name] = module

    if "." not in name:
        return module

    parent_name, child_name = name.rsplit(".", 1)
    parent = sys.modules.get(parent_name) or _stub_module(parent_name)
    setattr(parent, child_name, module)
    return module


for _mod in _OPTIONAL_MODULES:
    if _mod in sys.modules:
        continue
    try:
        importlib.import_module(_mod)
    except Exception:
        _stub_module(_mod)
