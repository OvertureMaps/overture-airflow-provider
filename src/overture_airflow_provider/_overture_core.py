"""Lazy imports for the optional ``overture-core`` dependency (``[overture]`` extra)."""

import importlib
from types import ModuleType


def import_overture_core(module: str) -> ModuleType:
    """Import an ``overture_core`` module, pointing at the extra when it's missing."""
    try:
        return importlib.import_module(module)
    except ModuleNotFoundError as exc:
        if exc.name != "overture_core":
            raise
        raise ModuleNotFoundError(
            "overture-core is required to download, cache and upload Spark job "
            'packages. Install it with `pip install "airflow-provider-overture[overture]"`.',
            name=exc.name,
        ) from exc
