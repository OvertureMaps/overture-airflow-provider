"""Databricks job runner entry point.

Dual-mode: works as a **Databricks Workspace Notebook** (``dbutils.widgets``)
and as a **spark_python_task** (``sys.argv`` / argparse).

This script is **Overture-free**. User job classes may import whatever they
need from the cluster's installed packages.
"""

import inspect
import sys
from importlib import import_module


def _parse_params() -> tuple[str, str, str]:
    """Return ``(module_name, class_name, params_json)`` from either context.

    In a Databricks Workspace Notebook the kernel injects ``dbutils`` as a
    global. Outside that context (spark_python_task) it is simply absent, so
    we fall back to argparse over ``sys.argv``.
    """
    _dbutils = globals().get("dbutils")
    if _dbutils is not None:
        _dbutils.widgets.text("module_name", "")
        _dbutils.widgets.text("class_name", "")
        _dbutils.widgets.text("params", "{}")
        return (
            _dbutils.widgets.get("module_name"),
            _dbutils.widgets.get("class_name"),
            _dbutils.widgets.get("params"),
        )

    # spark_python_task: parameters are passed as ``--key value`` pairs.
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("--module_name", required=True)
    p.add_argument("--class_name", required=True)
    p.add_argument("--params", default="{}")
    a = p.parse_args()
    return a.module_name, a.class_name, a.params


# Print installed packages to aid cluster debugging.
try:
    import importlib.metadata as _meta

    _pkgs = sorted(f"{d.metadata['Name']}=={d.version}" for d in _meta.distributions())
    print("Installed packages:")
    for _p in _pkgs:
        print(f"  {_p}")
except Exception:  # noqa: BLE001
    pass

module_name, class_name, params = _parse_params()

module = import_module(module_name)
job_cls = getattr(module, class_name)
instance = job_cls()

# Inject SparkSession if the job's run() accepts it.
run_kwargs: dict = {"params": params}
sig = inspect.signature(instance.run)
if "spark" in sig.parameters:
    from pyspark.sql import SparkSession

    run_kwargs["spark"] = SparkSession.builder.getOrCreate()

result = instance.run(**run_kwargs)

# Duck-type the result: honour JobResult.isSuccess when present.
if result is not None and hasattr(result, "isSuccess") and not result.isSuccess:
    raise result.exception or RuntimeError(
        f"Job {class_name!r} returned a failure result without an exception"
    )
