"""Wherobots job runner entry point.

Parses ``--key value`` pairs from ``sys.argv`` and dispatches to the user job
class. This script is **Overture-free**. User job classes may import whatever
they need from the cluster's installed packages.

Args (positional ``--key value`` pairs via ``sys.argv``):
    --module_name: Dotted Python module path containing the job class.
    --class_name: Class name to instantiate and call ``.run()``.
    --params: JSON-encoded parameters forwarded verbatim to the job class.
"""

import inspect
import sys
from importlib import import_module


def _parse_argv() -> dict:
    """Parse ``--key value`` pairs from ``sys.argv[1:]``."""
    it = iter(sys.argv[1:])
    d: dict = {}
    for tok in it:
        if tok.startswith("--"):
            try:
                d[tok[2:]] = next(it)
            except StopIteration:
                break
    return d


# Print installed packages to aid cluster debugging.
try:
    import importlib.metadata as _meta

    _pkgs = sorted(f"{d.metadata['Name']}=={d.version}" for d in _meta.distributions())
    print("Installed packages:")
    for _p in _pkgs:
        print(f"  {_p}")
except Exception:  # noqa: BLE001
    pass

args = _parse_argv()
module_name = args["module_name"]
class_name = args["class_name"]
params = args.get("params", "{}")

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
