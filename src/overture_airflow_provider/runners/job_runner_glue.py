"""AWS Glue job runner entry point.

Bootstraps job dispatch on the Glue cluster. This script is **Overture-free**:
no dependency on ``overture_spark`` or any Overture-specific package. User job
classes may import whatever they need from the cluster's installed packages.

Args (resolved via ``awsglue.utils.getResolvedOptions``):
    module_name: Dotted Python module path containing the job class.
    class_name: Class name to instantiate and call ``.run()``.
    params: JSON-encoded parameters forwarded verbatim to the job class.
    extra_spark_conf: JSON-encoded dict of additional SparkConf key/value pairs.
        Applied before ``run()`` in one of two ways: injected directly into the
        active ``SparkSession`` when ``run()`` accepts a ``spark`` keyword
        argument, or forwarded to ``init_spark_for_platform()`` for
        ``SparkSedonaJob``-style classes that initialise Spark internally.
"""

import inspect
import json
import sys
from importlib import import_module

from awsglue.utils import getResolvedOptions

args = getResolvedOptions(
    sys.argv,
    ["module_name", "class_name", "params", "extra_spark_conf"],
)
module_name = args["module_name"]
class_name = args["class_name"]
params = args["params"]
extra_spark_conf_raw = args.get("extra_spark_conf") or "{}"

# Print installed packages to aid cluster debugging.
try:
    import importlib.metadata as _meta

    _pkgs = sorted(f"{d.metadata['Name']}=={d.version}" for d in _meta.distributions())
    print("Installed packages:")
    for _p in _pkgs:
        print(f"  {_p}")
except Exception:  # noqa: BLE001
    pass

module = import_module(module_name)
job_cls = getattr(module, class_name)
instance = job_cls()

# Inject SparkSession only if the job's run() explicitly accepts it.
# For SparkSedonaJob-style classes (no 'spark' param), pre-call
# init_spark_for_platform() so extra_spark_conf reaches the Spark builder
# before getOrCreate(). Without this, run() calls it internally with "{}"
# and silently discards all DAG-level extra_spark_conf.
run_kwargs: dict = {"params": params}
sig = inspect.signature(instance.run)
if "spark" in sig.parameters:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for k, v in json.loads(extra_spark_conf_raw).items():
        spark.conf.set(k, v)
    run_kwargs["spark"] = spark
elif hasattr(instance, "init_spark_for_platform"):
    instance.init_spark_for_platform(extra_spark_conf=extra_spark_conf_raw)

result = instance.run(**run_kwargs)

# Duck-type the result: honour JobResult.isSuccess when present.
if result is not None and hasattr(result, "isSuccess") and not result.isSuccess:
    raise result.exception or RuntimeError(
        f"Job {class_name!r} returned a failure result without an exception"
    )
