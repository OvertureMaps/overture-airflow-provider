"""Local Spark job runner entry point (development only).

Started by ``spark-submit`` (``local[*]`` / ``spark://`` masters) or as a plain
Python process with ``SPARK_REMOTE`` set (``sc://`` Spark Connect). This script
is **Overture-free**: it depends only on pyspark and the standard library.

Args (``--key value`` pairs via ``sys.argv``):
    --module_name: Dotted Python module path containing the job class.
    --class_name: Class name to instantiate and call ``.run()``.
    --params: JSON-encoded parameters forwarded verbatim to the job class.
    --extra_spark_conf: JSON-encoded dict of SparkConf key/value pairs. Set on
        the active ``SparkSession`` when ``run()`` accepts ``spark``, otherwise
        forwarded to ``init_spark_for_platform()``.
"""

import inspect
import json
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


args = _parse_argv()
module_name = args["module_name"]
class_name = args["class_name"]
params = args.get("params", "{}")
extra_spark_conf_raw = args.get("extra_spark_conf") or "{}"

module = import_module(module_name)
job_cls = getattr(module, class_name)
instance = job_cls()

run_kwargs: dict = {"params": params}
sig = inspect.signature(instance.run)
if "spark" in sig.parameters:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for k, v in json.loads(extra_spark_conf_raw).items():
        try:
            spark.conf.set(k, v)
        except Exception as exc:  # noqa: BLE001
            # Static confs were already applied at launch by spark-submit --conf.
            print(f"Skipping spark conf {k}: {exc}")
    run_kwargs["spark"] = spark
elif hasattr(instance, "init_spark_for_platform"):
    instance.init_spark_for_platform(extra_spark_conf=extra_spark_conf_raw)

result = instance.run(**run_kwargs)

if result is not None and hasattr(result, "isSuccess") and not result.isSuccess:
    raise result.exception or RuntimeError(
        f"Job {class_name!r} returned a failure result without an exception"
    )
