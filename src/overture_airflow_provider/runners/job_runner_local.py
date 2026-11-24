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

    --executor_env_names: JSON list of environment variable names to forward to
        executors as ``spark.executorEnv.*``. Values are read from this
        process's environment so they never appear on a command line.

Under ``spark-submit`` the submitted ``--master`` wins over a master the job
hardcodes with ``SparkSession.builder.master(...)``; the job still configures
the rest of its own session. Jobs that need ``init_spark_for_platform()``
cannot run over Spark Connect, which has no ``SparkContext``.
"""

import inspect
import json
import os
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
spark_connect = bool(os.environ.get("SPARK_REMOTE"))
wants_spark = "spark" in sig.parameters
if spark_connect and not wants_spark and hasattr(instance, "init_spark_for_platform"):
    raise RuntimeError(
        f"{class_name} needs init_spark_for_platform(), which requires a SparkContext; "
        "Spark Connect (sc://) has none. Use a local[*] or spark:// master, or accept a "
        "`spark` argument in run()."
    )
if not spark_connect:
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    def _keep_submitted_master(builder, master):
        return builder

    SparkSession.Builder.master = _keep_submitted_master

    executor_env_names = json.loads(args.get("executor_env_names") or "[]")
    if executor_env_names:
        # SparkConf reads JVM system properties when the session is created.
        SparkContext._ensure_initialized()
        system = SparkContext._jvm.java.lang.System
        for name in executor_env_names:
            key = f"spark.executorEnv.{name}"
            if name in os.environ and system.getProperty(key) is None:
                system.setProperty(key, os.environ[name])

if wants_spark:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()
    for k, v in json.loads(extra_spark_conf_raw).items():
        if not spark_connect and k.startswith("spark."):
            continue
        try:
            spark.conf.set(k, v)
        except Exception as exc:
            if (
                not spark_connect
                or getattr(exc, "getErrorClass", lambda: None)() != "CANNOT_MODIFY_CONFIG"
            ):
                raise
            print(f"Cannot apply static Spark Connect config {k}: {exc}")
    run_kwargs["spark"] = spark
elif hasattr(instance, "init_spark_for_platform"):
    instance.init_spark_for_platform(extra_spark_conf=extra_spark_conf_raw)

result = instance.run(**run_kwargs)

if result is not None and hasattr(result, "isSuccess") and not result.isSuccess:
    raise result.exception or RuntimeError(
        f"Job {class_name!r} returned a failure result without an exception"
    )
