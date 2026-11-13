"""Scala matcher submission through a Spark task group."""

import json
from collections.abc import Callable
from typing import Any

from overture_airflow_provider._airflow_compat import XComArg, task
from overture_airflow_provider.matching_taskgroup import MatcherTaskFactory
from overture_airflow_provider.spark_agnostic_taskgroup import spark_agnostic_task_group

MATCHER_CLASS = "org.overturemaps.matching.Main"


@task
def _matcher_arguments(parameters: dict, output_result_file: str, extra_files: str) -> dict:
    arguments = {"--" + key: value for key, value in parameters.items()}
    arguments["--outputPath"] = arguments["--outputPath"].replace("s3:", "s3a:", 1)
    arguments["--overrideConfigPath"] = arguments["--overrideConfigPath"] or "none"
    arguments["--outputResultFile"] = output_result_file
    if extra_files:
        arguments["--extra-files"] = extra_files
    return arguments


@task
def _read_manifest(parameters: dict, output_result_file: str) -> dict:
    from overture_core.cloud.aws.object import get_object_bytes, parse_s3_uri

    output_path = parameters["outputPath"].replace("s3a:", "s3:", 1).rstrip("/")
    manifest = f"{output_path}/{output_result_file}"
    body = get_object_bytes(*parse_s3_uri(manifest))
    if body is None:
        raise FileNotFoundError(f"Matcher result manifest not found: {manifest}")
    return json.loads(body)


def spark_matcher_task_factory(
    spark_task_group: Callable[..., Any] = spark_agnostic_task_group,
    *,
    output_result_file: str = "data_paths.json",
    extra_files: Any = "",
    **spark_kwargs: Any,
) -> MatcherTaskFactory:
    """Return a ``matcher_task_factory`` that runs the published Scala matcher.

    The matcher is submitted as ``org.overturemaps.matching.Main`` through
    ``spark_task_group``, called with ``group_id="spark"``, ``class_name``,
    ``parameters`` and every ``spark_kwargs`` entry unchanged. Pass a
    deployment's own wrapper around ``spark_agnostic_task_group`` to keep its
    defaults and extra links. JARs, Sedona version, sizing, retries, Spark
    settings and platform config all come from ``spark_kwargs``.

    The matcher writes its result paths to ``<output_path>/<output_result_file>``
    on S3, and they are read back once the job succeeds. ``extra_files`` becomes
    the job's ``--extra-files`` (for example the libpostal data for places).
    """
    if not output_result_file or "/" in output_result_file:
        raise ValueError("output_result_file must be a non-empty filename")

    def build(*, parameters: XComArg) -> XComArg:
        arguments = _matcher_arguments(parameters, output_result_file, extra_files)
        job = spark_task_group(
            group_id="spark", class_name=MATCHER_CLASS, parameters=arguments, **spark_kwargs
        )
        manifest = _read_manifest(parameters, output_result_file)
        arguments >> job >> manifest
        return manifest

    return build
