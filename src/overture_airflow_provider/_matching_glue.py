"""Scala matcher submission through the Spark-agnostic Glue runner."""

from urllib.parse import urlsplit

from overture_airflow_provider._airflow_compat import task
from overture_airflow_provider.matching_config import GlueMatchingConfig
from overture_airflow_provider.spark_agnostic_taskgroup import spark_agnostic_task_group


@task
def _glue_parameters(parameters: dict, output_result_file: str, extra_files: str) -> dict:
    if not output_result_file or "/" in output_result_file:
        raise ValueError("output_result_file must be a non-empty filename")
    conf = {"--" + key: value for key, value in parameters.items()}
    conf["--outputPath"] = conf["--outputPath"].replace("s3:", "s3a:", 1)
    conf["--overrideConfigPath"] = conf["--overrideConfigPath"] or "none"
    conf["--outputResultFile"] = output_result_file
    if extra_files:
        conf["--extra-files"] = extra_files
    return conf


@task
def _read_manifest(parameters: dict, output_result_file: str) -> dict:
    import json

    import boto3

    uri = urlsplit(parameters["outputPath"])
    if uri.scheme not in ("s3", "s3a") or not uri.netloc:
        raise ValueError("Glue matching output_path must be an S3 URI")
    key = f"{uri.path.strip('/')}/{output_result_file}".lstrip("/")
    response = boto3.client("s3").get_object(Bucket=uri.netloc, Key=key)
    with response["Body"] as body:
        return json.loads(body.read())


def build_matching(config: GlueMatchingConfig, parameters, output_result_file, worker_cores):
    conf = _glue_parameters(parameters, output_result_file, config.extra_files)
    job = spark_agnostic_task_group(
        group_id="spark",
        class_name="org.overturemaps.matching.Main",
        parameters=conf,
        spark_impl_name=config.spark_impl_name,
        sedona_version=config.sedona_version,
        spark_jar_paths=config.spark_jar_paths,
        spark_cluster_desired_worker_cores=worker_cores,
        extra_spark_conf=config.extra_spark_conf,
        extra_spark_env_vars=config.extra_spark_env_vars,
        package_registry=config.package_registry,
        artifact_store=config.artifact_store,
        glue_config=config.glue_config,
        iceberg_config=config.iceberg_config,
        pool=config.pool,
        python_download_pool=config.python_download_pool,
        scala_download_pool=config.scala_download_pool,
        retries=config.retries,
        max_timeout_hours=config.max_timeout_hours,
        report_issue_config=config.report_issue_config,
    )
    output = _read_manifest(parameters, output_result_file)
    conf >> job >> output
    return output
