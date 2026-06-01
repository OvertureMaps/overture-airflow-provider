"""Airflow-free rendering of Spark job submissions.

`render_spark_job(...)` builds the same platform-specific payloads that the
task group would submit, but without importing or executing any Airflow
operators. The result includes:

- ``operator_kwargs``: the dict the Airflow operator would receive
- ``submit_payload``: a JSON-serialisable equivalent suitable for the
  platform CLI / REST API (e.g. ``aws glue create-job --cli-input-json``,
  ``databricks jobs submit --json``, Wherobots REST body)
- ``cli``: an example list of shell commands to invoke manually
- ``write_to(dir)``: helper to dump JSON payloads to disk so the CLI
  commands can use ``file://`` references

Designed for local testing against real cloud resources without standing up
Airflow, and for CI snapshot tests of payload shape.

Use ``python -m overture_airflow_provider.render --help`` for the CLI.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import sys
from dataclasses import dataclass, field
from typing import Any

from overture_airflow_provider._databricks import (
    build_databricks_operator_kwargs,
    setup_databricks_cluster,
)
from overture_airflow_provider._glue import build_glue_operator_kwargs
from overture_airflow_provider._wherobots import build_wherobots_operator_kwargs
from overture_airflow_provider.config import (
    ArtifactStoreConfig,
    DatabricksConfig,
    GlueConfig,
    IcebergConfig,
    PackageRegistryConfig,
    WherobotsConfig,
)
from overture_airflow_provider.runner_assets import _RUNNER_FILES, _file_sha256, get_runner_path
from overture_airflow_provider.spark import SparkFamily, SparkImpl, SparkSedona
from overture_airflow_provider.spark_platform_handlers import (
    GluePlatformHandler,
    WherobotsPlatformHandler,
    _merge_spark_conf,
)


class _StubPyPiClient:
    """Stub for ``CodeArtifactPyPiClient`` used in render mode.

    Avoids AWS calls; only implements ``get_url`` (which Databricks handler
    uses to build pypi repo URLs in the cluster libraries list).
    """

    def __init__(self, url: str = "https://pypi.example/simple/"):
        self._url = url

    def get_url(self) -> str:
        return self._url


@dataclass
class RenderResult:
    """Result of ``render_spark_job``."""

    platform: str
    spark_impl_name: str
    setup_info: dict
    merged_spark_conf: dict
    cluster_spec: dict
    operator_kwargs: dict
    submit_payload: dict | None
    cli: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "platform": self.platform,
            "spark_impl_name": self.spark_impl_name,
            "merged_spark_conf": self.merged_spark_conf,
            "cluster_spec": _jsonify(self.cluster_spec),
            "operator_kwargs": _jsonify(self.operator_kwargs),
            "submit_payload": _jsonify(self.submit_payload)
            if self.submit_payload is not None
            else None,
            "cli": self.cli,
        }

    def write_to(self, out_dir: str) -> dict[str, str]:
        """Dump JSON payloads to ``out_dir`` and return the file paths written."""
        os.makedirs(out_dir, exist_ok=True)
        written: dict[str, str] = {}
        files = {
            "operator_kwargs.json": self.operator_kwargs,
            "merged_spark_conf.json": self.merged_spark_conf,
        }
        if self.submit_payload is not None:
            files["submit_payload.json"] = self.submit_payload
        for name, payload in files.items():
            path = os.path.join(out_dir, name)
            with open(path, "w", encoding="utf-8") as fh:
                json.dump(_jsonify(payload), fh, indent=2, sort_keys=True, default=str)
            written[name] = path
        cli_path = os.path.join(out_dir, "cli.sh")
        with open(cli_path, "w", encoding="utf-8") as fh:
            fh.write("#!/usr/bin/env bash\nset -euo pipefail\n\n")
            for cmd in self.cli:
                fh.write(cmd + "\n")
        written["cli.sh"] = cli_path
        return written


def _jsonify(obj: Any) -> Any:
    """Coerce enums and other non-JSON values into JSON-friendly forms."""
    if isinstance(obj, dict):
        return {k: _jsonify(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_jsonify(v) for v in obj]
    if hasattr(obj, "name") and hasattr(obj, "value"):
        return obj.name
    return obj


def _load_json_config(raw: str | None, field_name: str) -> dict[str, Any]:
    """Parse a JSON config string and require a JSON object payload."""
    if not raw or raw == "{}":
        return {}

    try:
        loaded = json.loads(raw)
    except (TypeError, ValueError) as exc:
        detail = exc.msg if isinstance(exc, json.JSONDecodeError) else str(exc)
        raise ValueError(f"Invalid JSON in IcebergConfig.{field_name}: {detail}") from exc

    if not isinstance(loaded, dict):
        raise ValueError(
            f"IcebergConfig.{field_name} must decode to a JSON object, got {type(loaded).__name__}"
        )
    return loaded


def _select_iceberg_spark_config(
    iceberg_config: IcebergConfig | None, family: SparkFamily
) -> dict[str, Any]:
    """Resolve and merge the Iceberg config variants for the selected Spark family."""
    if iceberg_config is None:
        return {}

    if family == SparkFamily.WHEROBOTS:
        primary = _load_json_config(iceberg_config.wherobots_spark_config, "wherobots_spark_config")
        s3tables = _load_json_config(
            iceberg_config.wherobots_s3tables_spark_config,
            "wherobots_s3tables_spark_config",
        )
    else:
        primary = _load_json_config(iceberg_config.spark_config, "spark_config")
        s3tables = _load_json_config(
            iceberg_config.s3tables_spark_config,
            "s3tables_spark_config",
        )

    return {**primary, **s3tables}


def _build_render_setup_info(
    spark_impl_name: str,
    sedona_version: str,
    module_name: str,
    class_name: str,
    job_name: str,
    parameters,
    spark_jar_paths: str,
    package_registry: PackageRegistryConfig,
    artifact_store: ArtifactStoreConfig,
    glue_config: GlueConfig,
    databricks_config: DatabricksConfig,
    wherobots_config: WherobotsConfig,
) -> dict:
    """Build setup_info without importing the Airflow-touching ``_setup`` module.

    Identical shape and semantics to ``_setup.setup_spark_job``, but uses a
    stub py_pi_client so no AWS calls are made and no Airflow import chain is
    triggered.
    """
    full_job_name = (
        ".".join(part for part in (module_name, class_name, job_name) if part) or job_name
    )

    spark_impl = SparkImpl.from_str(spark_impl_name)
    spark_family = spark_impl.get_family()
    spark_version = spark_impl.get_spark_version()
    scala_version = spark_impl.get_scala_version()
    python_version = spark_impl.get_python_version()
    spark_version_for_sedona = SparkSedona.getSparkVersionForSedona(spark_version, sedona_version)
    geotools_wrapper_version = SparkSedona.getGeotoolsWrapperVersion(sedona_version)
    run_identifier = f"{full_job_name}_render"

    if isinstance(parameters, str):
        resolved_parameters = parameters
    elif isinstance(parameters, list):
        resolved_parameters = {
            split_param[0]: split_param[1].strip()
            for split_param in [param.split("=", 1) for param in parameters]
            if split_param[1].strip()
        }
    else:
        resolved_parameters = json.dumps(parameters)

    spark_jar_paths_list = spark_jar_paths.split(",") if spark_jar_paths else []

    return {
        "job_name": full_job_name,
        "spark_impl": spark_impl,
        "spark_impl_name": spark_impl_name,
        "spark_family": spark_family,
        "spark_version": spark_version,
        "scala_version": scala_version,
        "python_version": python_version,
        "sedona_version": sedona_version,
        "spark_version_for_sedona": spark_version_for_sedona,
        "geotools_wrapper_version": geotools_wrapper_version,
        "run_identifier": run_identifier,
        "py_pi_client": _StubPyPiClient(),
        "parameters": resolved_parameters,
        "spark_jar_paths": spark_jar_paths_list,
        "s3_assets_bucket": artifact_store.s3_bucket,
        "s3_assets_root": artifact_store.s3_root,
        "job_runner_wheel_prefix": artifact_store.job_runner_wheel_prefix,
        "force_pip_packages": list(artifact_store.force_pip_packages),
        "runner_script_overrides": dict(artifact_store.runner_script_overrides),
        "wherobots_external_id": wherobots_config.external_id,
        "wherobots_role_arn": wherobots_config.role_arn,
        "aws_region": wherobots_config.aws_region,
        "databricks_conf": databricks_config.cluster_conf,
        "databricks_extra_libraries": list(databricks_config.extra_libraries),
        "databricks_dbfs_root_template": databricks_config.dbfs_root_template,
        "databricks_workspace_scripts_path_template": (
            databricks_config.workspace_scripts_path_template
        ),
        "databricks_cluster_init_script_name": (databricks_config.cluster_init_script_name),
        "databricks_custom_tags": dict(databricks_config.custom_tags),
        "databricks_spark_conf": dict(databricks_config.spark_conf),
        "databricks_spark_env_vars": dict(databricks_config.spark_env_vars),
        "databricks_worker_instance_types": dict(databricks_config.worker_instance_types),
        "databricks_driver_node_type": databricks_config.driver_node_type,
        "databricks_spark_version": databricks_config.spark_version,
        "databricks_gpu": databricks_config.gpu,
        "glue_execution_class": glue_config.execution_class,
        "iam_role_name": glue_config.iam_role_name,
        "codeartifact_domain_owner": package_registry.domain_owner,
        "codeartifact_domain": package_registry.domain,
        "codeartifact_repository": package_registry.repository,
        "codeartifact_region": package_registry.region,
        "codeartifact_maven_repository": package_registry.maven_repository,
        "codeartifact_maven_repository_path": (
            package_registry.maven_repository_path
            or (
                f"maven/{package_registry.maven_repository}"
                if package_registry.maven_repository
                else ""
            )
        ),
    }


def _runner_s3_uri(platform: str, bucket: str, prefix: str) -> str:
    """Compute the content-hash-keyed S3 URI for a bundled runner script.

    Mirrors the key format used by ``upload_runners_to_s3`` so render output
    shows the exact S3 path that will be used at real submission time.
    """
    try:
        local = get_runner_path(platform)
        sha = _file_sha256(local)[:12]
    except Exception:  # noqa: BLE001
        sha = "unknown000000"
    finally:
        # get_runner_path("glue_scala") writes a temp file; clean it up.
        if platform == "glue_scala":
            try:
                local.unlink()
            except Exception:  # noqa: BLE001
                pass
    name = _RUNNER_FILES[platform]
    return f"s3://{bucket}/{prefix}/runners/{sha}-{name}"


def _stub_package_info_glue(
    setup_info: dict,
    pre_resolved: dict | None,
) -> dict:
    if pre_resolved is not None:
        return pre_resolved
    bucket = setup_info["s3_assets_bucket"] or "REPLACE-ME-bucket"
    root = setup_info["s3_assets_root"]
    prefix = f"{root}/{setup_info['run_identifier']}"
    overrides = setup_info.get("runner_script_overrides") or {}
    script_location = overrides.get("glue") or _runner_s3_uri("glue", bucket, root)
    scala_script_location = overrides.get("glue_scala") or _runner_s3_uri(
        "glue_scala", bucket, root
    )
    return {
        "py_files": f"s3://{bucket}/{prefix}/python_wheels/REPLACE-ME.whl",
        "script_location": script_location,
        "scala_script_location": scala_script_location,
        "s3_bucket": bucket,
        "s3_prefix": prefix,
        "native_packages": [],
    }


def _stub_jar_info_glue(setup_info: dict, pre_resolved: dict | None) -> dict:
    if pre_resolved is not None:
        return pre_resolved
    bucket = setup_info["s3_assets_bucket"] or "REPLACE-ME-bucket"
    sedona_packages = ",".join(
        SparkSedona.getSedonaJarPackages(
            sedona_version=setup_info["sedona_version"],
            py_spark_version=setup_info["spark_version"],
            scala_version=setup_info["scala_version"],
        )
    )
    return {
        "jars_s3": f"s3://{bucket}/scala_jars/REPLACE-ME.jar",
        "sedona_packages": sedona_packages,
        "sedona_module": f"apache-sedona=={setup_info['sedona_version']}",
    }


def _stub_package_info_wherobots(setup_info: dict, pre_resolved: dict | None) -> dict:
    if pre_resolved is not None:
        return pre_resolved
    bucket = setup_info["s3_assets_bucket"] or "REPLACE-ME-bucket"
    root = setup_info["s3_assets_root"]
    overrides = setup_info.get("runner_script_overrides") or {}
    script = overrides.get("wherobots") or _runner_s3_uri("wherobots", bucket, root)
    placeholder = f"s3://{bucket}/{root}/python_wheels/REPLACE-ME.whl"
    return {
        "py_files": [placeholder],
        "script_location": script,
        "python_packages_or_jars_list": [{"sourceType": "FILE", "filePath": placeholder}],
    }


def _stub_jar_info_wherobots(setup_info: dict, pre_resolved: dict | None) -> dict:
    if pre_resolved is not None:
        return pre_resolved
    return {"jars_s3": []}


def _glue_cli(
    setup_info: dict, operator_kwargs: dict, payload_path_var: str = "$PAYLOAD"
) -> list[str]:
    job_name = setup_info["job_name"]
    region = setup_info["aws_region"]
    return [
        "# Create or update the Glue job from the create_job_kwargs payload.",
        f"aws glue create-job --region {shlex.quote(region)} "
        f"--name {shlex.quote(job_name)} "
        f"--role {shlex.quote(operator_kwargs['iam_role_name'])} "
        f"--cli-input-json file://{payload_path_var}/create_job_kwargs.json",
        "# Start a job run with the resolved script arguments.",
        f"aws glue start-job-run --region {shlex.quote(region)} "
        f"--job-name {shlex.quote(job_name)} "
        f"--arguments file://{payload_path_var}/script_args.json",
    ]


def _databricks_cli(payload_path_var: str = "$PAYLOAD") -> list[str]:
    return [
        "# Requires the `databricks` CLI configured for your workspace.",
        f"databricks jobs submit --json @{payload_path_var}/submit_payload.json",
    ]


def _wherobots_cli(payload_path_var: str = "$PAYLOAD") -> list[str]:
    return [
        "# No public CLI; submit via Wherobots SDK or REST.",
        "# python -c 'from wherobots.db import connect; ...'",
        f"# Payload: {payload_path_var}/submit_payload.json",
    ]


def render_spark_job(
    spark_impl_name: str,
    module_name: str,
    class_name: str,
    parameters,
    sedona_version: str = "1.7.0",
    job_name: str = "",
    python_packages: str = "",
    spark_jar_paths: str = "",
    extra_spark_conf: dict | None = None,
    extra_spark_env_vars: str = "{}",
    spark_cluster_size: str = "",
    spark_cluster_desired_worker_cores: str = "40",
    spark_cluster_desired_workers: str = "",
    iceberg_config: IcebergConfig | None = None,
    package_registry: PackageRegistryConfig | None = None,
    artifact_store: ArtifactStoreConfig | None = None,
    glue_config: GlueConfig | None = None,
    databricks_config: DatabricksConfig | None = None,
    wherobots_config: WherobotsConfig | None = None,
    task_id: str = "execute_spark_job",
    dag_id: str = "",
    pre_resolved_package_info: dict | None = None,
    pre_resolved_jar_info: dict | None = None,
) -> RenderResult:
    """Render the platform submission payload without invoking Airflow.

    All arguments mirror ``spark_agnostic_task_group`` 1:1. Pass
    ``pre_resolved_package_info`` / ``pre_resolved_jar_info`` to use real S3
    URIs from a previous ``download_python_packages_*`` / ``download_jars_*``
    run; otherwise placeholder URIs (``s3://.../REPLACE-ME.whl``) are emitted
    so the operator kwargs structure is still complete.
    """
    package_registry = package_registry or PackageRegistryConfig(
        domain_owner="", domain="", repository=""
    )
    artifact_store = artifact_store or ArtifactStoreConfig(s3_bucket="")
    glue_config = glue_config or GlueConfig()
    databricks_config = databricks_config or DatabricksConfig()
    wherobots_config = wherobots_config or WherobotsConfig()
    iceberg_config = iceberg_config or IcebergConfig()
    extra_spark_conf = extra_spark_conf or {}

    setup_info = _build_render_setup_info(
        spark_impl_name=spark_impl_name,
        sedona_version=sedona_version,
        module_name=module_name,
        class_name=class_name,
        job_name=job_name,
        parameters=parameters,
        spark_jar_paths=spark_jar_paths,
        package_registry=package_registry,
        artifact_store=artifact_store,
        glue_config=glue_config,
        databricks_config=databricks_config,
        wherobots_config=wherobots_config,
    )

    family: SparkFamily = setup_info["spark_family"]

    # Resolve which Iceberg config variant applies for this family.
    iceberg_spark_config = _select_iceberg_spark_config(iceberg_config, family) or None

    if family == SparkFamily.GLUE:
        package_info = _stub_package_info_glue(setup_info, pre_resolved_package_info)
        jar_info = _stub_jar_info_glue(setup_info, pre_resolved_jar_info)

        handler = GluePlatformHandler(setup_info)
        cluster_spec = handler.setup_cluster(
            python_packages=python_packages,
            spark_jar_paths=spark_jar_paths,
            extra_spark_conf=extra_spark_conf,
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            iceberg_spark_config=iceberg_spark_config,
        )
        merged_conf = cluster_spec["merged_spark_conf"]

        built = build_glue_operator_kwargs(
            setup_info=setup_info,
            package_info=package_info,
            jar_info=jar_info,
            module_name=module_name,
            class_name=class_name,
            extra_spark_conf=merged_conf,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            iam_role_name=glue_config.iam_role_name,
            task_id=task_id,
            dag_id=dag_id,
            execution_class=glue_config.execution_class,
        )
        submit_payload = {
            "create_job_kwargs": built["create_job_kwargs"],
            "script_args": built["script_args"],
            "script_location": built["script_location"],
        }
        cli = _glue_cli(setup_info, built["operator_kwargs"])
        return RenderResult(
            platform="glue",
            spark_impl_name=spark_impl_name,
            setup_info=setup_info,
            merged_spark_conf=merged_conf,
            cluster_spec=cluster_spec,
            operator_kwargs=built["operator_kwargs"],
            submit_payload=submit_payload,
            cli=cli,
        )

    if family == SparkFamily.DATABRICKS:
        cluster_info = setup_databricks_cluster(
            setup_info=setup_info,
            python_packages=python_packages,
            spark_jar_paths=spark_jar_paths,
            extra_spark_conf=_merge_spark_conf({}, iceberg_spark_config, extra_spark_conf),
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
        )
        built = build_databricks_operator_kwargs(
            setup_info=setup_info,
            cluster_info=cluster_info,
            module_name=module_name,
            class_name=class_name,
            task_id=task_id,
        )
        cli = _databricks_cli()
        return RenderResult(
            platform="databricks",
            spark_impl_name=spark_impl_name,
            setup_info=setup_info,
            merged_spark_conf=cluster_info["new_cluster"]["spark_conf"],
            cluster_spec=cluster_info,
            operator_kwargs=built["operator_kwargs"],
            submit_payload=built["submit_payload"],
            cli=cli,
        )

    if family == SparkFamily.WHEROBOTS:
        package_info = _stub_package_info_wherobots(setup_info, pre_resolved_package_info)
        jar_info = _stub_jar_info_wherobots(setup_info, pre_resolved_jar_info)

        handler = WherobotsPlatformHandler(setup_info)
        cluster_spec = handler.setup_cluster(
            python_packages=python_packages,
            spark_jar_paths=spark_jar_paths,
            extra_spark_conf=extra_spark_conf,
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            iceberg_spark_config=iceberg_spark_config,
        )
        merged_conf = cluster_spec["merged_spark_conf"]

        built = build_wherobots_operator_kwargs(
            setup_info=setup_info,
            package_info=package_info,
            jar_info=jar_info,
            module_name=module_name,
            class_name=class_name,
            extra_spark_conf=merged_conf,
            spark_cluster_size=spark_cluster_size,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            wherobots_role_arn=wherobots_config.role_arn,
            task_id=task_id,
            resolve_region=False,
        )
        cli = _wherobots_cli()
        return RenderResult(
            platform="wherobots",
            spark_impl_name=spark_impl_name,
            setup_info=setup_info,
            merged_spark_conf=merged_conf,
            cluster_spec=cluster_spec,
            operator_kwargs=built["operator_kwargs"],
            submit_payload=built["submit_payload"],
            cli=cli,
        )

    raise ValueError(f"Unsupported Spark family: {family}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _load_config(path: str | None) -> dict:
    if not path:
        return {}
    with open(path, encoding="utf-8") as fh:
        return json.load(fh)


def _build_config_obj(cls, data: dict | None):
    if not data:
        return None
    return cls(**data)


def _cli(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m overture_airflow_provider.render",
        description="Render Spark job submission payloads without invoking Airflow.",
    )
    parser.add_argument("--config", help="Path to a JSON config file.", default=None)
    parser.add_argument("--spark-impl", dest="spark_impl_name")
    parser.add_argument("--module-name", default="")
    parser.add_argument("--class-name", default="")
    parser.add_argument("--job-name", default="")
    parser.add_argument("--sedona-version", default="1.7.0")
    parser.add_argument("--python-packages", default="")
    parser.add_argument("--spark-jar-paths", default="")
    parser.add_argument("--parameters", default="{}", help="JSON string or {} for empty.")
    parser.add_argument("--spark-cluster-size", default="")
    parser.add_argument("--spark-cluster-desired-worker-cores", default="40")
    parser.add_argument("--spark-cluster-desired-workers", default="")
    parser.add_argument("--task-id", default="execute_spark_job")
    parser.add_argument("--dag-id", default="")
    parser.add_argument(
        "--out",
        default=None,
        help="If set, write rendered JSON payloads + cli.sh to this directory.",
    )
    args = parser.parse_args(argv)

    config = _load_config(args.config)

    def _pick(key: str, default):
        return config.get(key, default)

    parameters = _pick("parameters", args.parameters)
    if isinstance(parameters, str):
        try:
            parameters = json.loads(parameters)
        except (TypeError, ValueError):
            pass

    result = render_spark_job(
        spark_impl_name=_pick("spark_impl_name", args.spark_impl_name),
        module_name=_pick("module_name", args.module_name),
        class_name=_pick("class_name", args.class_name),
        job_name=_pick("job_name", args.job_name),
        sedona_version=_pick("sedona_version", args.sedona_version),
        python_packages=_pick("python_packages", args.python_packages),
        spark_jar_paths=_pick("spark_jar_paths", args.spark_jar_paths),
        parameters=parameters,
        extra_spark_conf=_pick("extra_spark_conf", {}),
        spark_cluster_size=_pick("spark_cluster_size", args.spark_cluster_size),
        spark_cluster_desired_worker_cores=_pick(
            "spark_cluster_desired_worker_cores",
            args.spark_cluster_desired_worker_cores,
        ),
        spark_cluster_desired_workers=_pick(
            "spark_cluster_desired_workers", args.spark_cluster_desired_workers
        ),
        iceberg_config=_build_config_obj(IcebergConfig, config.get("iceberg")),
        package_registry=_build_config_obj(PackageRegistryConfig, config.get("package_registry")),
        artifact_store=_build_config_obj(ArtifactStoreConfig, config.get("artifact_store")),
        glue_config=_build_config_obj(GlueConfig, config.get("glue")),
        databricks_config=_build_config_obj(DatabricksConfig, config.get("databricks")),
        wherobots_config=_build_config_obj(WherobotsConfig, config.get("wherobots")),
        task_id=_pick("task_id", args.task_id),
        dag_id=_pick("dag_id", args.dag_id),
        pre_resolved_package_info=config.get("pre_resolved_package_info"),
        pre_resolved_jar_info=config.get("pre_resolved_jar_info"),
    )

    if args.out:
        written = result.write_to(args.out)
        print(f"# Rendered {result.platform} payload to {args.out}:", file=sys.stderr)
        for name, path in written.items():
            print(f"#   {name} -> {path}", file=sys.stderr)
        print("# CLI:", file=sys.stderr)
        for line in result.cli:
            print(line)
    else:
        json.dump(result.to_dict(), sys.stdout, indent=2, sort_keys=True, default=str)
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(_cli())
