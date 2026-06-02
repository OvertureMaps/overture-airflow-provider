"""Tests for the Airflow-free render module."""

import json
import os
import re

import pytest

from overture_airflow_provider.config import IcebergConfig
from overture_airflow_provider.render import (
    RenderResult,
    _jsonify,
    render_spark_job,
)
from overture_airflow_provider.spark import SparkFamily, SparkImpl

_COMMON_KWARGS = dict(
    module_name="my_module",
    class_name="MyJob",
    parameters={"date": "2024-01-01"},
    job_name="snapshot",
)


def _rest_catalog_config():
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "iceberg_catalog",
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        "spark.sql.catalog.iceberg_catalog.uri": "https://glue.us-west-2.amazonaws.com/iceberg",
    }


def _s3tables_catalog_config():
    return {
        "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables_catalog.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        "spark.sql.catalog.s3tables_catalog.uri": "https://s3tables.us-west-2.amazonaws.com/iceberg",
        "spark.sql.catalog.s3tables_catalog.warehouse": "arn:aws:s3tables:us-west-2:123456789012:bucket/my-bucket",
        "spark.sql.catalog.s3tables_catalog.rest.signing-name": "s3tables",
    }


def _wherobots_catalog_config():
    return {
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.iceberg_catalog.warehouse": "s3://my-bucket/warehouse",
    }


def _wherobots_s3tables_catalog_config():
    return {
        "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.s3tables_catalog.warehouse": "s3://my-bucket/s3tables-warehouse",
    }


@pytest.mark.parametrize(
    "spark_impl_name, platform",
    [
        ("GLUE_v5", "glue"),
        ("DATABRICKS_v15", "databricks"),
        ("WHEROBOTS_v1_5_0", "wherobots"),
    ],
)
def test_render_returns_complete_result(spark_impl_name, platform):
    result = render_spark_job(spark_impl_name=spark_impl_name, **_COMMON_KWARGS)

    assert isinstance(result, RenderResult)
    assert result.platform == platform
    assert result.spark_impl_name == spark_impl_name
    assert isinstance(result.operator_kwargs, dict) and result.operator_kwargs
    assert isinstance(result.merged_spark_conf, dict)
    assert result.submit_payload is not None
    assert isinstance(result.cli, list) and result.cli

    # to_dict must be JSON-serialisable (enums coerced via _jsonify).
    json.dumps(result.to_dict(), default=str)


def test_render_glue_emits_create_job_and_script_args():
    result = render_spark_job(spark_impl_name="GLUE_v5", **_COMMON_KWARGS)

    payload = result.submit_payload
    assert "create_job_kwargs" in payload
    assert "script_args" in payload
    assert payload["script_location"].startswith("s3://")
    # Parameters propagate as JSON string in script args.
    assert "--params" in payload["script_args"]
    assert json.loads(payload["script_args"]["--params"])["date"] == "2024-01-01"


def test_render_databricks_emits_submit_payload():
    result = render_spark_job(spark_impl_name="DATABRICKS_v15", **_COMMON_KWARGS)

    payload = result.submit_payload
    assert "new_cluster" in payload
    assert payload["new_cluster"]["spark_version"] == "15.4.x-scala2.12"
    assert payload["run_name"].endswith("_render")


def test_render_wherobots_skips_region_resolution():
    """Render mode must work even when the wherobots SDK is unavailable."""
    result = render_spark_job(spark_impl_name="WHEROBOTS_v1_5_0", **_COMMON_KWARGS)
    # Wherobots payload has no AWS Region enum — region stays a plain string.
    region = result.operator_kwargs.get("region") or result.submit_payload.get("region")
    assert isinstance(region, str)


@pytest.mark.parametrize(
    "spark_impl_name, iceberg_config, expected_primary, expected_s3tables",
    [
        (
            "GLUE_v5",
            IcebergConfig(
                spark_config=json.dumps(_rest_catalog_config()),
                s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
            ),
            _rest_catalog_config(),
            _s3tables_catalog_config(),
        ),
        (
            "DATABRICKS_v15",
            IcebergConfig(
                spark_config=json.dumps(_rest_catalog_config()),
                s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
            ),
            _rest_catalog_config(),
            _s3tables_catalog_config(),
        ),
        (
            "WHEROBOTS_v1_5_0",
            IcebergConfig(
                wherobots_spark_config=json.dumps(_wherobots_catalog_config()),
                wherobots_s3tables_spark_config=json.dumps(_wherobots_s3tables_catalog_config()),
            ),
            _wherobots_catalog_config(),
            _wherobots_s3tables_catalog_config(),
        ),
    ],
)
def test_render_merges_primary_and_s3tables_iceberg_configs(
    spark_impl_name, iceberg_config, expected_primary, expected_s3tables
):
    result = render_spark_job(
        spark_impl_name=spark_impl_name,
        iceberg_config=iceberg_config,
        **_COMMON_KWARGS,
    )

    assert result.merged_spark_conf.items() >= expected_primary.items()
    assert result.merged_spark_conf.items() >= expected_s3tables.items()


@pytest.mark.parametrize(
    "spark_impl_name, iceberg_config, expected_s3tables",
    [
        (
            "GLUE_v5",
            IcebergConfig(s3tables_spark_config=json.dumps(_s3tables_catalog_config())),
            _s3tables_catalog_config(),
        ),
        (
            "DATABRICKS_v15",
            IcebergConfig(s3tables_spark_config=json.dumps(_s3tables_catalog_config())),
            _s3tables_catalog_config(),
        ),
        (
            "WHEROBOTS_v1_5_0",
            IcebergConfig(
                wherobots_s3tables_spark_config=json.dumps(_wherobots_s3tables_catalog_config())
            ),
            _wherobots_s3tables_catalog_config(),
        ),
    ],
)
def test_render_preserves_s3tables_only_iceberg_configs(
    spark_impl_name, iceberg_config, expected_s3tables
):
    result = render_spark_job(
        spark_impl_name=spark_impl_name,
        iceberg_config=iceberg_config,
        **_COMMON_KWARGS,
    )

    assert result.merged_spark_conf.items() >= expected_s3tables.items()
    assert "spark.sql.catalog.iceberg_catalog" not in result.merged_spark_conf


@pytest.mark.parametrize(
    "spark_impl_name, iceberg_config, expected_error",
    [
        (
            "GLUE_v5",
            IcebergConfig(spark_config="[]"),
            "IcebergConfig.spark_config must decode to a JSON object, got list",
        ),
        (
            "DATABRICKS_v15",
            IcebergConfig(s3tables_spark_config='{"bad"'),
            "Invalid JSON in IcebergConfig.s3tables_spark_config",
        ),
        (
            "WHEROBOTS_v1_5_0",
            IcebergConfig(wherobots_s3tables_spark_config='"bad"'),
            "IcebergConfig.wherobots_s3tables_spark_config must decode to a JSON object, got str",
        ),
    ],
)
def test_render_rejects_invalid_iceberg_json(spark_impl_name, iceberg_config, expected_error):
    with pytest.raises(ValueError, match=re.escape(expected_error)):
        render_spark_job(
            spark_impl_name=spark_impl_name,
            iceberg_config=iceberg_config,
            **_COMMON_KWARGS,
        )


_SCALA_KWARGS = dict(
    module_name="",
    class_name="com.example.Main",
    parameters={"date": "2024-01-01"},
    job_name="snapshot",
)


def test_render_glue_scala_emits_conf_in_default_args():
    """Glue Scala render must include Iceberg catalog conf in DefaultArguments['--conf']."""
    result = render_spark_job(
        spark_impl_name="GLUE_v5",
        iceberg_config=IcebergConfig(
            spark_config=json.dumps(_rest_catalog_config()),
            s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
        ),
        **_SCALA_KWARGS,
    )
    default_args = result.submit_payload["create_job_kwargs"]["DefaultArguments"]
    assert "--conf" in default_args, "Glue Scala job must inject Iceberg conf via --conf"
    conf_str = default_args["--conf"]
    assert "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" in conf_str
    assert "spark.sql.catalog.s3tables_catalog" in conf_str
    # Excluded keys must not appear.
    assert "spark.jars.packages" not in conf_str
    assert "spark.driver.extraJavaOptions" not in conf_str
    assert "spark.executor.extraJavaOptions" not in conf_str


def test_render_glue_pyspark_injects_conf_in_default_args():
    """Glue PySpark render must inject Iceberg catalog conf into DefaultArguments['--conf'].

    Catalog plugin keys and spark.sql.extensions must be applied by Glue at session-creation
    time; the runner cannot reliably register them at runtime after getOrCreate().
    """
    result = render_spark_job(
        spark_impl_name="GLUE_v5",
        iceberg_config=IcebergConfig(spark_config=json.dumps(_rest_catalog_config())),
        **_COMMON_KWARGS,
    )
    default_args = result.submit_payload["create_job_kwargs"]["DefaultArguments"]
    assert "--conf" in default_args, "PySpark Glue job must inject Iceberg conf via --conf"
    conf_str = default_args["--conf"]
    assert "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" in conf_str
    assert "spark.sql.extensions=" in conf_str
    assert "spark.jars.packages" not in conf_str


def test_render_write_to_creates_files(tmp_path):
    result = render_spark_job(spark_impl_name="GLUE_v5", **_COMMON_KWARGS)
    written = result.write_to(str(tmp_path))

    assert set(written) >= {"operator_kwargs.json", "merged_spark_conf.json", "cli.sh"}
    for name, path in written.items():
        assert os.path.exists(path)
        if name.endswith(".json"):
            with open(path) as fh:
                json.load(fh)  # must be valid JSON
        if name == "cli.sh":
            with open(path) as fh:
                content = fh.read()
            assert content.startswith("#!/usr/bin/env bash")


def test_render_accepts_pre_resolved_package_info():
    overrides = {
        "py_files": "s3://my-bucket/wheels/my-pkg-1.0.0.whl",
        "script_location": "s3://my-bucket/scripts/runner.py",
        "scala_script_location": "s3://my-bucket/scripts/runner.scala",
        "s3_bucket": "my-bucket",
        "s3_prefix": "prefix",
        "native_packages": [],
    }
    result = render_spark_job(
        spark_impl_name="GLUE_v5",
        pre_resolved_package_info=overrides,
        **_COMMON_KWARGS,
    )
    assert result.submit_payload["script_location"] == "s3://my-bucket/scripts/runner.py"


def test_jsonify_handles_enums_and_nesting():
    obj = {
        "family": SparkFamily.GLUE,
        "impl": SparkImpl.GLUE_v5,
        "nested": [{"x": SparkFamily.WHEROBOTS}],
        "plain": "string",
    }
    out = _jsonify(obj)
    assert out["family"] == "GLUE"
    assert out["impl"] == "GLUE_v5"
    assert out["nested"][0]["x"] == "WHEROBOTS"
    assert out["plain"] == "string"
    # Result must round-trip through JSON.
    json.dumps(out)
