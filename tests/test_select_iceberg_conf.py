"""Tests for _select_iceberg_conf with S3 Tables catalog coexistence."""

import json

import pytest

from overture_airflow_provider.config import IcebergConfig
from overture_airflow_provider.spark_agnostic_taskgroup import _select_iceberg_conf


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
        "spark.sql.catalog.s3tables_catalog.rest.sigv4-enabled": "true",
        "spark.sql.catalog.s3tables_catalog.rest.signing-name": "s3tables",
        "spark.sql.catalog.s3tables_catalog.rest.signing-region": "us-west-2",
    }


def _wherobots_catalog_config():
    return {
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.iceberg_catalog.warehouse": "s3://my-bucket/warehouse",
    }


class TestSelectIcebergConfNone:
    def test_returns_empty_when_config_is_none(self):
        assert _select_iceberg_conf(None, "GLUE") == {}
        assert _select_iceberg_conf(None, "DATABRICKS") == {}
        assert _select_iceberg_conf(None, "WHEROBOTS") == {}


class TestSelectIcebergConfPrimaryOnly:
    def test_glue_returns_spark_config(self):
        cfg = IcebergConfig(spark_config=json.dumps(_rest_catalog_config()))
        result = _select_iceberg_conf(cfg, "GLUE")
        assert result == _rest_catalog_config()

    def test_databricks_returns_spark_config(self):
        cfg = IcebergConfig(spark_config=json.dumps(_rest_catalog_config()))
        result = _select_iceberg_conf(cfg, "DATABRICKS")
        assert result == _rest_catalog_config()

    def test_wherobots_returns_wherobots_spark_config(self):
        cfg = IcebergConfig(wherobots_spark_config=json.dumps(_wherobots_catalog_config()))
        result = _select_iceberg_conf(cfg, "WHEROBOTS")
        assert result == _wherobots_catalog_config()

    def test_invalid_json_identifies_field_name(self):
        cfg = IcebergConfig(spark_config="{not-json")

        with pytest.raises(ValueError, match=r"IcebergConfig\.spark_config"):
            _select_iceberg_conf(cfg, "GLUE")

    def test_non_object_json_identifies_field_name(self):
        cfg = IcebergConfig(wherobots_s3tables_spark_config='["not", "a", "dict"]')

        with pytest.raises(
            ValueError,
            match=r"IcebergConfig\.wherobots_s3tables_spark_config must decode to a JSON object",
        ):
            _select_iceberg_conf(cfg, "WHEROBOTS")


class TestSelectIcebergConfDictInput:
    """On DAGs with render_template_as_native_obj=True, Airflow's native renderer
    literal_evals a rendered JSON-object op_kwarg back into a dict, so the
    IcebergConfig fields arrive already parsed. _select_iceberg_conf must accept
    dicts as well as JSON strings (regression for the json.loads(dict) TypeError).
    """

    def test_glue_accepts_dict_spark_config(self):
        cfg = IcebergConfig(spark_config=_rest_catalog_config())
        assert _select_iceberg_conf(cfg, "GLUE") == _rest_catalog_config()

    def test_wherobots_accepts_dict_spark_config(self):
        cfg = IcebergConfig(wherobots_spark_config=_wherobots_catalog_config())
        assert _select_iceberg_conf(cfg, "WHEROBOTS") == _wherobots_catalog_config()

    def test_glue_merges_dict_primary_and_dict_s3tables(self):
        cfg = IcebergConfig(
            spark_config=_rest_catalog_config(),
            s3tables_spark_config=_s3tables_catalog_config(),
        )
        result = _select_iceberg_conf(cfg, "GLUE")
        assert "spark.sql.catalog.iceberg_catalog" in result
        assert "spark.sql.catalog.s3tables_catalog" in result

    def test_dict_with_int_value_preserved(self):
        """Spark config values may be ints (e.g. max-connections); native render
        keeps them as ints in the dict, which must survive untouched."""
        cfg = IcebergConfig(
            spark_config={
                "spark.sql.catalog.s3tables_catalog.http-client.apache.max-connections": 3000
            }
        )
        result = _select_iceberg_conf(cfg, "GLUE")
        assert (
            result["spark.sql.catalog.s3tables_catalog.http-client.apache.max-connections"] == 3000
        )


class TestSelectIcebergConfS3Tables:
    def test_glue_merges_primary_and_s3tables(self):
        cfg = IcebergConfig(
            spark_config=json.dumps(_rest_catalog_config()),
            s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
        )
        result = _select_iceberg_conf(cfg, "GLUE")
        # Both catalog configs present
        assert "spark.sql.catalog.iceberg_catalog" in result
        assert "spark.sql.catalog.s3tables_catalog" in result
        assert result["spark.sql.catalog.s3tables_catalog.rest.signing-name"] == "s3tables"

    def test_databricks_merges_primary_and_s3tables(self):
        cfg = IcebergConfig(
            spark_config=json.dumps(_rest_catalog_config()),
            s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
        )
        result = _select_iceberg_conf(cfg, "DATABRICKS")
        assert "spark.sql.catalog.iceberg_catalog" in result
        assert "spark.sql.catalog.s3tables_catalog" in result

    def test_wherobots_merges_primary_and_s3tables(self):
        wherobots_s3t = {
            "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.s3tables_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        }
        cfg = IcebergConfig(
            wherobots_spark_config=json.dumps(_wherobots_catalog_config()),
            wherobots_s3tables_spark_config=json.dumps(wherobots_s3t),
        )
        result = _select_iceberg_conf(cfg, "WHEROBOTS")
        assert "spark.sql.catalog.iceberg_catalog" in result
        assert "spark.sql.catalog.s3tables_catalog" in result

    def test_s3tables_only_without_primary(self):
        cfg = IcebergConfig(
            s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
        )
        result = _select_iceberg_conf(cfg, "GLUE")
        assert "spark.sql.catalog.s3tables_catalog" in result
        assert "spark.sql.catalog.iceberg_catalog" not in result

    def test_empty_s3tables_does_not_change_primary(self):
        cfg = IcebergConfig(
            spark_config=json.dumps(_rest_catalog_config()),
            s3tables_spark_config="{}",
        )
        result = _select_iceberg_conf(cfg, "GLUE")
        assert result == _rest_catalog_config()

    def test_extra_spark_conf_still_wins_downstream(self):
        """Verify that S3 Tables keys can still be overridden by extra_spark_conf
        (tested at the handler level, not here — this confirms merge order)."""
        cfg = IcebergConfig(
            spark_config=json.dumps(_rest_catalog_config()),
            s3tables_spark_config=json.dumps(_s3tables_catalog_config()),
        )
        result = _select_iceberg_conf(cfg, "GLUE")
        # S3 Tables values are present as-is before extra_spark_conf is applied
        assert result["spark.sql.catalog.s3tables_catalog.rest.signing-region"] == "us-west-2"
