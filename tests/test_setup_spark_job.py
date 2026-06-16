"""Tests for setup_spark_job."""

import json
from unittest.mock import MagicMock, patch

import pytest

from overture_airflow_provider._setup import setup_spark_job
from overture_airflow_provider.spark import SparkFamily, SparkImpl

_GLUE_V5 = "GLUE_v5"
_SEDONA = "1.7.0"


def _run(
    spark_impl_name=_GLUE_V5,
    sedona_version=_SEDONA,
    module_name="my_module",
    class_name="MyClass",
    job_name="",
    parameters="{}",
    spark_jar_paths="",
):
    with patch(
        "overture_airflow_provider._setup.CodeArtifactPyPiClient",
        return_value=MagicMock(),
    ):
        return setup_spark_job(
            spark_impl_name=spark_impl_name,
            sedona_version=sedona_version,
            module_name=module_name,
            class_name=class_name,
            job_name=job_name,
            parameters=parameters,
            spark_jar_paths=spark_jar_paths,
        )


class TestJobNameAssembly:
    def test_module_and_class(self):
        result = _run(module_name="pkg.mod", class_name="Worker", job_name="")
        assert result["job_name"] == "pkg.mod.Worker"

    def test_module_class_and_job_name(self):
        result = _run(module_name="pkg.mod", class_name="Worker", job_name="run_v2")
        assert result["job_name"] == "pkg.mod.Worker.run_v2"

    def test_class_only_no_module(self):
        result = _run(module_name="", class_name="com.example.Main", job_name="")
        assert result["job_name"] == "com.example.Main"

    def test_job_name_only(self):
        result = _run(module_name="", class_name="", job_name="standalone_job")
        assert result["job_name"] == "standalone_job"

    def test_empty_parts_are_excluded(self):
        result = _run(module_name="mod", class_name="Cls", job_name="")
        assert result["job_name"] == "mod.Cls"
        assert not result["job_name"].endswith(".")


class TestVersionDerivation:
    def test_glue_v5_versions(self):
        result = _run(spark_impl_name="GLUE_v5", sedona_version="1.7.0")
        assert result["spark_version"] == "3.5.2"
        assert result["scala_version"] == "2.12"
        assert result["python_version"] == "3.11"
        assert result["sedona_version"] == "1.7.0"

    def test_glue_v4_versions(self):
        result = _run(spark_impl_name="GLUE_v4", sedona_version="1.6.1")
        assert result["spark_version"] == "3.3.0"
        assert result["python_version"] == "3.10"

    def test_spark_version_for_sedona_derived(self):
        result = _run(spark_impl_name="GLUE_v5", sedona_version="1.7.0")
        assert result["spark_version_for_sedona"] == "3.5"

    def test_geotools_version_derived(self):
        result = _run(sedona_version="1.7.0")
        assert result["geotools_wrapper_version"] == "28.5"

    def test_spark_family_is_enum(self):
        result = _run(spark_impl_name="GLUE_v5")
        assert result["spark_family"] == SparkFamily.GLUE

    def test_spark_impl_is_enum(self):
        result = _run(spark_impl_name="GLUE_v5")
        assert result["spark_impl"] == SparkImpl.GLUE_v5

    def test_unknown_impl_raises(self):
        with pytest.raises(ValueError, match="not a valid SparkImpl"):
            _run(spark_impl_name="GLUE_v99")


class TestParameterParsing:
    def test_string_passthrough(self):
        params = '{"key": "value"}'
        result = _run(parameters=params)
        assert result["parameters"] == params

    def test_dict_serialised_to_json(self):
        params = {"key": "value", "num": 42}
        result = _run(parameters=params)
        assert json.loads(result["parameters"]) == params

    def test_list_of_key_equals_value(self):
        result = _run(parameters=["key1=val1", "key2=val2"])
        assert result["parameters"]["key1"] == "val1"
        assert result["parameters"]["key2"] == "val2"

    def test_list_with_value_containing_equals(self):
        result = _run(parameters=["url=https://host/path?a=1"])
        assert result["parameters"]["url"] == "https://host/path?a=1"

    def test_empty_string_params(self):
        result = _run(parameters="{}")
        assert result["parameters"] == "{}"


class TestJarPathSplitting:
    def test_empty_string_gives_empty_list(self):
        result = _run(spark_jar_paths="")
        assert result["spark_jar_paths"] == []

    def test_single_jar(self):
        result = _run(spark_jar_paths="s3://bucket/my.jar")
        assert result["spark_jar_paths"] == ["s3://bucket/my.jar"]

    def test_multiple_jars_comma_separated(self):
        result = _run(spark_jar_paths="s3://bucket/a.jar,s3://bucket/b.jar")
        assert result["spark_jar_paths"] == ["s3://bucket/a.jar", "s3://bucket/b.jar"]


class TestRunIdentifier:
    def test_run_identifier_contains_job_name(self):
        result = _run(module_name="mod", class_name="Cls")
        assert "mod.Cls" in result["run_identifier"]

    def test_run_identifier_is_unique(self):
        import re

        # Second-precision timestamp suffix ensures uniqueness across runs
        assert re.search(r"_\d{14}$", _run()["run_identifier"])


# ─── spark_execution_logic lazy-facade ────────────────────────────────────────


def test_execution_logic_resolves_known_symbol():
    import overture_airflow_provider.spark_execution_logic as sel

    assert sel.MAX_TIMEOUT_HOURS == 8  # re-exported from _glue


def test_execution_logic_raises_for_unknown_symbol():
    import overture_airflow_provider.spark_execution_logic as sel

    with pytest.raises(AttributeError):
        _ = sel.DOES_NOT_EXIST
