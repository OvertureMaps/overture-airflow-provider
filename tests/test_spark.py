"""Tests for SparkImpl, SparkFamily, and SparkSedona."""

import pytest

from overture_airflow_provider.spark import SparkFamily, SparkImpl, SparkSedona


class TestSparkImplFromStr:
    def test_known_values_round_trip(self):
        for name in SparkImpl.__members__:
            assert SparkImpl.from_str(name).name == name

    def test_unknown_raises(self):
        with pytest.raises(ValueError, match="not a valid SparkImpl"):
            SparkImpl.from_str("GLUE_v99")

    def test_case_sensitive(self):
        with pytest.raises(ValueError):
            SparkImpl.from_str("glue_v5")


class TestSparkImplMetadata:
    def test_glue_v5_family(self):
        assert SparkImpl.GLUE_v5.get_family() == SparkFamily.GLUE

    def test_glue_v4_family(self):
        assert SparkImpl.GLUE_v4.get_family() == SparkFamily.GLUE

    def test_databricks_v15_family(self):
        assert SparkImpl.DATABRICKS_v15.get_family() == SparkFamily.DATABRICKS

    def test_wherobots_family(self):
        assert SparkImpl.WHEROBOTS_v1_5_0.get_family() == SparkFamily.WHEROBOTS

    @pytest.mark.parametrize(
        "impl, expected_spark",
        [
            (SparkImpl.GLUE_v4, "3.3.0"),
            (SparkImpl.GLUE_v5, "3.5.2"),
            (SparkImpl.DATABRICKS_v13, "3.4.1"),
            (SparkImpl.DATABRICKS_v14, "3.5.0"),
            (SparkImpl.DATABRICKS_v15, "3.5.0"),
            (SparkImpl.WHEROBOTS_v1_5_0, "3.5.0"),
        ],
    )
    def test_spark_version(self, impl, expected_spark):
        assert impl.get_spark_version() == expected_spark

    @pytest.mark.parametrize(
        "impl, expected_python",
        [
            (SparkImpl.GLUE_v4, "3.10"),
            (SparkImpl.GLUE_v5, "3.11"),
            (SparkImpl.DATABRICKS_v15, "3.11.0"),
            (SparkImpl.WHEROBOTS_v1_5_0, "3.11"),
        ],
    )
    def test_python_version(self, impl, expected_python):
        assert impl.get_python_version() == expected_python

    def test_all_impls_have_scala_2_12(self):
        for impl in SparkImpl:
            assert impl.get_scala_version() == "2.12", (
                f"{impl.name} has unexpected scala version {impl.get_scala_version()}"
            )


class TestSparkSedonaVersionForSedona:
    @pytest.mark.parametrize(
        "spark_v, sedona_v, expected",
        [
            ("3.3.0", "1.5.3", "3.0"),
            ("3.3.0", "1.6.1", "3.0"),
            ("3.3.0", "1.7.0", "3.3"),
            ("3.4.1", "1.6.1", "3.4"),
            ("3.4.1", "1.7.0", "3.4"),
            ("3.5.0", "1.7.0", "3.5"),
            ("3.5.2", "1.7.1", "3.5"),
        ],
    )
    def test_spark_version_for_sedona(self, spark_v, sedona_v, expected):
        result = SparkSedona.getSparkVersionForSedona(spark_v, sedona_v)
        assert result == expected

    def test_non_spark3_raises(self):
        with pytest.raises(RuntimeError, match="only supporting spark 3"):
            SparkSedona.getSparkVersionForSedona("2.4.0", "1.7.0")


class TestSparkSedonaGeotoolsVersion:
    @pytest.mark.parametrize(
        "sedona_v, expected_geotools",
        [
            ("1.5.3", "28.2"),
            ("1.6.1", "28.2"),
            ("1.7.0", "28.5"),
            ("1.7.1", "28.5"),
            ("1.7.2", "28.5"),
        ],
    )
    def test_known_versions(self, sedona_v, expected_geotools):
        assert SparkSedona.getGeotoolsWrapperVersion(sedona_v) == expected_geotools

    def test_unknown_version_raises(self):
        with pytest.raises(KeyError):
            SparkSedona.getGeotoolsWrapperVersion("9.9.9")


class TestSparkSedonaJarPackages:
    def test_package_count(self):
        pkgs = SparkSedona.getSedonaJarPackages("1.7.0", "3.5.2", "2.12")
        assert len(pkgs) == 2

    def test_sedona_jar_coordinate(self):
        pkgs = SparkSedona.getSedonaJarPackages("1.7.0", "3.5.2", "2.12")
        sedona_pkg = next(p for p in pkgs if "sedona" in p)
        assert "sedona-spark-shaded-3.5_2.12:1.7.0" in sedona_pkg

    def test_geotools_jar_coordinate(self):
        pkgs = SparkSedona.getSedonaJarPackages("1.7.0", "3.5.2", "2.12")
        geotools_pkg = next(p for p in pkgs if "geotools" in p)
        assert "geotools-wrapper" in geotools_pkg
        assert "1.7.0-28.5" in geotools_pkg

    def test_old_sedona_uses_compressed_spark_version(self):
        pkgs = SparkSedona.getSedonaJarPackages("1.6.1", "3.3.0", "2.12")
        sedona_pkg = next(p for p in pkgs if "sedona" in p)
        assert "3.0_2.12" in sedona_pkg
