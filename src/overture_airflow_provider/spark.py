"""Spark platform/version enums and Sedona JAR coordinate helpers.

Pure-Python, zero Airflow / AWS dependencies. Safe to import from anywhere.
"""

from collections import namedtuple
from enum import Enum, IntEnum, auto

SparkPlatform = namedtuple(
    "SparkPlatform",
    ["family", "native_version", "spark_version", "scala_version", "python_version"],
)


class SparkFamily(IntEnum):
    GLUE = auto()
    DATABRICKS = auto()
    SYNAPSE = auto()
    WHEROBOTS = auto()


class SparkImpl(Enum):
    # We do not support Spark < 3.3 — too many ecosystem issues.
    GLUE_v4 = SparkPlatform(SparkFamily.GLUE, "4.0", "3.3.0", "2.12", "3.10")
    GLUE_v5 = SparkPlatform(SparkFamily.GLUE, "5.0", "3.5.2", "2.12", "3.11")
    DATABRICKS_v13 = SparkPlatform(
        SparkFamily.DATABRICKS, "13.3.x-scala2.12", "3.4.1", "2.12", "3.10.6"
    )
    DATABRICKS_v14 = SparkPlatform(
        SparkFamily.DATABRICKS, "14.3.x-scala2.12", "3.5.0", "2.12", "3.10.12"
    )
    DATABRICKS_v15 = SparkPlatform(
        SparkFamily.DATABRICKS, "15.4.x-scala2.12", "3.5.0", "2.12", "3.11.0"
    )
    SYNAPSE_v3_3_1 = SparkPlatform(SparkFamily.SYNAPSE, "?", "3.3.1", "2.12", "3.10")
    SYNAPSE_v3_4_1 = SparkPlatform(SparkFamily.SYNAPSE, "?", "3.4.1", "2.12", "3.10")
    WHEROBOTS_v1_5_0 = SparkPlatform(SparkFamily.WHEROBOTS, "1.5.0", "3.5.0", "2.12", "3.11")

    @classmethod
    def from_str(cls, name: str) -> "SparkImpl":
        if name in cls.__members__:
            return cls[name]
        raise ValueError(f"{name} is not a valid SparkImpl")

    def __str__(self) -> str:
        return (
            f"{self.name} - {self.get_native_version()} - "
            f"Spark {self.get_spark_version()} - "
            f"Scala {self.get_scala_version()} - "
            f"Python {self.get_python_version()}"
        )

    def get_family(self) -> SparkFamily:
        return self.value.family

    def get_native_version(self) -> str:
        return self.value.native_version

    def get_spark_version(self) -> str:
        return self.value.spark_version

    def get_scala_version(self) -> str:
        return self.value.scala_version

    def get_python_version(self) -> str:
        return self.value.python_version


class SparkSedona:
    """Sedona-specific version + Maven coordinate derivations."""

    @classmethod
    def getSparkVersionForSedona(cls, py_spark_version: str, sedona_version: str) -> str:
        # https://sedona.apache.org/latest-snapshot/setup/install-python/#prepare-sedona-spark-jar
        spark_major, spark_minor = py_spark_version.split(".")[:2]
        sedona_major, sedona_minor = sedona_version.split(".")[:2]
        if int(sedona_major) == 1 and int(sedona_minor) >= 8 and int(spark_minor) <= 3:
            raise RuntimeError(
                f"Sedona {sedona_version} dropped Spark 3.3 support. "
                "Use GLUE_v5 or another Spark 3.4+ implementation."
            )
        if spark_major != "3":
            raise RuntimeError("I'm only supporting spark 3")
        if int(spark_minor) <= 3 and int(sedona_major) == 1 and int(sedona_minor) <= 6:
            return f"{spark_major}.0"
        return f"{spark_major}.{spark_minor}"

    @classmethod
    def getGeotoolsWrapperVersion(cls, sedona_version: str) -> str:
        # https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/
        geotools_version_map = {
            "1.5.3": "28.2",
            "1.6.1": "28.2",
            "1.7.0": "28.5",
            "1.7.1": "28.5",
            "1.7.2": "28.5",
            "1.8.1": "33.1",
            "1.9.0": "33.5",
        }
        return geotools_version_map[sedona_version]

    @classmethod
    def getSedonaJarPackages(
        cls, sedona_version: str, py_spark_version: str, scala_version: str
    ) -> list[str]:
        spark_for_sedona = cls.getSparkVersionForSedona(
            py_spark_version=py_spark_version, sedona_version=sedona_version
        )
        geotools_version = cls.getGeotoolsWrapperVersion(sedona_version=sedona_version)
        return [
            f"org.apache.sedona:sedona-spark-shaded-{spark_for_sedona}_{scala_version}:{sedona_version}",
            f"org.datasyslab:geotools-wrapper:{sedona_version}-{geotools_version}",
        ]
