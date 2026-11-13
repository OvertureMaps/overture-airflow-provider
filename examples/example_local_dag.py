"""Example DAG that runs a job on a local Spark install (development only).

``LOCAL_v3_5`` refuses to run unless the Airflow worker sets
``OVERTURE_AIRFLOW_PROVIDER_ALLOW_LOCAL_SPARK=1``, so this DAG parses
everywhere but fails at ``setup`` in staging and production.

Before triggering it on a dev worker:

* Replace the placeholder job and interpreter, and install PySpark 3.5,
  ``apache-sedona``, and the job's package in that interpreter.
* Export ``OVERTURE_AIRFLOW_PROVIDER_ALLOW_LOCAL_SPARK=1`` for the worker.

``package_registry`` and ``artifact_store`` are passed so the call matches
the cloud examples, but LOCAL never contacts CodeArtifact or S3 with them.
"""

import json
from datetime import datetime

from overture_airflow_provider import (
    ArtifactStoreConfig,
    LocalSparkConfig,
    PackageRegistryConfig,
    spark_agnostic_task_group,
)
from overture_airflow_provider._airflow_compat import DAG

with DAG(
    dag_id="example_spark_agnostic_local",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "spark-agnostic", "local"],
) as dag:
    spark_agnostic_task_group(
        group_id="local_job",
        spark_impl_name="LOCAL_v3_5",
        sedona_version="1.7.0",
        module_name="my_pkg.jobs",
        class_name="MyJob",
        python_packages="my-pkg==1.0.0",
        parameters=json.dumps(
            {
                "input": "/tmp/spark-agnostic-example/in/",
                "output": "/tmp/spark-agnostic-example/out/",
            }
        ),
        artifact_store=ArtifactStoreConfig(
            s3_bucket="example-bucket",
            s3_root="spark-agnostic-operator",
        ),
        package_registry=PackageRegistryConfig(
            domain="my-domain",
            domain_owner="123456789012",
            repository="my-pypi",
            region="us-east-1",
            maven_repository="my-maven",
            maven_repository_path="maven/my-maven",
        ),
        local_spark_config=LocalSparkConfig(
            master="local[*]",  # or "spark://host:7077", or "sc://host:15002" (PySpark only)
            python_executable="/opt/venvs/spark-dev/bin/python",
            driver_memory="8g",
            # aws_profile="my-dev-profile",  # for s3:// / s3a:// paths
            # Keep Iceberg writes on the laptop instead of the shared catalog.
            iceberg_spark_config={
                "spark.sql.catalog.local": "org.apache.iceberg.spark.SparkCatalog",
                "spark.sql.catalog.local.type": "hadoop",
                "spark.sql.catalog.local.warehouse": "/tmp/spark-agnostic-example/warehouse",
            },
            extra_packages=["org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"],
        ),
    )
