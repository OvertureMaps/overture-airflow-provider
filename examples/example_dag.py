"""Example DAG that targets all three supported Spark engines.

Each task group runs the same job on a different engine. In real use you would
pick one engine per DAG; this file is just for documentation.
"""

import json
from datetime import datetime

from overture_airflow_provider import (
    ArtifactStoreConfig,
    DatabricksConfig,
    GlueConfig,
    IcebergConfig,
    PackageRegistryConfig,
    WherobotsConfig,
    spark_agnostic_task_group,
)
from overture_airflow_provider._airflow_compat import DAG

_ARTIFACT_STORE = ArtifactStoreConfig(
    s3_bucket="example-bucket",
    s3_root="spark-agnostic-operator",
)

_PACKAGE_REGISTRY = PackageRegistryConfig(
    domain="my-domain",
    domain_owner="123456789012",
    repository="my-pypi",
    region="us-east-1",
    maven_repository="my-maven",
    maven_repository_path="maven/my-maven",
)

# IcebergConfig holds per-platform Spark config JSON strings.
# spark_config: used by Glue and Databricks (REST catalog).
# wherobots_spark_config: used by Wherobots (GlueCatalog cross-account).
_ICEBERG = IcebergConfig(
    spark_config=json.dumps(
        {
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg_catalog.catalog-impl": (
                "org.apache.iceberg.aws.glue.GlueCatalog"
            ),
            "spark.sql.catalog.iceberg_catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
            "spark.sql.defaultCatalog": "iceberg_catalog",
        }
    ),
)

_JOB_KWARGS = dict(
    sedona_version="1.7.0",
    module_name="my_pkg.jobs",
    class_name="MyJob",
    python_packages="my-pkg==1.0.0",
    parameters=json.dumps(
        {
            "s3_input": "s3://example-bucket/in/",
            "s3_output": "s3://example-bucket/out/",
        }
    ),
    artifact_store=_ARTIFACT_STORE,
    package_registry=_PACKAGE_REGISTRY,
    iceberg_config=_ICEBERG,
)


with DAG(
    dag_id="example_spark_agnostic_all_platforms",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "spark-agnostic"],
) as dag:
    spark_agnostic_task_group(
        group_id="glue_job",
        spark_impl_name="GLUE_v5",
        spark_cluster_desired_worker_cores="160",  # ~M: 20 × G.2X (8 vCPU each)
        glue_config=GlueConfig(iam_role_name="AWSGlueServiceRole"),
        **_JOB_KWARGS,
    )

    spark_agnostic_task_group(
        group_id="databricks_job",
        spark_impl_name="DATABRICKS_v15",
        spark_cluster_desired_worker_cores="160",
        databricks_config=DatabricksConfig(
            cluster_conf={"databricks_conn_id": "databricks_default"},
            dbfs_root_template="dbfs:/FileStore/deploy/{s3_assets_root}",
            workspace_scripts_path_template="/Workspace/Shared/{s3_assets_root}",
            cluster_init_script_name="agnostic_operator_cluster_init_databricks.sh",
        ),
        **_JOB_KWARGS,
    )

    spark_agnostic_task_group(
        group_id="wherobots_job",
        spark_impl_name="WHEROBOTS_v1_5_0",
        spark_cluster_desired_worker_cores="160",
        wherobots_config=WherobotsConfig(
            role_arn="arn:aws:iam::123456789012:role/wherobots-access",
            external_id="example-external-id",
            aws_region="us-west-2",
        ),
        **_JOB_KWARGS,
    )
