"""Multi-provider matching scaffold; parses without credentials or optional SDKs.

Replace the source gate, provenance publisher and source finalizer with pipeline
tasks before running. Configure the matcher JAR, Spark settings and Corpus DAGs
for the deployment. No discovery, cloud calls or metadata writes happen at DAG
parse.
"""

from datetime import datetime

from overture_airflow_provider import (
    ArtifactStoreConfig,
    CorpusDagConfig,
    GlueConfig,
    PackageRegistryConfig,
    matching_task_group,
    spark_matcher_task_factory,
)
from overture_airflow_provider._airflow_compat import (
    DAG,
    EmptyOperator,
    TaskGroup,
    TriggerRule,
    task,
)

# Forwarded unchanged to the Spark task group that runs the matcher.
_MATCHER_JOB = dict(
    spark_impl_name="GLUE_v5",
    sedona_version="1.7.0",
    spark_jar_paths="s3://example-bucket/jars/matching-shaded.jar",
    spark_cluster_desired_worker_cores="160",
    artifact_store=ArtifactStoreConfig(
        s3_bucket="example-bucket", s3_root="spark-agnostic-operator"
    ),
    package_registry=PackageRegistryConfig(
        domain="my-domain",
        domain_owner="123456789012",
        repository="my-pypi",
        region="us-east-1",
        maven_repository="my-maven",
        maven_repository_path="maven/my-maven",
    ),
    glue_config=GlueConfig(iam_role_name="AWSGlueServiceRole"),
)

_CORPUS = CorpusDagConfig(
    export_dag_id="example_corpus_export",
    load_dag_id="example_corpus_load",
    export_output_task_id="output",
)


@task.short_circuit(ignore_downstream_trigger_rules=False)
def source_changed(provider):
    raise NotImplementedError("Replace with the pipeline's new-data check and ingest tasks")


@task
def publish_matching_metadata(input_paths, baseline_paths, result_paths, metadata_root, parameters):
    raise NotImplementedError("Write the matching metadata at metadata_root, then return it")


@task
def finalize_source(completion):
    if not completion["loaded"]:
        raise ValueError("Source finalization requires completed Corpus loads")
    raise NotImplementedError("Replace with the pipeline's source completion marker")


with DAG(
    dag_id="example_per_provider_matching",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["example", "matching"],
) as dag:
    completed = []
    with TaskGroup("providers"):
        for provider in ("osm", "linz"):
            with TaskGroup(provider):
                changed = source_changed(provider)
                matching = matching_task_group(
                    group_id="match",
                    scenario="overture",
                    theme="divisions",
                    provider=provider,
                    input={
                        "override_paths": {
                            table: f"s3://example-bucket/{provider}/data/type={table}"
                            for table in ("division", "division_area")
                        }
                    },
                    baseline={"branch": "main"},
                    tables=("division", "division_area"),
                    output_path=f"s3://example-bucket/matching/{provider}/{{{{ run_id }}}}",
                    run_id="{{ run_id }}",
                    matcher_env="dev",
                    matcher_task_factory=spark_matcher_task_factory(**_MATCHER_JOB),
                    corpus=_CORPUS,
                    output_corpus_branch="main",
                    provenance_task_factory=publish_matching_metadata,
                )
                changed >> matching.group
                completed.append(finalize_source(matching.complete))
    ingest_done = EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)
    completed >> ingest_done
