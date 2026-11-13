"""Multi-provider matching scaffold; parses without credentials or optional SDKs.

Each provider gets its own matching group: a Spark job matches the provider's
data against a baseline, the matching metadata is published, and every result
table is loaded by a child DAG run. Replace the source gate, the metadata
publisher and the source finalizer with pipeline tasks before running, and point
the Spark job and the load DAG at real ones.
"""

import json
from datetime import datetime

from overture_airflow_provider import (
    ArtifactStoreConfig,
    GlueConfig,
    PackageRegistryConfig,
    child_dag_run_task_group,
    matching_task_group,
    spark_agnostic_task_group,
)
from overture_airflow_provider._airflow_compat import (
    DAG,
    EmptyOperator,
    TaskGroup,
    TriggerRule,
    task,
)

TABLES = ("division", "division_area")

_MATCHER_JOB = dict(
    spark_impl_name="GLUE_v5",
    sedona_version="1.7.0",
    module_name="my_pkg.jobs",
    class_name="MatchingJob",
    python_packages="my-pkg==1.0.0",
    spark_cluster_desired_worker_cores="160",
    artifact_store=ArtifactStoreConfig(
        s3_bucket="example-bucket", s3_root="spark-agnostic-operator"
    ),
    package_registry=PackageRegistryConfig(
        domain="my-domain",
        domain_owner="123456789012",
        repository="my-pypi",
        region="us-east-1",
    ),
    glue_config=GlueConfig(iam_role_name="AWSGlueServiceRole"),
)


@task.short_circuit(ignore_downstream_trigger_rules=False)
def source_changed(provider):
    raise NotImplementedError("Replace with the pipeline's new-data check and ingest tasks")


@task
def matcher_parameters(input_paths, baseline_paths, output_path):
    return json.dumps(
        {"input": input_paths, "baseline": baseline_paths, "output_path": output_path}
    )


@task
def matcher_results(output_path):
    return {table: f"{output_path}/type={table}" for table in TABLES}


def matcher_job(output_path):
    """Return a matcher factory whose job writes one directory per table under ``output_path``."""

    def build(*, input_paths, baseline_paths):
        parameters = matcher_parameters(input_paths, baseline_paths, output_path)
        job = spark_agnostic_task_group(group_id="spark", parameters=parameters, **_MATCHER_JOB)
        results = matcher_results(output_path)
        job >> results
        return results

    return build


@task
def publish_matching_metadata(input_paths, baseline_paths, result_paths):
    raise NotImplementedError("Write the matching metadata next to the results")


@task
def load_requests(provider, result_paths):
    return [
        {"table": table, "path": path, "source": provider} for table, path in result_paths.items()
    ]


def load_tables(provider):
    """Return a sink factory that loads every result table with a child DAG run."""

    def build(*, result_paths):
        loads = child_dag_run_task_group(
            "load",
            trigger_dag_id="example_load_table",
            requests=load_requests(provider, result_paths),
        )
        return loads.run_ids

    return build


@task
def finalize_source(load_run_ids):
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
                    tables=TABLES,
                    input={
                        table: f"s3://example-bucket/{provider}/data/type={table}"
                        for table in TABLES
                    },
                    baseline={
                        table: f"s3://example-bucket/baseline/type={table}" for table in TABLES
                    },
                    matcher_task_factory=matcher_job(
                        f"s3://example-bucket/matching/{provider}/{{{{ run_id }}}}"
                    ),
                    provenance_task_factory=publish_matching_metadata,
                    sink_task_factory=load_tables(provider),
                )
                changed >> matching.group
                completed.append(finalize_source(matching.complete))
    ingest_done = EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)
    completed >> ingest_done
