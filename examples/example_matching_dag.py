"""Multi-provider matching scaffold; parses without credentials or optional SDKs.

Replace the source gate, provenance publisher and source finalizer with pipeline
tasks before running. Configure the existing matcher job and Corpus DAGs in the
deployment. No discovery, cloud calls or metadata writes happen at DAG parse.
"""

from datetime import datetime

from airflow.providers.standard.operators.empty import EmptyOperator

from overture_airflow_provider import (
    CorpusDagConfig,
    DatabricksMatchingConfig,
    matching_task_group,
)
from overture_airflow_provider._airflow_compat import DAG, TaskGroup, TriggerRule, task


@task.short_circuit(ignore_downstream_trigger_rules=False)
def source_changed(provider):
    raise NotImplementedError("Replace with the pipeline's new-data check and ingest tasks")


@task
def publish_matching_metadata(input_paths, baseline_paths, result_paths, metadata_root, parameters):
    raise NotImplementedError(
        "Publish complete metadata, then return the provenance receipt documented in README.md"
    )


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
                    runtime=DatabricksMatchingConfig(
                        databricks_conn_id="example_databricks",
                        job_name="example_matching",
                    ),
                    corpus=CorpusDagConfig(
                        export_dag_id="example_corpus_export",
                        load_dag_id="example_corpus_load",
                        export_output_task_id="output",
                    ),
                    output_corpus_branch="main",
                    provenance_task_factory=publish_matching_metadata,
                )
                changed >> matching.group
                completed.append(finalize_source(matching.complete))
    ingest_done = EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)
    completed >> ingest_done
