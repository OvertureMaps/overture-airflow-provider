from datetime import datetime

import pytest

from overture_airflow_provider import (
    ArtifactStoreConfig,
    CorpusDagConfig,
    DatabricksMatchingConfig,
    GlueConfig,
    GlueMatchingConfig,
    PackageRegistryConfig,
    matching_task_group,
)
from overture_airflow_provider._airflow_compat import DAG, TaskGroup, task
from tests.test_matching import BASELINE, INPUTS, TABLES


@task
def publish(input_paths, baseline_paths, result_paths, metadata_root, parameters):
    return {}


@task
def validate(input_paths, baseline_paths, embeddings_cache_table):
    return True


def make_group(group_id="matching_tg", **kwargs):
    return matching_task_group(
        group_id=group_id,
        scenario="overture",
        theme="divisions",
        provider="linz",
        input={"override_paths": INPUTS},
        baseline={"override_paths": BASELINE},
        tables=TABLES,
        output_path="s3://caller/matches/linz/{{ run_id }}",
        run_id="{{ run_id }}",
        **{
            "runtime": DatabricksMatchingConfig("caller_connection", "caller_job"),
            **kwargs,
        },
    )


def test_nested_multiple_and_repeated_instances_have_local_edges_and_branches():
    corpus = CorpusDagConfig(
        "export", "load", "output", create_branch_dag_id="create", create_branch=True
    )
    with DAG("matching_isolation", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        with TaskGroup("providers"):
            groups = []
            for name in ("osm", "linz", "dados_abertos", "geoboundaries"):
                with TaskGroup(name):
                    for _ in range(2):
                        groups.append(
                            make_group(
                                group_id="custom_match",
                                corpus=corpus,
                                output_corpus_branch="main",
                                provenance_task_factory=publish,
                                validation_task_factory=validate,
                                embeddings_cache_table="caller.embeddings",
                            )
                        )
    assert len({task.task_id for task in dag.tasks}) == len(dag.tasks)
    assert groups[1].group.group_id == "providers.osm.custom_match__1"
    for group in groups:
        prefix = group.group.group_id + "."
        for operator in group.group.iter_tasks():
            assert all(task_id.startswith(prefix) for task_id in operator.upstream_task_ids)
            if operator.task_id.endswith("._choose_selection"):
                for task_id in operator.op_args[1:]:
                    assert task_id.startswith(prefix)
                    assert task_id in dag.task_ids
            if operator.task_id.endswith("._choose_load"):
                assert all(task_id.startswith(prefix) for task_id in operator.op_args[1:])
        loaded = dag.get_task(prefix + "load_results._loaded")
        assert loaded.trigger_rule == "none_failed_min_one_success"
        assert prefix + "load_results.load" in loaded.upstream_task_ids
        assert group.complete.operator.task_id == prefix + "load_results._complete"


def test_match_only_without_corpus_and_non_overture_branch_compatibility():
    with DAG("match_only", schedule=None, start_date=datetime(2026, 1, 1)):
        result = make_group()
    assert result.complete.operator.task_id == "matching_tg.load_results._match_only"
    assert not any(task.task_type == "TriggerDagRunOperator" for task in result.group.iter_tasks())


def test_loading_and_embeddings_require_explicit_factories():
    with DAG("bad_matching", schedule=None, start_date=datetime(2026, 1, 1)):
        with pytest.raises(ValueError, match="provenance"):
            make_group(
                output_corpus_branch="main", corpus=CorpusDagConfig("export", "load", "output")
            )
        with pytest.raises(ValueError, match="validation"):
            make_group(embeddings_cache_table="catalog.table")
        with pytest.raises(ValueError, match="supports"):
            make_group(runtime="WHEROBOTS_v1_5_0")


def test_provenance_is_a_required_dependency_before_loading():
    with DAG("provenance_barrier", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        result = make_group(
            corpus=CorpusDagConfig("export", "load", "output"),
            output_corpus_branch="{{ params.branch }}",
            provenance_task_factory=publish,
        )
    ready = dag.get_task("matching_tg.provenance._provenance_ready")
    load_plan = dag.get_task("matching_tg.load_results._load_plan")
    assert ready.task_id in {task.task_id for task in load_plan.get_flat_relatives(upstream=True)}
    assert result.complete.operator.trigger_rule == "none_failed_min_one_success"


def test_real_dag_serialization_has_no_pipeline_dependency():
    from airflow.serialization.serialized_objects import DagSerialization

    with DAG("serialized_matching", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        make_group(
            corpus=CorpusDagConfig(
                "export", "load", "output", create_branch_dag_id="create", create_branch=True
            ),
            output_corpus_branch="main",
            provenance_task_factory=publish,
        )
    serialized = DagSerialization.to_dict(dag)
    assert serialized
    restored = DagSerialization.from_dict(serialized)
    assert set(restored.task_ids) == set(dag.task_ids)


def test_glue_adapter_forwards_injected_config_and_waits_for_execution(mocker):
    from overture_airflow_provider import _matching_glue

    registry = PackageRegistryConfig("caller", "123456789012", "packages")
    store = ArtifactStoreConfig(s3_bucket="caller-artifacts")
    glue = GlueConfig(iam_role_name="caller-role")
    build = mocker.spy(_matching_glue, "spark_agnostic_task_group")
    runtime = GlueMatchingConfig(
        spark_jar_paths="s3://caller/matcher.jar",
        sedona_version="1.7.0",
        package_registry=registry,
        artifact_store=store,
        glue_config=glue,
        extra_files="s3://caller/libpostal.tar.gz",
        extra_spark_conf='{"spark.example": "value"}',
        pool="caller_matching",
        scala_download_pool="caller_download",
    )
    with DAG("glue_matching", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        make_group(runtime=runtime)
    kwargs = build.call_args.kwargs
    assert kwargs["package_registry"] is registry
    assert kwargs["artifact_store"] is store
    assert kwargs["glue_config"] is glue
    assert kwargs["extra_spark_conf"] == runtime.extra_spark_conf
    assert kwargs["class_name"] == "org.overturemaps.matching.Main"
    prefix = "matching_tg.matcher."
    assert dag.get_task(prefix + "spark.execute_spark_job").pool == "caller_matching"
    assert dag.get_task(prefix + "spark.download_jars").pool == "caller_download"
    assert (
        prefix + "spark.execute_spark_job"
        in dag.get_task(prefix + "_read_manifest").upstream_task_ids
    )
    assert dag.get_task(prefix + "_glue_parameters").op_args[2] == runtime.extra_files
