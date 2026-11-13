from datetime import datetime

import pytest

from overture_airflow_provider import (
    ArtifactStoreConfig,
    CorpusDagConfig,
    GlueConfig,
    PackageRegistryConfig,
    matching_task_group,
    spark_agnostic_task_group,
    spark_matcher_task_factory,
)
from overture_airflow_provider._airflow_compat import DAG, TaskGroup, task
from tests.test_matching import BASELINE, INPUTS, TABLES


@task
def match(parameters):
    return {}


@task
def publish(input_paths, baseline_paths, result_paths, metadata_root, parameters):
    return metadata_root


@task
def check(input_paths, baseline_paths):
    return True


def make_group(group_id="matching_tg", **kwargs):
    defaults = {
        "scenario": "overture",
        "theme": "divisions",
        "provider": "linz",
        "input": {"override_paths": INPUTS},
        "baseline": {"override_paths": BASELINE},
        "tables": TABLES,
        "output_path": "s3://caller/matches/linz/{{ run_id }}",
        "run_id": "{{ run_id }}",
        "matcher_env": "test",
        "matcher_task_factory": match,
    }
    return matching_task_group(group_id=group_id, **{**defaults, **kwargs})


def corpus(**kwargs):
    return CorpusDagConfig(
        "export", "load", "output", create_branch_dag_id="create", create_branch=True, **kwargs
    )


def test_nested_multiple_and_repeated_instances_have_local_edges():
    with DAG("matching_isolation", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        with TaskGroup("providers"):
            groups = []
            for name in ("osm", "linz", "dados_abertos", "geoboundaries"):
                with TaskGroup(name):
                    for _ in range(2):
                        groups.append(
                            make_group(
                                group_id="custom_match",
                                corpus=corpus(),
                                output_corpus_branch="main",
                                provenance_task_factory=publish,
                                pre_match_task_factory=check,
                            )
                        )
    assert len({task.task_id for task in dag.tasks}) == len(dag.tasks)
    assert groups[1].group.group_id == "providers.osm.custom_match__1"
    for group in groups:
        prefix = group.group.group_id + "."
        for operator in group.group.iter_tasks():
            assert all(task_id.startswith(prefix) for task_id in operator.upstream_task_ids)
        # Child run IDs are looked up by task ID, so these must be this instance's own.
        for selection in ("input", "baseline"):
            exported = dag.get_task(f"{prefix}{selection}._exported_paths")
            assert exported.op_args[0] == f"{prefix}{selection}.export"
        complete = group.complete.operator
        assert complete.task_id == prefix + "load_results._complete"
        assert complete.op_args[2] == prefix + "load_results.load"
        assert group.matcher.group_id == prefix + "matcher"


def test_match_only_without_corpus_triggers_no_child_runs():
    with DAG("match_only", schedule=None, start_date=datetime(2026, 1, 1)):
        result = make_group()
    assert result.complete.operator.task_id == "matching_tg.load_results._match_only"
    assert not any(task.task_type == "ChildDagRunOperator" for task in result.group.iter_tasks())


def test_loading_and_factories_are_validated_at_dag_construction():
    with DAG("bad_matching", schedule=None, start_date=datetime(2026, 1, 1)):
        # Any Jinja form can render to "overture", not only an expression.
        for scenario in (
            "overture",
            "{{ params.scenario }}",
            "{% if params.live %}overture{% else %}test{% endif %}",
        ):
            with pytest.raises(ValueError, match="provenance"):
                make_group(scenario=scenario, output_corpus_branch="main", corpus=corpus())
        with pytest.raises(TypeError, match="matcher_task_factory"):
            make_group(matcher_task_factory=lambda *, parameters: {"division": "s3://x"})


def test_matcher_waits_for_parameters_even_when_its_factory_ignores_them():
    @task
    def preconfigured_match():
        return {}

    with DAG("matcher_order", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        make_group(
            matcher_task_factory=lambda *, parameters: preconfigured_match(),
            pre_match_task_factory=check,
        )
    matcher = dag.get_task("matching_tg.matcher.preconfigured_match")
    assert "matching_tg._build_parameters" in matcher.upstream_task_ids
    assert "matching_tg.pre_match.check" in matcher.get_flat_relative_ids(upstream=True)


def test_provenance_is_a_required_dependency_before_loading():
    with DAG("provenance_barrier", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        result = make_group(
            corpus=corpus(),
            output_corpus_branch="{{ params.branch }}",
            provenance_task_factory=publish,
        )
    for name in ("_load_requests", "_create_requests"):
        upstream = dag.get_task("matching_tg.load_results." + name).get_flat_relative_ids(
            upstream=True
        )
        assert "matching_tg.provenance._provenance_ready" in upstream
    assert result.complete.operator.trigger_rule == "none_failed_min_one_success"


def test_child_triggers_rerun_the_same_run_and_keep_their_conf_untemplated():
    with DAG("child_triggers", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        make_group(
            baseline={"branch": "main"},
            corpus=corpus(export_pool="caller_serial"),
            output_corpus_branch="main",
            provenance_task_factory=publish,
        )
    for name in ("baseline.export", "load_results.create_branch", "load_results.load"):
        trigger = dag.get_task("matching_tg." + name)
        # Clearing a trigger must rerun its own child run, not fail on the existing run ID.
        assert trigger.partial_kwargs["reset_dag_run"] is True
        # Native rendering would turn a version such as "123" back into an int.
        assert "conf" not in trigger.template_fields
    export = dag.get_task("matching_tg.baseline.export")
    assert export.partial_kwargs["pool"] == "caller_serial"
    assert export.partial_kwargs["max_active_tis_per_dagrun"] == 1


def test_real_dag_serialization_has_no_pipeline_dependency():
    from airflow.serialization.serialized_objects import DagSerialization

    with DAG("serialized_matching", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        make_group(
            baseline={"branch": "main"},
            corpus=corpus(),
            output_corpus_branch="main",
            provenance_task_factory=publish,
        )
    serialized = DagSerialization.to_dict(dag)
    assert serialized
    restored = DagSerialization.from_dict(serialized)
    assert set(restored.task_ids) == set(dag.task_ids)


def test_spark_matcher_runs_through_the_callers_spark_factory():
    registry = PackageRegistryConfig("caller", "123456789012", "packages")
    store = ArtifactStoreConfig(s3_bucket="caller-artifacts")
    glue = GlueConfig(iam_role_name="caller-role")
    calls = []

    def caller_spark_factory(**kwargs):
        calls.append(kwargs)
        return spark_agnostic_task_group(
            package_registry=registry, artifact_store=store, glue_config=glue, **kwargs
        )

    @task
    def signed_jar_url():
        return "https://caller/matcher.jar"

    with DAG("spark_matching", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        jar = signed_jar_url()
        result = make_group(
            matcher_task_factory=spark_matcher_task_factory(
                caller_spark_factory,
                extra_files="s3://caller/libpostal.tar.gz",
                spark_impl_name="GLUE_v5",
                sedona_version="1.7.2",
                spark_jar_paths=jar,
                pool="caller_matching",
                retries=1,
            )
        )
    (kwargs,) = calls
    assert kwargs["group_id"] == "spark"
    assert kwargs["class_name"] == "org.overturemaps.matching.Main"
    assert kwargs["spark_jar_paths"] is jar
    prefix = result.matcher.group_id + "."
    execute = dag.get_task(prefix + "spark.execute_spark_job")
    assert (execute.pool, execute.retries) == ("caller_matching", 1)
    assert execute.task_id in dag.get_task(prefix + "_read_manifest").upstream_task_ids
    # A jar URL minted by a task must be an upstream of the job that downloads it.
    downloads = dag.get_task(prefix + "spark.download_jars")
    assert "signed_jar_url" in downloads.get_flat_relative_ids(upstream=True)
    assert dag.get_task(prefix + "_matcher_arguments").op_args[2] == "s3://caller/libpostal.tar.gz"
