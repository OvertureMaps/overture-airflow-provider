import json
from datetime import datetime

import pytest

from overture_airflow_provider import child_dag_run_task_group, matching_task_group
from overture_airflow_provider._airflow_compat import DAG, AirflowFailException, TaskGroup, task
from overture_airflow_provider.matching_taskgroup import _paths, _result_paths, table_names

TABLES = ["division", "division_area"]
INPUTS = {table: f"s3://feeds/provider=linz/data/type={table}" for table in TABLES}
BASELINE = {table: f"s3://baseline/type={table}" for table in TABLES}
RESULTS = {table: f"s3://matches/linz/type={table}" for table in TABLES}


@task
def match(input_paths, baseline_paths):
    return RESULTS


@task
def check(input_paths, baseline_paths):
    return True


@task
def publish(input_paths, baseline_paths, result_paths):
    return "published"


@task
def store(result_paths):
    return "stored"


@task
def resolve_baseline():
    return BASELINE


@task
def load_requests(result_paths):
    return [{"table": table, "path": path} for table, path in result_paths.items()]


def load_tables(*, result_paths):
    loads = child_dag_run_task_group(
        "load", trigger_dag_id="load_table", requests=load_requests(result_paths)
    )
    return loads.run_ids


def make_group(group_id="matching", **kwargs):
    defaults = {
        "tables": TABLES,
        "input": INPUTS,
        "baseline": BASELINE,
        "matcher_task_factory": match,
    }
    return matching_task_group(group_id, **{**defaults, **kwargs})


def new_dag(dag_id):
    return DAG(dag_id, schedule=None, start_date=datetime(2026, 1, 1))


def test_nested_and_repeated_instances_are_isolated():
    with new_dag("matching_isolation") as dag:
        with TaskGroup("providers"):
            groups = []
            for name in ("osm", "linz"):
                with TaskGroup(name):
                    for _ in range(2):
                        groups.append(
                            make_group(
                                group_id="custom_match",
                                baseline=resolve_baseline,
                                pre_match_task_factory=check,
                                provenance_task_factory=publish,
                                sink_task_factory=load_tables,
                            )
                        )
    assert len({task.task_id for task in dag.tasks}) == len(dag.tasks)
    assert groups[1].group.group_id == "providers.osm.custom_match__1"
    for group in groups:
        prefix = group.group.group_id + "."
        for operator in group.group.iter_tasks():
            assert all(task_id.startswith(prefix) for task_id in operator.upstream_task_ids)
        assert group.matcher.group_id == prefix + "matcher"
        assert group.result_paths.operator.task_id == prefix + "result_paths"
        assert group.complete.operator.task_id == prefix + "sink.load.run_ids"


def test_a_paths_factory_builds_its_tasks_inside_the_group():
    with new_dag("paths_factory") as dag:
        make_group(baseline=resolve_baseline)
    paths = dag.get_task("matching.baseline.paths")
    assert paths.upstream_task_ids == {"matching.baseline.resolve_baseline"}


def test_each_step_waits_for_the_steps_before_it_even_when_it_ignores_them():
    @task
    def standalone(name):
        return name

    def step(task_id, value):
        # A second task the factory neither chains nor returns must still be waited for.
        def build(**paths):
            standalone.override(task_id=f"{task_id}_side")("side")
            return standalone.override(task_id=task_id)(value)

        return build

    with new_dag("matching_order") as dag:
        make_group(
            pre_match_task_factory=step("check", "c"),
            matcher_task_factory=step("match", RESULTS),
            provenance_task_factory=step("publish", "p"),
            sink_task_factory=step("store", "s"),
        )

    def upstream(task_id):
        return dag.get_task(task_id).upstream_task_ids

    def both(task_id):
        return {task_id, f"{task_id}_side"}

    inputs = {"matching.input.paths", "matching.baseline.paths"}
    for name, before in (
        ("pre_match.check", inputs),
        ("matcher.match", inputs | both("matching.pre_match.check")),
        ("provenance.publish", {"matching.result_paths"}),
        ("sink.store", both("matching.provenance.publish")),
    ):
        assert upstream(f"matching.{name}") == upstream(f"matching.{name}_side") == before
    assert upstream("matching.result_paths") == both("matching.matcher.match")
    assert dag.get_task("matching.sink.store").downstream_task_ids == set()


def test_complete_is_the_last_configured_step():
    with new_dag("matching_complete"):
        match_only = make_group("match_only")
        published = make_group("published", provenance_task_factory=publish)
        stored = make_group("stored", provenance_task_factory=publish, sink_task_factory=store)
    assert match_only.complete is match_only.result_paths
    assert published.complete.operator.task_id == "published.provenance.publish"
    assert stored.complete.operator.task_id == "stored.sink.store"


def test_factories_and_tables_are_validated_at_dag_construction():
    with new_dag("bad_matching"):
        prebuilt = match(INPUTS, BASELINE)
        for factory in (
            "matcher_task_factory",
            "provenance_task_factory",
            "sink_task_factory",
            "baseline",
        ):
            with pytest.raises(TypeError, match="must return a task output"):
                make_group(**{factory: lambda **paths: RESULTS})
        # A task built outside the call would not wait for the steps before it.
        for factory in (
            "matcher_task_factory",
            "provenance_task_factory",
            "sink_task_factory",
            "pre_match_task_factory",
        ):
            with pytest.raises(ValueError, match=f"{factory} must build its tasks"):
                make_group(**{factory: lambda **paths: prebuilt})
        for tables in ("", [], ["division", "division"], [" "]):
            with pytest.raises(ValueError, match="tables"):
                make_group(tables=tables)


def test_path_maps_must_cover_exactly_the_tables():
    assert table_names("division, division_area") == TABLES
    assert _paths.function(json.dumps(INPUTS), TABLES, "input paths") == INPUTS
    assert _result_paths.function(RESULTS, TABLES) == RESULTS
    for bad in (
        {},
        {"division": INPUTS["division"]},
        {**INPUTS, "place": "s3://other"},
        {**INPUTS, "division": " "},
        [],
        None,
    ):
        # Contract violations are permanent: a retry cannot change the outcome.
        with pytest.raises(AirflowFailException, match="input paths"):
            _paths.function(bad, TABLES, "input paths")
    with pytest.raises(AirflowFailException, match="matching results"):
        _result_paths.function({"division": RESULTS["division"]}, TABLES)


def test_dag_serialization_round_trips():
    from airflow.serialization.serialized_objects import DagSerialization

    with new_dag("serialized_matching") as dag:
        make_group(provenance_task_factory=publish, sink_task_factory=load_tables)
    restored = DagSerialization.from_dict(DagSerialization.to_dict(dag))
    assert set(restored.task_ids) == set(dag.task_ids)
