import json
from collections import UserList
from datetime import datetime

import pytest

from overture_airflow_provider import child_dag_run_task_group, matching_task_group
from overture_airflow_provider._airflow_compat import (
    DAG,
    AirflowFailException,
    TaskGroup,
    TriggerRule,
    task,
)
from overture_airflow_provider.matching_taskgroup import (
    _complete,
    _paths,
    _result_paths,
    table_names,
)

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
        assert group.complete.operator.task_id == prefix + "complete"


def test_a_paths_factory_builds_its_tasks_inside_the_group():
    with new_dag("paths_factory") as dag:
        make_group(baseline=resolve_baseline)
    paths = dag.get_task("matching.baseline.paths")
    assert paths.upstream_task_ids == {"matching.baseline.resolve_baseline"}


def test_each_step_waits_for_every_task_of_the_step_before_it():
    @task
    def standalone(value):
        return value

    def step(task_id, value):
        # A second task the factory neither chains nor returns must still be waited for.
        def build(**paths):
            standalone.override(task_id=f"{task_id}_side")("side")
            return standalone.override(task_id=task_id)(value)

        return build

    with new_dag("matching_order") as dag:
        make_group(
            input=step("export", INPUTS),
            baseline=step("export", BASELINE),
            pre_match_task_factory=step("check", "c"),
            matcher_task_factory=step("match", RESULTS),
            provenance_task_factory=step("publish", "p"),
            sink_task_factory=step("store", "s"),
        )
        make_group("no_check", input=step("export", INPUTS))

    def upstream(task_id):
        return dag.get_task(task_id).upstream_task_ids

    def both(task_id):
        return {task_id, f"{task_id}_side"}

    sources = {
        f"matching.{name}.{task_id}"
        for name in ("input", "baseline")
        for task_id in ("paths", "export_side")
    }
    for name, before in (
        ("pre_match.check", sources),
        ("matcher.match", both("matching.pre_match.check")),
        ("provenance.publish", {"matching.result_paths"}),
        ("sink.store", both("matching.provenance.publish")),
    ):
        assert upstream(f"matching.{name}") == upstream(f"matching.{name}_side") == before
    assert upstream("matching.result_paths") == both("matching.matcher.match")
    # `start` is the only root, so a gate on the group reaches it whatever the factories built.
    group_tasks = {task_id for task_id in dag.task_ids if task_id.startswith("matching.")}
    assert {task_id for task_id in group_tasks if not upstream(task_id)} == {"matching.start"}
    assert dag.get_task("matching.start").downstream_task_ids == {"matching.complete"} | {
        f"matching.{name}.{task_id}"
        for name in ("input", "baseline")
        for task_id in ("export", "export_side")
    }
    # `complete` waits directly for `start` and for every leaf and returned output of
    # every step: all of the group but the two exports, which only feed a `paths` check.
    assert upstream("matching.complete") == group_tasks - {
        "matching.complete",
        "matching.input.export",
        "matching.baseline.export",
    }
    assert dag.get_task("matching.complete").downstream_task_ids == set()
    assert upstream("no_check.matcher.match") == {
        "no_check.input.paths",
        "no_check.input.export_side",
        "no_check.baseline.paths",
    }


def test_complete_forwards_the_last_configured_step():
    @task
    def tidy():
        return None

    def publish_then_tidy(**paths):
        published = publish(**paths)
        published >> tidy()
        return published

    with new_dag("matching_complete") as dag:
        results = {
            "result_paths": make_group("match_only"),
            "provenance.publish": make_group("published", provenance_task_factory=publish),
            "sink.store": make_group(
                "stored", provenance_task_factory=publish, sink_task_factory=store
            ),
        }
        make_group("tidied", provenance_task_factory=publish_then_tidy, sink_task_factory=store)
    for last_task, result in results.items():
        prefix = result.group.group_id
        complete = result.complete.operator
        assert complete.task_id == f"{prefix}.complete"
        # The forwarded value is the last step's output, read from that task's XCom.
        assert [operator.task_id for operator, _ in complete.op_args[0].iter_references()] == [
            f"{prefix}.{last_task}"
        ]
        assert {f"{prefix}.start", f"{prefix}.{last_task}"} <= complete.upstream_task_ids
    # The provenance output is not a leaf here, and the sink must still wait for it.
    assert dag.get_task("tidied.sink.store").upstream_task_ids == {
        "tidied.result_paths",
        "tidied.provenance.publish",
        "tidied.provenance.tidy",
    }


def test_complete_forwards_a_lazily_read_output_as_a_list():
    # A mapped step's output is a sequence that is not a list and cannot be pushed as it is.
    forwarded = _complete.function(UserList(["stored division", "stored division_area"]))
    assert type(forwarded) is list
    assert forwarded == ["stored division", "stored division_area"]
    for plain in ("stored", RESULTS, ["stored"], None):
        assert _complete.function(plain) is plain


def test_own_tasks_keep_the_default_trigger_rule_under_permissive_default_args():
    with DAG(
        "permissive_defaults",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        default_args={"trigger_rule": TriggerRule.NONE_FAILED},
    ) as dag:
        make_group(sink_task_factory=load_tables)
    rules = {operator.task_id: operator.trigger_rule for operator in dag.tasks}
    # A factory's task follows the DAG. The group's own tasks must not: with
    # ``none_failed`` they would run, and trigger child runs, behind a skipped gate.
    assert rules["matching.matcher.match"] == TriggerRule.NONE_FAILED
    for own in (
        "start",
        "input.paths",
        "baseline.paths",
        "result_paths",
        "complete",
        "sink.load.requests",
        "sink.load.trigger",
    ):
        assert rules[f"matching.{own}"] == TriggerRule.ALL_SUCCESS
    assert rules["matching.sink.load.run_ids"] == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


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
        # A task built outside the call would not wait for earlier steps or the group's gate.
        for argument, name in (
            ("matcher_task_factory", "matcher_task_factory"),
            ("provenance_task_factory", "provenance_task_factory"),
            ("sink_task_factory", "sink_task_factory"),
            ("pre_match_task_factory", "pre_match_task_factory"),
            ("input", "The input factory"),
            ("baseline", "The baseline factory"),
        ):
            with pytest.raises(ValueError, match=f"{name} must build its tasks"):
                make_group(**{argument: lambda **paths: prebuilt})
        # Passed directly, a task output built elsewhere is an input like any other.
        make_group("direct", input=prebuilt, baseline=prebuilt)
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
