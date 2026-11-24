import json
from datetime import datetime

import pytest

from overture_airflow_provider import (
    child_dag_run_task_group,
    matching_task_group,
    spark_agnostic_task_group,
)
from overture_airflow_provider._airflow_compat import (
    DAG,
    AirflowFailException,
    EmptyOperator,
    TaskGroup,
    TriggerRule,
    task,
)
from overture_airflow_provider.matching_taskgroup import _paths, _result_paths

TABLES = ["division", "division_area"]
INPUTS = {table: f"s3://feeds/provider=linz/data/type={table}" for table in TABLES}
BASELINE = {table: f"s3://baseline/type={table}" for table in TABLES}
RESULTS = {table: f"s3://matches/linz/type={table}" for table in TABLES}
JOIN = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


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
    child_dag_run_task_group(
        "load", trigger_dag_id="load_table", requests=load_requests(result_paths)
    )


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
        assert group.result_paths.operator.task_id == prefix + "result_paths"
        # One way in and one way out, so `upstream >> group >> finalize` gates and completes it.
        assert [root.task_id for root in group.group.get_roots()] == [prefix + "start"]
        assert [leaf.task_id for leaf in group.group.get_leaves()] == [prefix + "complete"]


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
    group_tasks = {task_id for task_id in dag.task_ids if task_id.startswith("matching.")}
    exports = {"matching.input.export", "matching.baseline.export"}
    assert (
        dag.get_task("matching.start").downstream_task_ids
        == {
            "matching.work_succeeded",
            "matching.input.export_side",
            "matching.baseline.export_side",
        }
        | exports
    )
    # `complete` waits for the last tasks of every step and for the failure barrier, which
    # waits for every task: `start` and the exports are the only ones it does not follow.
    barrier = {"matching.work_succeeded"}
    assert upstream("matching.complete") == (
        group_tasks - exports - {"matching.start", "matching.complete"}
    )
    assert upstream("matching.work_succeeded") == group_tasks - barrier - {"matching.complete"}
    assert upstream("no_check.matcher.match") == {
        "no_check.input.paths",
        "no_check.input.export_side",
        "no_check.baseline.paths",
    }


@pytest.mark.parametrize("rule", [TriggerRule.NONE_FAILED, TriggerRule.ALL_DONE])
@pytest.mark.parametrize("scope", ["dag", "parent_group"])
def test_factory_tasks_ignore_permissive_inherited_trigger_rules(rule, scope):
    defaults = {"trigger_rule": rule, "retries": 2}
    with DAG(
        "permissive_defaults",
        schedule=None,
        start_date=datetime(2026, 1, 1),
        default_args=defaults if scope == "dag" else {},
    ) as dag:
        with TaskGroup("parent", default_args=defaults if scope == "parent_group" else {}):
            outside = resolve_baseline()
            result = make_group(
                input=lambda: resolve_baseline(),
                baseline=resolve_baseline,
                pre_match_task_factory=check,
                provenance_task_factory=publish,
                sink_task_factory=load_tables,
            )
    rules = {operator.task_id: operator.trigger_rule for operator in dag.tasks}
    assert outside.operator.trigger_rule == rule
    for operator in result.group.iter_tasks():
        joins = operator.task_id.endswith((".run_ids", ".work_succeeded"))
        assert rules[operator.task_id] == (JOIN if joins else TriggerRule.ALL_SUCCESS)
        assert operator.retries == (0 if operator.task_id.endswith(".trigger") else 2)


def sink_of(build):
    return {"sink_task_factory": lambda *, result_paths: build(result_paths)}


def all_done_cleanup(result_paths):
    store(result_paths) >> EmptyOperator(task_id="cleanup", trigger_rule=TriggerRule.ALL_DONE)


def none_failed_load(result_paths):
    store.override(trigger_rule=TriggerRule.NONE_FAILED)(result_paths)


def join_fed_from_outside(outside):
    def build(result_paths):
        [store(result_paths), outside] >> EmptyOperator(task_id="join", trigger_rule=JOIN)

    return build


def work_after_a_teardown(result_paths):
    (
        store(result_paths)
        >> EmptyOperator(task_id="cleanup").as_teardown()
        >> EmptyOperator(task_id="report")
    )


def test_tasks_that_a_skipped_gate_would_not_stop_are_rejected():
    with new_dag("ungated"):
        outside = resolve_baseline()
        for build, message in (
            (all_done_cleanup, r"sink\.cleanup has trigger rule 'all_done'.*as_teardown"),
            (none_failed_load, r"sink\.store has trigger rule 'none_failed'"),
            (
                join_fed_from_outside(outside),
                r"sink\.join could run after a skipped gate.*\['resolve_baseline'\]"
                r".*only non-teardown tasks of the group",
            ),
            (
                work_after_a_teardown,
                r"sink\.report could run after a skipped gate.*sink\.cleanup'\]"
                r".*a non-teardown task of the group",
            ),
        ):
            with pytest.raises(ValueError, match=message):
                make_group(**sink_of(build))


def test_branches_teardowns_and_the_provider_task_groups_are_accepted():
    def spark_matcher(*, input_paths, baseline_paths):
        job = spark_agnostic_task_group(
            group_id="spark", spark_impl_name="GLUE_v5", sedona_version="1.7.0"
        )
        results = match(input_paths, baseline_paths)
        job >> results
        return results

    def branching_sink(*, result_paths):
        @task.branch
        def decide():
            return "matching.sink.no_op"

        register = EmptyOperator(task_id="register").as_setup()
        requests = load_requests(result_paths)
        loads = child_dag_run_task_group("load", trigger_dag_id="load_table", requests=requests)
        no_op = EmptyOperator(task_id="no_op")
        decide() >> [requests, no_op]
        register >> loads.group >> EmptyOperator(task_id="cleanup").as_teardown(setups=register)
        [loads.group, no_op] >> EmptyOperator(task_id="end", trigger_rule=JOIN)

    with new_dag("accepted") as dag:
        outside = resolve_baseline()
        # A task output built elsewhere is an input, not gated work; `paths` waits for `start` too.
        make_group(
            input=outside, matcher_task_factory=spark_matcher, sink_task_factory=branching_sink
        )
    dag.validate()
    watched = dag.get_task("matching.work_succeeded").upstream_task_ids
    assert "matching.sink.cleanup" not in watched
    # Airflow only lets `all_success` tasks follow a setup, so the barrier follows a relay.
    assert "matching.sink.register" not in watched
    assert dag.get_task("matching.sink_register_succeeded").upstream_task_ids == {
        "matching.sink.register"
    }
    assert "matching.sink_register_succeeded" in watched


def test_factories_and_tables_are_validated_at_dag_construction():
    with new_dag("bad_matching"):
        prebuilt = match(INPUTS, BASELINE)
        for factory in ("matcher_task_factory", "input", "baseline"):
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
        for tables in ("division,division_area", "", [], ["division", "division"], [" "]):
            with pytest.raises(ValueError, match="tables"):
                make_group(tables=tables)
        # A literal map is checked without waiting for the task that would reject it.
        with pytest.raises(ValueError, match=r"baseline paths .* found \['place'\]"):
            make_group(baseline={"place": "s3://other"})


def test_path_maps_must_cover_exactly_the_tables():
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
