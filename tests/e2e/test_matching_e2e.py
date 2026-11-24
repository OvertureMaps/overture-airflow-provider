"""Run matching groups through real Airflow scheduling, with simulated child DAG runs."""

import json
from datetime import UTC, datetime

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from overture_airflow_provider import child_dag_run_task_group, matching_task_group
from overture_airflow_provider._airflow_compat import (
    DAG,
    EmptyOperator,
    TaskGroup,
    TriggerDagRunOperator,
    TriggerRule,
    task,
)

pytestmark = pytest.mark.e2e
TABLES = ("division", "division_area")
PROVIDERS = ("osm", "linz")


@task.short_circuit(ignore_downstream_trigger_rules=False)
def changed(selected):
    return selected


@task
def export(provider):
    return {table: f"s3://feeds/{provider}/type={table}" for table in TABLES}


@task(task_id="side")
def side(step, failure):
    """A task its factory neither chains nor returns; later steps must still wait for it."""
    if failure == f"{step}_side":
        raise ValueError(f"simulated {step} side-task failure")
    if failure == f"{step}_side_skip":
        raise AirflowSkipException(f"simulated {step} side-task skip")


@task
def check_inputs(input_paths, baseline_paths, failure):
    if failure == "pre_match":
        raise ValueError("simulated pre-match failure")
    return True


@task
def fake_match(input_paths, baseline_paths, provider, failure):
    if failure == "matcher":
        raise ValueError("simulated matcher failure")
    if failure == "matcher_skip":
        raise AirflowSkipException("simulated matcher skip")
    return {table: f"s3://matches/{provider}/type={table}" for table in TABLES}


@task
def publish(input_paths, baseline_paths, result_paths, failure):
    if failure == "provenance":
        raise ValueError("simulated provenance failure")
    return "published"


@task
def load_requests(provider, result_paths, load):
    if not load:
        return []
    return [
        {"table": table, "path": path, "source": provider, "version": "123"}
        for table, path in result_paths.items()
    ]


@task
def finalized():
    return "finalized"


@pytest.mark.parametrize(
    "failure,selected,load",
    [
        ("", True, True),
        ("", False, True),
        ("", "osm", True),
        ("", True, False),
        ("input_side", True, True),
        ("pre_match", True, True),
        ("matcher", True, True),
        ("matcher_skip", True, True),
        ("provenance", True, True),
        ("load", True, True),
        ("load_skip", True, True),
        ("sink_side", True, True),
        ("sink_side_skip", True, True),
    ],
)
def test_matching_execution_states(monkeypatch, run_dag, failure, selected, load):
    loads = []
    versions = set()

    def execute_trigger(self, context):
        conf = self.conf
        if failure == "load_skip" and conf["table"] == "division_area":
            raise AirflowSkipException("simulated trigger skip, before the run ID is pushed")
        context["ti"].xcom_push(key="trigger_run_id", value=self.trigger_run_id)
        loads.append((self.task_id, conf["source"], conf["table"], self.trigger_run_id))
        versions.add(conf["version"])
        if failure == "load" and conf["table"] == "division_area":
            raise ValueError("simulated child run failure")

    monkeypatch.setattr(TriggerDagRunOperator, "execute", execute_trigger)

    def input_factory(provider):
        def build():
            side("input", failure)
            return export(provider)

        return build

    def sink_factory(provider):
        def build(*, result_paths):
            side("sink", failure)
            child_dag_run_task_group(
                "load",
                trigger_dag_id="simulated_load",
                requests=load_requests(provider, result_paths, load),
            )

        return build

    with DAG(
        f"matching_states_{failure or 'ok'}_{selected}_{load}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
        render_template_as_native_obj=True,
        params={"baseline": json.dumps({table: f"s3://baseline/type={table}" for table in TABLES})},
    ) as dag:
        completions = []
        with TaskGroup("providers"):
            for provider in PROVIDERS:
                with TaskGroup(provider):
                    gate = changed(selected is True or selected == provider)
                    result = matching_task_group(
                        group_id="arbitrary",
                        tables=TABLES,
                        input=input_factory(provider),
                        baseline="{{ params.baseline }}",
                        pre_match_task_factory=lambda **paths: check_inputs(
                            **paths, failure=failure
                        ),
                        matcher_task_factory=lambda provider=provider, **paths: fake_match(
                            **paths, provider=provider, failure=failure
                        ),
                        provenance_task_factory=lambda **paths: publish(**paths, failure=failure),
                        sink_task_factory=sink_factory(provider),
                    )
                    final = finalized()
                    gate >> result.group >> final
                    completions.append(final)
        ingest_done = EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)
        completions >> ingest_done

    run = run_dag(dag)
    states = {(ti.task_id, ti.map_index): ti.state for ti in run.get_task_instances()}

    # A skip stops the steps after it without failing the run; it is never a completion.
    skips = ("matcher_skip", "sink_side_skip")
    assert run.state == ("failed" if selected and failure and failure not in skips else "success")
    for provider in PROVIDERS:
        prefix = f"providers.{provider}.arbitrary."
        complete = states[(prefix + "complete", -1)]
        final = states[(f"providers.{provider}.finalized", -1)]
        if not (selected is True or selected == provider):
            assert all(
                state == "skipped"
                for (task_id, _), state in states.items()
                if task_id.startswith(prefix)
            )
            assert final == "skipped"
        elif failure:
            assert complete != "success" and final != "success"
        else:
            assert complete == "success" and final == "success"
        if failure.startswith("sink_side"):
            # Every load finished, and the sink's other task still holds completion back.
            assert states[(prefix + "sink.load.run_ids", -1)] == "success"
    if failure in ("input_side", "pre_match", "matcher", "matcher_skip"):
        assert all(
            states[(f"providers.{provider}.arbitrary.result_paths", -1)] != "success"
            for provider in PROVIDERS
        )

    reaches_the_loads = ("", "load", "load_skip", "sink_side", "sink_side_skip")
    loaded = selected and load and failure in reaches_the_loads
    expected = {
        (provider, table)
        for provider in PROVIDERS
        if loaded and (selected is True or selected == provider)
        for table in TABLES
        if not (failure == "load_skip" and table == "division_area")
    }
    assert {(source, table) for _, source, table, _ in loads} == expected
    assert all(task_id.startswith(f"providers.{source}.") for task_id, source, _, _ in loads)
    # The DAG renders natively; a mapped conf must still arrive as the request built it.
    assert versions <= {"123"}
    run_ids = [run_id for *_, run_id in loads]
    assert len(set(run_ids)) == len(run_ids)
    assert all(
        run_id.startswith(f"child__{dag.dag_id}__{task_id}__") for task_id, *_, run_id in loads
    )


@task
def work(result_paths=None):
    return "done"


@task
def fails():
    raise ValueError("simulated failure")


@task
def skips():
    raise AirflowSkipException("simulated skip")


JOIN = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
PATHS = {table: f"s3://example/type={table}" for table in TABLES}


def run_with_sink(run_dag, name, sink):
    with DAG(
        f"matching_sink_{name}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
    ) as dag:
        result = matching_task_group(
            "matching",
            tables=TABLES,
            input=PATHS,
            baseline=PATHS,
            matcher_task_factory=lambda **paths: fake_match(**paths, provider="x", failure=""),
            sink_task_factory=sink,
        )
        (
            result.group
            >> finalized()
            >> EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)
        )
    run = run_dag(dag)
    return run, {ti.task_id: ti.state for ti in run.get_task_instances() if ti.map_index == -1}


def failure_behind_a_join(*, result_paths):
    # `load` is skipped as soon as `skipped` is, before `late_failure` has failed.
    late_failure = fails.override(task_id="late_failure")()
    work.override(task_id="takes_a_while")() >> late_failure
    load = work.override(task_id="load")(result_paths)
    [skips(), late_failure] >> load
    [load, work.override(task_id="other")()] >> EmptyOperator(task_id="end", trigger_rule=JOIN)


def failure_after_a_branch(*, result_paths):
    # A branch chooses not to load; the task that the load also waits for fails afterwards.
    @task.branch
    def decide():
        return "matching.sink.no_op"

    create_branch = fails.override(task_id="create_branch")()
    work.override(task_id="takes_a_while")() >> create_branch
    requests = load_requests("x", result_paths, True)
    loads = child_dag_run_task_group("load", trigger_dag_id="simulated_load", requests=requests)
    no_op = EmptyOperator(task_id="no_op")
    decide() >> [requests, no_op]
    create_branch >> loads.group
    [loads.group, no_op] >> EmptyOperator(task_id="end", trigger_rule=JOIN)


def failed_setup_after_a_branch(*, result_paths):
    # The same with a setup, which Airflow only lets `all_success` tasks follow.
    @task.branch
    def decide():
        return "matching.sink.no_op"

    register = fails.override(task_id="register")().as_setup()
    work.override(task_id="takes_a_while")() >> register
    requests = work.override(task_id="requests")(result_paths)
    load = work.override(task_id="load")()
    no_op = EmptyOperator(task_id="no_op")
    decide() >> [requests, no_op]
    (
        [requests, register]
        >> load
        >> work.override(task_id="deregister")().as_teardown(setups=register)
    )
    [load, no_op] >> EmptyOperator(task_id="end", trigger_rule=JOIN)


def failure_before_a_teardown(*, result_paths):
    fails.override(task_id="load")() >> work.override(task_id="end")().as_teardown()


@pytest.mark.parametrize(
    "sink,failed",
    [
        (failure_behind_a_join, "late_failure"),
        (failure_after_a_branch, "create_branch"),
        (failed_setup_after_a_branch, "register"),
        (failure_before_a_teardown, "load"),
    ],
)
def test_a_failed_task_blocks_completion_even_when_the_last_tasks_succeed(run_dag, sink, failed):
    run, states = run_with_sink(run_dag, sink.__name__, sink)
    assert states[f"matching.sink.{failed}"] == "failed"
    # The step's last task succeeded all the same: only the failure barrier holds completion.
    assert states["matching.sink.end"] == "success"
    assert states["matching.work_succeeded"] == "upstream_failed"
    for task_id in ("matching.complete", "finalized", "ingest_done"):
        assert states[task_id] == "upstream_failed"
    assert run.state == "failed"


def branch_to_a_no_op(*, result_paths):
    @task.branch
    def decide():
        return "matching.sink.no_op"

    load = work.override(task_id="load")(result_paths)
    no_op = EmptyOperator(task_id="no_op")
    decide() >> [load, no_op] >> EmptyOperator(task_id="end", trigger_rule=JOIN)


def short_circuit(*, result_paths):
    @task.short_circuit(ignore_downstream_trigger_rules=False)
    def should_load():
        return False

    load = work.override(task_id="load")(result_paths)
    should_load() >> load >> EmptyOperator(task_id="end", trigger_rule=JOIN)


def map_over_nothing(*, result_paths):
    @task
    def nothing_to_store(result_paths):
        return []

    work.override(task_id="end").expand(result_paths=nothing_to_store(result_paths))


@pytest.mark.parametrize(
    "sink,completion",
    [(branch_to_a_no_op, "success"), (short_circuit, "skipped"), (map_over_nothing, "skipped")],
)
def test_only_a_branch_with_a_join_finishes_a_step_early(run_dag, sink, completion):
    run, states = run_with_sink(run_dag, sink.__name__, sink)
    assert states["matching.sink.end"] == completion
    assert states["matching.complete"] == states["finalized"] == completion
    # A skip is not a completion, and it does not fail the run either.
    assert run.state == "success"


@task
def store(table):
    return f"stored {table}"


@task
def source_gate(state):
    if state == "unchanged":
        raise AirflowSkipException("simulated unchanged source")
    if state == "failed":
        raise ValueError("simulated source check failure")


def store_between_setup_and_teardown(*, result_paths):
    prepare = work.override(task_id="prepare")().as_setup()
    tidy = work.override(task_id="tidy")().as_teardown(setups=prepare)
    prepare >> store.expand(table=list(TABLES)) >> tidy


@pytest.mark.parametrize("source_state", ["changed", "unchanged", "failed"])
def test_source_gates_under_permissive_default_args(run_dag, source_state):
    with DAG(
        f"matching_gate_{source_state}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0, "trigger_rule": TriggerRule.NONE_FAILED},
    ) as dag:
        with TaskGroup("permissive", default_args={"trigger_rule": TriggerRule.ALL_DONE}):
            gated = matching_task_group(
                "gated",
                tables=TABLES,
                input=lambda: export("gated"),
                baseline=lambda: export("baseline"),
                pre_match_task_factory=lambda **paths: check_inputs(**paths, failure=""),
                matcher_task_factory=lambda **paths: fake_match(
                    **paths, provider="gated", failure=""
                ),
                provenance_task_factory=lambda **paths: publish(**paths, failure=""),
                sink_task_factory=store_between_setup_and_teardown,
            )
            source_gate(source_state) >> gated.group
        gated.group >> finalized.override(trigger_rule=TriggerRule.ALL_SUCCESS)()

    run = run_dag(dag)
    assert run.state == ("failed" if source_state == "failed" else "success")
    expected = {
        "changed": "success",
        "unchanged": "skipped",
        "failed": "upstream_failed",
    }[source_state]
    states = {ti.task_id: ti.state for ti in run.get_task_instances()}
    assert {
        state for task_id, state in states.items() if task_id.startswith("permissive.gated.")
    } == {expected}
    assert states["finalized"] == expected


def test_providers_take_turns_and_a_failed_one_holds_the_rest(run_dag):
    # The scaffold's chain: each provider waits for the one before it to finish or be skipped.
    outcomes = {"unchanged": "", "matched": "", "broken": "matcher", "held": ""}
    with DAG(
        "matching_turns",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
    ) as dag:
        sources = []
        previous_done = None
        for provider, failure in outcomes.items():
            with TaskGroup(provider) as source:
                result = matching_task_group(
                    "match",
                    tables=TABLES,
                    input=PATHS,
                    baseline=PATHS,
                    matcher_task_factory=lambda provider=provider, failure=failure, **paths: (
                        fake_match(**paths, provider=provider, failure=failure)
                    ),
                )
                changed(provider != "unchanged") >> result.group >> finalized()
            if previous_done is not None:
                previous_done >> result.group
            previous_done = EmptyOperator(
                task_id=f"{provider}_done", trigger_rule=TriggerRule.NONE_FAILED
            )
            source >> previous_done
            sources.append(source)
        sources >> EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)

    run = run_dag(dag)
    states = {ti.task_id: ti.state for ti in run.get_task_instances()}
    assert {
        provider: (states[f"{provider}.match.start"], states[f"{provider}.finalized"])
        for provider in outcomes
    } == {
        "unchanged": ("skipped", "skipped"),
        "matched": ("success", "success"),
        "broken": ("success", "upstream_failed"),
        "held": ("upstream_failed", "upstream_failed"),
    }
    assert states["unchanged_done"] == states["matched_done"] == "success"
    assert states["broken_done"] == states["ingest_done"] == "upstream_failed"
    assert run.state == "failed"
