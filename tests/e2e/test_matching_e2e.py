"""Run matching groups through real Airflow scheduling, with simulated child DAG runs."""

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
def finalized(completion):
    return completion


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
            return child_dag_run_task_group(
                "load",
                trigger_dag_id="simulated_load",
                requests=load_requests(provider, result_paths, load),
            ).run_ids

        return build

    with DAG(
        f"matching_states_{failure or 'ok'}_{selected}_{load}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
        render_template_as_native_obj=True,
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
                        baseline={table: f"s3://baseline/type={table}" for table in TABLES},
                        pre_match_task_factory=lambda **paths: check_inputs(
                            **paths, failure=failure
                        ),
                        matcher_task_factory=lambda provider=provider, **paths: fake_match(
                            **paths, provider=provider, failure=failure
                        ),
                        provenance_task_factory=lambda **paths: publish(**paths, failure=failure),
                        sink_task_factory=sink_factory(provider),
                    )
                    gate >> result.group
                    completions.append(finalized(result.complete))
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
    assert all(run_id.startswith("child__") for run_id in run_ids)


@pytest.mark.parametrize(
    "failed_step", ["input", "baseline", "pre_match", "matcher", "provenance", "sink"]
)
def test_completion_rejects_failed_non_leaf_work(run_dag, failed_step):
    @task
    def work(fail):
        if fail:
            raise ValueError("simulated non-leaf failure")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup(value):
        return value

    def factory(step, value):
        def build(**paths):
            output = cleanup(value)
            work(step == failed_step) >> output
            return output

        return build

    paths = {table: f"s3://example/type={table}" for table in TABLES}
    with DAG(
        f"matching_non_leaf_failure_{failed_step}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
    ) as dag:
        result = matching_task_group(
            "matching",
            tables=TABLES,
            input=factory("input", paths),
            baseline=factory("baseline", paths),
            pre_match_task_factory=factory("pre_match", None),
            matcher_task_factory=factory("matcher", paths),
            provenance_task_factory=factory("provenance", "published"),
            sink_task_factory=factory("sink", "stored"),
        )
        finalized(result.complete)

    run = run_dag(dag)
    states = {ti.task_id: ti.state for ti in run.get_task_instances()}
    assert states[f"matching.{failed_step}.work"] == "failed"
    assert states[f"matching.{failed_step}.cleanup"] == "success"
    assert states["matching.complete"] == states["finalized"] == "upstream_failed"
    assert run.state == "failed"


@task
def store(table):
    return f"stored {table}"


@task
def result_tables(result_paths):
    return sorted(result_paths)


def stored(table):
    return f"stored {table}"


@task
def expect(value, expected):
    assert value == expected, value


@task
def source_gate(state):
    if state == "unchanged":
        raise AirflowSkipException("simulated unchanged source")
    if state == "failed":
        raise ValueError("simulated source check failure")


@pytest.mark.parametrize("source_state", ["changed", "unchanged", "failed"])
def test_completion_with_mapped_outputs_and_permissive_default_args(run_dag, source_state):
    # Both sinks return an output that is read lazily: a mapped task's, and a ``.map()`` result.
    sinks = {
        "mapped": lambda *, result_paths: store.expand(table=list(TABLES)),
        "map": lambda *, result_paths: result_tables(result_paths).map(stored),
    }

    def group(name, sink, exported=False):
        return matching_task_group(
            name,
            tables=TABLES,
            input=(lambda: export(name))
            if exported
            else {table: f"s3://feeds/{name}/type={table}" for table in TABLES},
            baseline=(lambda: export("baseline"))
            if exported
            else {table: f"s3://baseline/type={table}" for table in TABLES},
            pre_match_task_factory=lambda **paths: check_inputs(**paths, failure=""),
            matcher_task_factory=lambda **paths: fake_match(**paths, provider=name, failure=""),
            provenance_task_factory=lambda **paths: publish(**paths, failure=""),
            sink_task_factory=sink,
        )

    with DAG(
        f"matching_completion_{source_state}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0, "trigger_rule": TriggerRule.NONE_FAILED},
    ) as dag:
        for name, sink in sinks.items():
            complete = group(name, sink).complete
            expect.override(task_id=f"{name}_finalized")(
                complete, [f"stored {table}" for table in TABLES]
            )
        with TaskGroup("permissive", default_args={"trigger_rule": TriggerRule.ALL_DONE}):
            gated = group("gated", sinks["mapped"], exported=True)
            source_gate(source_state) >> gated.group
        finalized.override(task_id="gated_finalized", trigger_rule=TriggerRule.ALL_SUCCESS)(
            gated.complete
        )

    run = run_dag(dag)
    assert run.state == ("failed" if source_state == "failed" else "success")
    states = {ti.task_id: ti.state for ti in run.get_task_instances() if ti.map_index == -1}
    for name in sinks:
        assert states[f"{name}.complete"] == states[f"{name}_finalized"] == "success"
    expected = {
        "changed": "success",
        "unchanged": "skipped",
        "failed": "upstream_failed",
    }[source_state]
    assert {
        ti.state for ti in run.get_task_instances() if ti.task_id.startswith("permissive.gated.")
    } == {expected}
    assert states["gated_finalized"] == expected
