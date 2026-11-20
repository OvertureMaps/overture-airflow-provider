"""Run child DAG run groups through real Airflow scheduling, with simulated child runs."""

from datetime import UTC, datetime

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from overture_airflow_provider import child_dag_run_task_group
from overture_airflow_provider._airflow_compat import (
    DAG,
    TaskGroup,
    TriggerDagRunOperator,
    TriggerRule,
    task,
)

pytestmark = pytest.mark.e2e

BOTH_RUN = [{"table": "a"}, {"table": "b"}]
ONE_SKIPS = [{"table": "a"}, {"table": "b", "skip": True}]
ALL_SKIP = [{"table": "a", "skip": True}, {"table": "b", "skip": True}]

# name: (requests, how they are built, gate open, run_ids state, finalize state)
CASES = {
    "literal": (BOTH_RUN, "literal", True, "success", "success"),
    "task": (BOTH_RUN, "task", True, "success", "success"),
    "task_empty": ([], "task", True, "success", "success"),
    # A mapped task's output and a ``.map()`` result are read lazily, not as lists.
    "mapped": (BOTH_RUN, "mapped", True, "success", "success"),
    "map": (BOTH_RUN, "map", True, "success", "success"),
    # A gated-off group does nothing, wherever its requests were built.
    "literal_gated": (BOTH_RUN, "literal", False, "skipped", "skipped"),
    "task_gated": (BOTH_RUN, "task", False, "skipped", "skipped"),
    "task_empty_gated": ([], "task", False, "skipped", "skipped"),
    # A request without a run ID is a failure, not a skip and not a completion.
    "literal_one_skips": (ONE_SKIPS, "literal", True, "failed", "upstream_failed"),
    "task_one_skips": (ONE_SKIPS, "task", True, "failed", "upstream_failed"),
    "literal_all_skip": (ALL_SKIP, "literal", True, "failed", "upstream_failed"),
    "task_all_skip": (ALL_SKIP, "task", True, "failed", "upstream_failed"),
    "task_not_a_list": ({"table": "a"}, "task", True, "upstream_failed", "upstream_failed"),
}


@task.short_circuit(ignore_downstream_trigger_rules=False)
def gate(is_open):
    return is_open


@task
def skips_itself():
    raise AirflowSkipException("simulated gate that skips itself")


@task
def build_requests(requests):
    return requests


@task
def build_request(conf):
    return conf


def as_built(conf):
    return conf


def build(name, requests, how):
    if how == "task":
        return build_requests.override(task_id=f"{name}_requests")(requests)
    if how == "mapped":
        return build_request.override(task_id=f"{name}_requests").expand(conf=requests)
    if how == "map":
        return build_requests.override(task_id=f"{name}_requests")(requests).map(as_built)
    return requests


@task
def finalize(run_ids):
    return len(run_ids)


def test_child_run_execution_states(monkeypatch, run_dag):
    triggered = []

    def execute_trigger(self, context):
        if self.conf.get("skip"):
            raise AirflowSkipException("simulated trigger skip, before the run ID is pushed")
        context["ti"].xcom_push(key="trigger_run_id", value=self.trigger_run_id)
        triggered.append(self.task_id)

    monkeypatch.setattr(TriggerDagRunOperator, "execute", execute_trigger)

    with DAG(
        "child_run_states",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
    ) as dag:
        for name, (requests, how, is_open, _, _) in CASES.items():
            runs = child_dag_run_task_group(
                name, trigger_dag_id="simulated_child", requests=build(name, requests, how)
            )
            gate.override(task_id=f"{name}_gate")(is_open) >> runs.group
            finalize.override(task_id=f"{name}_finalize")(runs.run_ids)
        # ``default_args`` that make tasks run after a skip must not reach the group's own.
        with TaskGroup("permissive", default_args={"trigger_rule": TriggerRule.NONE_FAILED}):
            runs = child_dag_run_task_group(
                "load", trigger_dag_id="simulated_child", requests=BOTH_RUN
            )
            skips_itself() >> runs.group

    run = run_dag(dag)
    states = {ti.task_id: ti.state for ti in run.get_task_instances() if ti.map_index == -1}
    observed = {name: (states[f"{name}.run_ids"], states[f"{name}_finalize"]) for name in CASES}
    assert observed == {name: case[3:] for name, case in CASES.items()}
    assert {
        states[f"permissive.load.{task_id}"] for task_id in ("requests", "trigger", "run_ids")
    } == {"skipped"}
    # Each request that neither skipped nor was gated off triggered exactly one child run.
    expected = [
        f"{name}.trigger"
        for name, (requests, _, is_open, _, _) in CASES.items()
        if is_open and isinstance(requests, list)
        for conf in requests
        if not conf.get("skip")
    ]
    assert sorted(triggered) == sorted(expected)
