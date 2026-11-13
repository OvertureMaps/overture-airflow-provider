from collections import UserList
from datetime import datetime
from types import SimpleNamespace

import pytest

from overture_airflow_provider import child_dag_run_task_group
from overture_airflow_provider._airflow_compat import (
    DAG,
    AirflowFailException,
    TriggerDagRunOperator,
    task,
)
from overture_airflow_provider.child_dag_runs import ChildDagRunOperator, _requests, _run_ids


@task
def requests():
    return [{"table": "division"}, {"table": "division_area"}]


def test_triggers_wait_and_rerun_the_same_child_run():
    with DAG("child_runs", schedule=None, start_date=datetime(2026, 1, 1)) as dag:
        defaults = child_dag_run_task_group(
            "load", trigger_dag_id="load_table", requests=requests()
        )
        tuned = child_dag_run_task_group(
            "export",
            trigger_dag_id="export_table",
            requests=[{"table": "division"}],
            retries=2,
            max_active_tis_per_dagrun=1,
            pool="serial",
        )
    trigger = dag.get_task("load.trigger")
    assert trigger.partial_kwargs["wait_for_completion"] is True
    # Clearing a trigger must rerun its own child run, not fail on the existing run ID.
    assert trigger.partial_kwargs["reset_dag_run"] is True
    assert trigger.partial_kwargs["retries"] == 0
    assert trigger.partial_kwargs["max_active_tis_per_dagrun"] is None
    assert defaults.group.group_id == "load"
    run_ids = defaults.run_ids.operator
    assert run_ids.task_id == "load.run_ids"

    tuned_kwargs = dag.get_task("export.trigger").partial_kwargs
    assert (tuned_kwargs["retries"], tuned_kwargs["max_active_tis_per_dagrun"]) == (2, 1)
    assert tuned_kwargs["pool"] == "serial"

    # Requests built outside the group or given literally: the staged copy is the only
    # root either way, so a gate on the group reaches every task in it.
    for runs, outside in ((defaults, {"requests"}), (tuned, set())):
        prefix = runs.group.group_id
        assert [root.task_id for root in runs.group.get_roots()] == [f"{prefix}.requests"]
        assert dag.get_task(f"{prefix}.requests").upstream_task_ids == outside
        assert dag.get_task(f"{prefix}.trigger").upstream_task_ids == {f"{prefix}.requests"}
        assert runs.run_ids.operator.upstream_task_ids == {
            f"{prefix}.requests",
            f"{prefix}.trigger",
        }


def test_literal_requests_are_checked_at_dag_construction():
    with DAG("bad_requests", schedule=None, start_date=datetime(2026, 1, 1)):
        with pytest.raises(ValueError, match="requests must not be empty"):
            child_dag_run_task_group("load", trigger_dag_id="load_table", requests=[])
        # A single conf, a template string or nothing at all is not a list of confs.
        for bad in ({"table": "division"}, "{{ params.requests }}", None):
            with pytest.raises(TypeError, match="list of conf mappings or a task output"):
                child_dag_run_task_group("load", trigger_dag_id="load_table", requests=bad)


def test_child_run_id_is_fixed_by_the_task_instance_and_its_conf(mocker):
    mocker.patch.object(TriggerDagRunOperator, "execute")

    def run_id(conf=None, **ti):
        operator = ChildDagRunOperator(
            task_id="trigger", trigger_dag_id="load_table", conf=conf or {"table": "division"}
        )
        instance = {"dag_id": "parent", "run_id": "run", "task_id": "load.trigger", "map_index": 0}
        operator.execute({"ti": SimpleNamespace(**{**instance, **ti})})
        return operator.trigger_run_id

    first = run_id()
    assert first.startswith("child__") and first == run_id()
    others = [
        run_id(conf={"table": "division_area"}),
        run_id(map_index=1),
        run_id(task_id="other.load.trigger"),
        run_id(run_id="later"),
        run_id(dag_id="other_parent"),
    ]
    assert len({first, *others}) == 6


def test_requests_must_be_a_list_of_conf_mappings():
    confs = [{"table": "division"}, {}]
    assert _requests.function(confs) == _requests.function(tuple(confs)) == confs
    assert _requests.function([]) == []
    # A mapped task's output is a sequence that is not a list; it is staged as one.
    staged = _requests.function(UserList(confs))
    assert type(staged) is list and staged == confs
    # A producer that returned nothing must not pass for one with nothing to do.
    for bad in (None, {"table": "division"}, "division", [None], ["division"], [confs]):
        with pytest.raises(AirflowFailException, match="list of conf mappings"):
            _requests.function(bad)


def test_run_ids_require_one_completed_run_per_request():
    assert _run_ids.function("load_table", [{}, {}], ["a", "b"]) == ["a", "b"]
    # Nothing was requested, so the mapped trigger was skipped and left no run IDs.
    assert _run_ids.function("load_table", [], None) == []
    for runs in (["a"], [], None, ["a", None], ["a", ""]):
        with pytest.raises(AirflowFailException, match="Expected 2 completed child runs"):
            _run_ids.function("load_table", [{}, {}], runs)
