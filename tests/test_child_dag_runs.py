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
from overture_airflow_provider.child_dag_runs import ChildDagRunOperator, _run_ids


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
    assert run_ids.upstream_task_ids == {"requests", "load.trigger"}

    tuned_kwargs = dag.get_task("export.trigger").partial_kwargs
    assert (tuned_kwargs["retries"], tuned_kwargs["max_active_tis_per_dagrun"]) == (2, 1)
    assert tuned_kwargs["pool"] == "serial"
    assert tuned.run_ids.operator.upstream_task_ids == {"export.trigger"}


def test_an_empty_literal_request_list_is_rejected():
    with DAG("no_requests", schedule=None, start_date=datetime(2026, 1, 1)):
        with pytest.raises(ValueError, match="requests"):
            child_dag_run_task_group("load", trigger_dag_id="load_table", requests=[])


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


def test_run_ids_require_one_completed_run_per_request():
    assert _run_ids.function("load_table", [{}, {}], ["a", "b"]) == ["a", "b"]
    # Nothing was requested, so the mapped trigger was skipped and left no run IDs.
    assert _run_ids.function("load_table", [], None) == []
    for runs in (["a"], [], None, ["a", None], ["a", ""]):
        with pytest.raises(AirflowFailException, match="Expected 2 completed child runs"):
            _run_ids.function("load_table", [{}, {}], runs)
