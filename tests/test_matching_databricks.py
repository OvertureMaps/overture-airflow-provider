import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from overture_airflow_provider._airflow_compat import AirflowException
from overture_airflow_provider._matching_databricks import DatabricksMatchingOperator
from tests.test_matching import RESULTS, parameters


@pytest.fixture
def operator():
    return DatabricksMatchingOperator(
        task_id="outer.linz.match",
        parameters=parameters(),
        databricks_conn_id="caller_connection",
        job_name="caller_job",
    )


@pytest.fixture
def hook(mocker):
    result = mocker.patch(
        "airflow.providers.databricks.hooks.databricks.DatabricksHook"
    ).return_value
    result.find_job_id_by_name.return_value = 10
    result.run_now.return_value = 20
    result.get_run_state.return_value = SimpleNamespace(is_terminal=True, is_successful=True)
    result.get_run.return_value = {"tasks": [{"task_key": "id_assignment", "run_id": 30}]}
    result.get_run_output.return_value = {
        "notebook_output": {"result": json.dumps({"resultDataPaths": RESULTS})}
    }
    return result


@pytest.fixture
def context():
    return {"ti": Mock(dag_id="caller_dag", run_id="caller_run")}


def test_named_job_parameters_and_output_preserve_legacy_contract(operator, hook, context):
    assert operator.execute(context) == RESULTS
    hook.find_job_id_by_name.assert_called_once_with("caller_job")
    request = hook.run_now.call_args.kwargs["json"]
    assert request["job_parameters"] == parameters()
    assert request["job_id"] == 10
    assert len(request["idempotency_token"]) == 64
    hook.get_run_output.assert_called_once_with(30)
    context["ti"].xcom_push.assert_called_once_with(key="databricks_run_id", value=20)


def test_same_invocation_reuses_idempotency_token_but_other_instances_do_not(
    operator, hook, context
):
    operator.execute(context)
    first = hook.run_now.call_args.kwargs["json"]["idempotency_token"]
    operator.execute(context)
    assert hook.run_now.call_args.kwargs["json"]["idempotency_token"] == first
    operator.task_id = "outer.osm.match"
    operator.execute(context)
    assert hook.run_now.call_args.kwargs["json"]["idempotency_token"] != first


def test_failed_job_never_fetches_success_output(operator, hook, context):
    hook.get_run_state.return_value = SimpleNamespace(is_terminal=True, is_successful=False)
    with pytest.raises(AirflowException, match="failed"):
        operator.execute(context)
    hook.get_run_output.assert_not_called()


def test_timeout_and_missing_job_are_explicit(operator, hook, context, mocker):
    hook.get_run_state.return_value = SimpleNamespace(is_terminal=False)
    mocker.patch(
        "overture_airflow_provider._matching_databricks.time.monotonic", side_effect=[0, 50000]
    )
    with pytest.raises(AirflowException, match="timed out"):
        operator.execute(context)
    hook.find_job_id_by_name.return_value = None
    with pytest.raises(AirflowException, match="does not exist"):
        operator.execute(context)


def test_ambiguous_or_missing_output_task_fails(operator, hook, context):
    for tasks in (
        [],
        [{"task_key": "assign_1", "run_id": 1}, {"task_key": "assign_2", "run_id": 2}],
    ):
        hook.get_run.return_value = {"tasks": tasks}
        with pytest.raises(AirflowException, match="exactly one"):
            operator.execute(context)
