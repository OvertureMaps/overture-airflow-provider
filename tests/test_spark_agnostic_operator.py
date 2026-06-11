"""Tests for SparkAgnosticExecuteOperator (deferrable execution)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from overture_airflow_provider._airflow_compat import TaskDeferred
from overture_airflow_provider._operator import SparkAgnosticExecuteOperator
from overture_airflow_provider.spark import SparkFamily

_SETUP_INFO = {
    "spark_impl_name": "GLUE_SEDONA",
    "spark_family_name": "GLUE",
    "spark_version": "3.5",
    "sedona_version": "1.7.0",
}

_FULL = {**_SETUP_INFO, "spark_family": SparkFamily.GLUE}


def _make_operator():
    return SparkAgnosticExecuteOperator(
        task_id="execute_spark_job",
        setup_info=_SETUP_INFO,
        package_info={},
        jar_info={},
        cluster_info={"merged_spark_conf": {}},
        module_name="my_module",
        class_name="MyClass",
        parameters="{}",
    )


def test_execute_defers_when_trigger_returned():
    op = _make_operator()
    trigger = MagicMock(name="trigger")
    handler = MagicMock()
    handler.submit_job.return_value = {"trigger": trigger, "run_id": "jr_1"}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(TaskDeferred) as exc:
            op.execute({"ti": MagicMock()})

    assert exc.value.trigger is trigger
    assert exc.value.method_name == "execute_complete"


def test_execute_returns_synchronously_for_wherobots():
    op = _make_operator()
    handler = MagicMock()
    handler.submit_job.return_value = {
        "trigger": None,
        "result": {"job_url": "https://wherobots/run/1", "status": "SUCCESS"},
    }
    context = {"ti": MagicMock()}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        result = op.execute(context)

    assert result["job_url"] == "https://wherobots/run/1"
    assert result["spark_impl"] == "GLUE_SEDONA"
    spark_agnostic_calls = [
        c for c in context["ti"].xcom_push.call_args_list if c.kwargs.get("key") == "spark_agnostic"
    ]
    assert spark_agnostic_calls


def test_execute_complete_finalizes_and_pushes_xcom():
    op = _make_operator()
    handler = MagicMock()
    handler.complete_job.return_value = {
        "job_url": "https://glue/run/jr_1",
        "status": {"JobRunState": "SUCCEEDED"},
    }
    context = {"ti": MagicMock()}
    event = {"status": "success", "run_id": "jr_1"}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        result = op.execute_complete(context, event)

    handler.complete_job.assert_called_once()
    assert result["job_url"] == "https://glue/run/jr_1"
    assert result["spark_family"] == "GLUE"
    spark_agnostic_calls = [
        c for c in context["ti"].xcom_push.call_args_list if c.kwargs.get("key") == "spark_agnostic"
    ]
    assert spark_agnostic_calls
    payload = json.loads(spark_agnostic_calls[0].kwargs["value"])
    assert payload["job_url"] == "https://glue/run/jr_1"
