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


def test_execute_wraps_submit_failure_with_classified_message():
    from overture_airflow_provider._airflow_compat import AirflowException
    from overture_airflow_provider._failures import SUBMIT_CONFIG, FailureInfo

    op = _make_operator()
    handler = MagicMock()
    handler.submit_job.side_effect = RuntimeError("CLUSTER_NOT_FOUND")
    handler.describe_failure.return_value = FailureInfo(
        platform="DATABRICKS",
        job_ref="metrics_job",
        state="FAILED",
        reason="CLUSTER_NOT_FOUND",
        classification=SUBMIT_CONFIG,
        hint="Cluster config: check the cluster id/policy.",
    )
    ti = MagicMock()
    ti.xcom_pull.return_value = None  # no early XCom -> job never launched
    context = {"ti": ti}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(AirflowException) as exc:
            op.execute(context)

    msg = str(exc.value)
    assert "Spark job FAILED on DATABRICKS" in msg
    assert "submit/config failure" in msg
    assert "hint:" in msg
    assert handler.describe_failure.call_args.kwargs["run_launched"] is False


def test_execute_submit_failure_marks_downstream_when_launched():
    from overture_airflow_provider._airflow_compat import AirflowException
    from overture_airflow_provider._failures import DOWNSTREAM_JOB, FailureInfo

    op = _make_operator()
    handler = MagicMock()
    handler.submit_job.side_effect = RuntimeError("job blew up")
    handler.describe_failure.return_value = FailureInfo(
        platform="WHEROBOTS",
        job_ref="metrics_job",
        state="FAILED",
        classification=DOWNSTREAM_JOB,
    )
    ti = MagicMock()
    ti.xcom_pull.return_value = '{"job_url": "https://wherobots/run/1"}'  # early XCom present
    context = {"ti": ti}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(AirflowException):
            op.execute(context)

    assert handler.describe_failure.call_args.kwargs["run_launched"] is True


def test_resume_execution_enriches_trigger_failure():
    from overture_airflow_provider._airflow_compat import AirflowException
    from overture_airflow_provider._failures import TRIGGER_POLLING, FailureInfo

    op = _make_operator()
    handler = MagicMock()
    handler.describe_failure.return_value = FailureInfo(
        platform="GLUE",
        job_ref="metrics_job",
        state="FAILED",
        reason="Triggerer lost connection",
        classification=TRIGGER_POLLING,
    )

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(AirflowException) as exc:
            op.resume_execution(
                "__fail__",
                {"error": "Triggerer lost connection", "traceback": ["line1", "line2"]},
                {"ti": MagicMock()},
            )

    msg = str(exc.value)
    assert "Spark job FAILED on GLUE" in msg
    assert "trigger/polling failure" in msg
    kwargs = handler.describe_failure.call_args.kwargs
    assert kwargs["is_trigger_failure"] is True
    assert kwargs["run_launched"] is True


def test_execute_complete_calls_handler_and_finalizes():
    op = _make_operator()
    handler = MagicMock()
    handler.complete_job.return_value = {"job_url": "https://glue/run/1", "status": "SUCCEEDED"}
    context = {"ti": MagicMock()}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        result = op.execute_complete(context, event={"state": "SUCCEEDED"})

    handler.complete_job.assert_called_once()
    assert result["status"] == "SUCCEEDED"
    assert result["spark_impl"] == "GLUE_SEDONA"


def test_resume_execution_delegates_normal_event():
    op = _make_operator()
    op.execute_complete = MagicMock(return_value="finalized")
    event = {"status": "SUCCESS"}

    result = op.resume_execution("execute_complete", {"event": event}, {"ti": MagicMock()})

    assert result == "finalized"
    op.execute_complete.assert_called_once()
    assert op.execute_complete.call_args.kwargs["event"] == event


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


def test_report_issue_link_attached_when_configured():
    from overture_airflow_provider.links import ReportIssueLink

    op = SparkAgnosticExecuteOperator(
        task_id="execute_spark_job",
        setup_info=_SETUP_INFO,
        report_issue_config={"provider": "github", "target": "owner/repo", "labels": []},
    )
    assert any(isinstance(link, ReportIssueLink) for link in op.operator_extra_links)


def test_report_issue_link_absent_by_default():
    from overture_airflow_provider.links import ReportIssueLink

    op = _make_operator()
    assert not any(isinstance(link, ReportIssueLink) for link in op.operator_extra_links)


def test_execute_pushes_report_issue_config_early():
    op = SparkAgnosticExecuteOperator(
        task_id="execute_spark_job",
        setup_info=_SETUP_INFO,
        cluster_info={"merged_spark_conf": {}},
        report_issue_config={"provider": "github", "target": "owner/repo", "labels": ["bug"]},
    )
    handler = MagicMock()
    handler.submit_job.return_value = {"trigger": MagicMock(), "run_id": "jr_1"}
    ti = MagicMock()
    context = {"ti": ti}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(TaskDeferred):
            op.execute(context)

    report_calls = [c for c in ti.xcom_push.call_args_list if c.kwargs.get("key") == "report_issue"]
    assert report_calls
    pushed = json.loads(report_calls[0].kwargs["value"])
    assert pushed["target"] == "owner/repo"
    assert pushed["provider"] == "github"


def test_execute_skips_report_issue_push_when_unconfigured():
    op = _make_operator()
    handler = MagicMock()
    handler.submit_job.return_value = {
        "trigger": None,
        "result": {"job_url": "https://wherobots/run/1", "status": "SUCCESS"},
    }
    ti = MagicMock()
    context = {"ti": ti}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        op.execute(context)

    report_calls = [c for c in ti.xcom_push.call_args_list if c.kwargs.get("key") == "report_issue"]
    assert not report_calls
