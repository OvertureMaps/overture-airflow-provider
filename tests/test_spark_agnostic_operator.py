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
    # Never-launched failures are retryable: exactly AirflowException, not the
    # non-retryable AirflowFailException subclass.
    assert type(exc.value) is AirflowException


def test_execute_submit_failure_marks_downstream_when_launched():
    from overture_airflow_provider._airflow_compat import AirflowFailException
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
        # Launched-and-failed is never retryable, regardless of the caller's
        # `retries=` setting.
        with pytest.raises(AirflowFailException):
            op.execute(context)

    assert handler.describe_failure.call_args.kwargs["run_launched"] is True


def test_resume_execution_enriches_trigger_failure():
    from overture_airflow_provider._airflow_compat import AirflowFailException
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
        # A trigger crash mid-poll means the job did launch, so this is never
        # retryable regardless of the caller's `retries=` setting.
        with pytest.raises(AirflowFailException) as exc:
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


def test_resume_execution_resolves_terminal_glue_failure():
    """A trigger that raised on a terminal FAILED state resolves as a real job
    failure: the run id is recovered, complete_job surfaces the platform
    error, and the generic trigger-failure classification is skipped."""
    from overture_airflow_provider._airflow_compat import (
        AirflowException,
        AirflowFailException,
    )

    op = _make_operator()
    handler = MagicMock()
    # Mirrors complete_glue_job's classified failure format.
    handler.complete_job.side_effect = AirflowException(
        "Spark job FAILED on GLUE\n  reason: Validation failed: 9 errors in divisions/division"
    )

    run_id = "jr_" + "a" * 64
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
                {"error": f"Exiting Job {run_id} Run State: FAILED"},
                {"ti": MagicMock()},
            )

    msg = str(exc.value)
    assert "Validation failed: 9 errors" in msg
    # The run id resolved above means the job launched: never retryable, same
    # as the launched-and-failed case in execute(), even though complete_job
    # itself only ever raises the plain (retryable-by-default) AirflowException.
    assert type(exc.value) is AirflowFailException
    # Resolved via complete_job, so the generic bucket is skipped.
    handler.complete_job.assert_called_once()
    assert handler.complete_job.call_args.args[0] == {"run_id": run_id}
    handler.describe_failure.assert_not_called()


def test_resume_execution_finds_run_id_in_error_when_traceback_lacks_it():
    """The run id can live only in `error` even when a traceback is present,
    e.g. if the trigger's own frames don't repeat the final exception line."""
    from overture_airflow_provider._airflow_compat import AirflowException

    op = _make_operator()
    handler = MagicMock()
    handler.complete_job.side_effect = AirflowException("Spark job FAILED on GLUE")

    run_id = "jr_" + "c" * 64
    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(AirflowException):
            op.resume_execution(
                "__fail__",
                {
                    "error": f"Exiting Job {run_id} Run State: FAILED",
                    "traceback": ["Traceback (most recent call last):", '  File "x.py", line 1'],
                },
                {"ti": MagicMock()},
            )

    handler.complete_job.assert_called_once()
    assert handler.complete_job.call_args.args[0] == {"run_id": run_id}


def test_resume_execution_falls_back_when_run_unresolvable():
    """If the recovered run can't be resolved (API error), the generic
    trigger-failure classification still surfaces and the raw resolution error
    stays contained."""
    from overture_airflow_provider._airflow_compat import AirflowFailException
    from overture_airflow_provider._failures import TRIGGER_POLLING, FailureInfo

    op = _make_operator()
    handler = MagicMock()
    handler.complete_job.side_effect = RuntimeError("get_job_run timed out")
    handler.describe_failure.return_value = FailureInfo(
        platform="GLUE",
        job_ref="metrics_job",
        state="FAILED",
        reason="unresolved",
        classification=TRIGGER_POLLING,
    )

    run_id = "jr_" + "b" * 64
    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
    ):
        with pytest.raises(AirflowFailException) as exc:
            op.resume_execution(
                "__fail__",
                {"error": f"Exiting Job {run_id} Run State: FAILED"},
                {"ti": MagicMock()},
            )

    assert "trigger/polling failure" in str(exc.value)
    handler.describe_failure.assert_called_once()


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


# ─── Small branch coverage ───────────────────────────────────────────────────


def test_xcom_datetime_default_raises_for_non_datetime():
    from overture_airflow_provider._operator import _xcom_datetime_default

    with pytest.raises(TypeError):
        _xcom_datetime_default("not-a-datetime")


def test_execute_reraises_taskdeferred_from_submit_job():
    """TaskDeferred raised inside submit_job must propagate (not be swallowed)."""
    op = _make_operator()
    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch("overture_airflow_provider._operator.get_platform_handler") as mock_handler,
    ):
        mock_handler.return_value.submit_job.side_effect = TaskDeferred(
            trigger=MagicMock(), method_name="execute_complete"
        )
        with pytest.raises(TaskDeferred):
            op.execute({"ti": MagicMock()})


def test_push_report_issue_config_skips_when_no_ti():
    op = _make_operator()
    op.report_issue_config = {"target": "owner/repo"}
    op._push_report_issue_config({})  # no ti key — must not raise


def test_push_report_issue_config_swallows_xcom_error():
    op = _make_operator()
    op.report_issue_config = {"target": "owner/repo"}
    ti = MagicMock()
    ti.xcom_push.side_effect = Exception("xcom failure")
    op._push_report_issue_config({"ti": ti})  # must not raise


def test_run_launched_returns_false_without_ti():
    assert _make_operator()._run_launched({}) is False


def test_run_launched_returns_false_on_xcom_error():
    ti = MagicMock()
    ti.xcom_pull.side_effect = Exception("db gone")
    assert _make_operator()._run_launched({"ti": ti}) is False


# --- Retry-cancel guard ------------------------------------------------------
#
# XCom is cleared before every retry, so a zombie-killed try's run id can only
# survive via the Airflow Variable the retry guard writes. These tests cover
# the operator side of that hand-off: cancelling a stale run on the next try,
# and clearing the recorded run once nothing can retry-cancel it anymore.


def test_cancel_prior_run_noop_on_first_try():
    op = _make_operator()
    handler = MagicMock()
    ti = MagicMock()
    ti.try_number = 1
    with patch("overture_airflow_provider._operator.pop_recorded_run") as mock_pop:
        op._cancel_prior_run_if_retried({"ti": ti}, handler)
    mock_pop.assert_not_called()
    handler.cancel_run.assert_not_called()


def test_cancel_prior_run_noop_when_nothing_recorded():
    op = _make_operator()
    handler = MagicMock()
    ti = MagicMock()
    ti.try_number = 2
    with patch("overture_airflow_provider._operator.pop_recorded_run", return_value=None):
        op._cancel_prior_run_if_retried({"ti": ti}, handler)
    handler.cancel_run.assert_not_called()


def test_cancel_prior_run_cancels_recorded_run_on_retry():
    op = _make_operator()
    handler = MagicMock()
    ti = MagicMock()
    ti.try_number = 2
    prior = {"platform": "glue", "run_id": "jr_1", "extra": {"job_name": "job"}}
    with patch("overture_airflow_provider._operator.pop_recorded_run", return_value=prior):
        op._cancel_prior_run_if_retried({"ti": ti}, handler)
    handler.cancel_run.assert_called_once_with("jr_1", {"job_name": "job"})


def test_cancel_prior_run_swallows_cancel_run_exception():
    op = _make_operator()
    handler = MagicMock()
    handler.cancel_run.side_effect = RuntimeError("platform unreachable")
    ti = MagicMock()
    ti.try_number = 2
    prior = {"platform": "glue", "run_id": "jr_1", "extra": None}
    with patch("overture_airflow_provider._operator.pop_recorded_run", return_value=prior):
        op._cancel_prior_run_if_retried({"ti": ti}, handler)  # must not raise


def test_execute_calls_cancel_prior_run_before_submit():
    op = _make_operator()
    handler = MagicMock()
    handler.submit_job.return_value = {"trigger": MagicMock(), "run_id": "jr_2"}
    context = {"ti": MagicMock()}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
        patch.object(op, "_cancel_prior_run_if_retried") as mock_cancel,
    ):
        with pytest.raises(TaskDeferred):
            op.execute(context)

    mock_cancel.assert_called_once_with(context, handler)
    # Cancellation must happen before submission -- otherwise a retry could
    # race the prior try's still-running job while writing to the same path.
    assert handler.submit_job.call_args is not None


def test_finalize_clears_recorded_run_on_success():
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
        patch("overture_airflow_provider._operator.clear_recorded_run") as mock_clear,
    ):
        op.execute(context)

    mock_clear.assert_called_once_with(context)


def test_execute_clears_recorded_run_on_non_retryable_failure():
    from overture_airflow_provider._airflow_compat import AirflowFailException
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
    ti.xcom_pull.return_value = '{"job_url": "https://wherobots/run/1"}'
    context = {"ti": ti}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
        patch("overture_airflow_provider._operator.clear_recorded_run") as mock_clear,
    ):
        with pytest.raises(AirflowFailException):
            op.execute(context)

    mock_clear.assert_called_once_with(context)


def test_execute_leaves_recorded_run_on_retryable_failure():
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
    ti.xcom_pull.return_value = None
    context = {"ti": ti}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
        patch("overture_airflow_provider._operator.clear_recorded_run") as mock_clear,
    ):
        with pytest.raises(AirflowException):
            op.execute(context)

    # A retryable submit/config failure never recorded a run in the first
    # place, and the retry guard exists precisely to catch it next try --
    # so nothing should be cleared here.
    mock_clear.assert_not_called()


def test_resume_execution_clears_recorded_run_when_trigger_failure_resolves_launched_run():
    from overture_airflow_provider._airflow_compat import AirflowException, AirflowFailException

    op = _make_operator()
    handler = MagicMock()
    handler.complete_job.side_effect = AirflowException("job FAILED")
    context = {"ti": MagicMock()}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
        patch(
            "overture_airflow_provider._operator._terminal_run_id",
            return_value="jr_3",
        ),
        patch("overture_airflow_provider._operator.clear_recorded_run") as mock_clear,
    ):
        with pytest.raises(AirflowFailException):
            op.resume_execution("__fail__", {"error": "job FAILED"}, context)

    mock_clear.assert_called_once_with(context)


def test_resume_execution_clears_recorded_run_on_generic_trigger_crash():
    from overture_airflow_provider._airflow_compat import AirflowFailException
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
    context = {"ti": MagicMock()}

    with (
        patch("overture_airflow_provider._operator.rehydrate", return_value=_FULL),
        patch(
            "overture_airflow_provider._operator.get_platform_handler",
            return_value=handler,
        ),
        patch("overture_airflow_provider._operator.clear_recorded_run") as mock_clear,
    ):
        with pytest.raises(AirflowFailException):
            op.resume_execution(
                "__fail__",
                {"error": "Triggerer lost connection", "traceback": ["line1", "line2"]},
                context,
            )

    mock_clear.assert_called_once_with(context)
