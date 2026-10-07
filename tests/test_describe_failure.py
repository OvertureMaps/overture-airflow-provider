"""Tests for ``describe_failure`` on each platform handler."""

from types import SimpleNamespace

from overture_airflow_provider._failures import (
    CANCELLED,
    DOWNSTREAM_JOB,
    FAILED,
    INTERNAL_ERROR,
    PLATFORM_CAPACITY,
    PLATFORM_GLUE,
    PLATFORM_INFRA,
    PLATFORM_WHEROBOTS,
    SUBMIT_CONFIG,
    TIMEOUT,
    TRIGGER_POLLING,
)
from overture_airflow_provider.spark import SparkFamily
from overture_airflow_provider.spark_platform_handlers import (
    DatabricksPlatformHandler,
    GluePlatformHandler,
    WherobotsPlatformHandler,
)


def _glue(job_name="metrics_job"):
    return GluePlatformHandler(
        {"spark_family": SparkFamily.GLUE, "job_name": job_name, "aws_region": "us-east-1"}
    )


def _databricks(job_name="metrics_job"):
    return DatabricksPlatformHandler({"spark_family": SparkFamily.DATABRICKS, "job_name": job_name})


def _wherobots(job_name="metrics_job"):
    return WherobotsPlatformHandler({"spark_family": SparkFamily.WHEROBOTS, "job_name": job_name})


class TestGlueDescribeFailure:
    def test_extracts_error_message_and_classifies_downstream(self):
        info = _glue().describe_failure(
            payload={"JobRunState": "FAILED", "ErrorMessage": "S3 AccessDenied on DeleteObject"},
            run_id="jr_1",
            run_launched=True,
            console_url="https://console/run",
        )
        assert info.platform == PLATFORM_GLUE
        assert info.job_ref == "metrics_job"
        assert info.run_id == "jr_1"
        assert info.state == FAILED
        assert info.reason == "S3 AccessDenied on DeleteObject"
        assert info.classification == DOWNSTREAM_JOB
        assert info.hint.startswith("IAM:")
        assert info.console_url == "https://console/run"

    def test_timeout_state_normalized(self):
        info = _glue().describe_failure(payload={"JobRunState": "TIMEOUT"}, run_id="jr_2")
        assert info.state == TIMEOUT

    def test_stopped_state_is_cancelled(self):
        info = _glue().describe_failure(payload={"JobRunState": "STOPPED"})
        assert info.state == CANCELLED

    def test_unlaunched_run_is_submit_config(self):
        info = _glue().describe_failure(
            error=RuntimeError("boom"), payload=None, run_launched=False
        )
        assert info.classification == SUBMIT_CONFIG
        assert info.state == FAILED
        assert info.reason == "boom"

    def test_missing_job_name_falls_back(self):
        info = _glue(job_name="").describe_failure(payload={"JobRunState": "FAILED"})
        assert info.job_ref == "<unknown>"


_CAPACITY_MSG = "Exceeded maximum concurrent compute"


class TestGlueCapacityNeverStarted:
    """A queued Glue run the service failed for capacity before it ran (prod:
    ``ErrorMessage: "Exceeded maximum concurrent compute"``, ``ExecutionTime: 0``)
    classifies as ``platform/capacity``; a run that did execute never does."""

    def test_zero_execution_time_with_capacity_message(self):
        info = _glue().describe_failure(
            payload={"JobRunState": "FAILED", "ErrorMessage": _CAPACITY_MSG, "ExecutionTime": 0},
            run_id="jr_cap",
            run_launched=True,
        )
        assert info.classification == PLATFORM_CAPACITY
        assert info.state == FAILED
        assert info.reason == _CAPACITY_MSG
        assert info.hint.startswith("Glue capacity:")

    def test_missing_execution_time_counts_as_never_started(self):
        info = _glue().describe_failure(
            payload={"JobRunState": "FAILED", "ErrorMessage": _CAPACITY_MSG},
            run_launched=True,
        )
        assert info.classification == PLATFORM_CAPACITY

    def test_positive_execution_time_is_downstream_even_with_capacity_message(self):
        # The job started, so whatever it wrote is suspect: not retryable.
        info = _glue().describe_failure(
            payload={"JobRunState": "FAILED", "ErrorMessage": _CAPACITY_MSG, "ExecutionTime": 42},
            run_launched=True,
        )
        assert info.classification == DOWNSTREAM_JOB
        # The capacity hint still applies as a diagnostic.
        assert info.hint.startswith("Glue capacity:")

    def test_zero_execution_time_with_other_message_is_downstream(self):
        info = _glue().describe_failure(
            payload={
                "JobRunState": "FAILED",
                "ErrorMessage": "Job run failed with exit code 1",
                "ExecutionTime": 0,
            },
            run_launched=True,
        )
        assert info.classification == DOWNSTREAM_JOB

    def test_trigger_failure_still_wins(self):
        info = _glue().describe_failure(
            payload={"JobRunState": "FAILED", "ErrorMessage": _CAPACITY_MSG, "ExecutionTime": 0},
            run_launched=True,
            is_trigger_failure=True,
        )
        assert info.classification == TRIGGER_POLLING


class TestDatabricksDescribeFailure:
    def test_parses_json_run_state_and_errors(self):
        info = _databricks().describe_failure(
            payload={
                "run_state": (
                    '{"life_cycle_state": "TERMINATED", "result_state": "FAILED",'
                    ' "state_message": "task failed"}'
                ),
                "errors": [{"code": "CLUSTER_NOT_FOUND"}],
                "run_page_url": "https://dbx/run/1",
                "run_id": "run-1",
            },
            run_launched=True,
        )
        assert info.state == FAILED
        assert info.reason == "task failed"
        assert info.run_id == "run-1"
        assert info.console_url == "https://dbx/run/1"
        assert "CLUSTER_NOT_FOUND" in info.root_cause
        assert info.classification == DOWNSTREAM_JOB
        assert info.hint.startswith("Cluster config:")

    def test_internal_error_is_platform_infra(self):
        info = _databricks().describe_failure(
            payload={
                "run_state": {
                    "life_cycle_state": INTERNAL_ERROR,
                    "result_state": "FAILED",
                    "state_message": "infra blip",
                }
            },
            run_launched=True,
        )
        assert info.state == INTERNAL_ERROR
        assert info.classification == PLATFORM_INFRA

    def test_run_state_object_shape(self):
        rs = SimpleNamespace(
            life_cycle_state="TERMINATED", result_state="TIMEDOUT", state_message="slow"
        )
        info = _databricks().describe_failure(payload={"run_state": rs})
        assert info.state == TIMEOUT
        assert info.reason == "slow"

    def test_canceled_result_state(self):
        info = _databricks().describe_failure(
            payload={"run_state": {"life_cycle_state": "TERMINATED", "result_state": "CANCELED"}}
        )
        assert info.state == CANCELLED

    def test_trigger_failure_classification(self):
        info = _databricks().describe_failure(
            error=RuntimeError("trigger crashed"),
            run_id="run-9",
            is_trigger_failure=True,
        )
        assert info.classification == TRIGGER_POLLING
        assert info.reason == "trigger crashed"


class TestWherobotsDescribeFailure:
    def test_extracts_status_and_oom_hint(self):
        info = _wherobots().describe_failure(
            payload={"status": "FAILED", "error": "java.lang.OutOfMemoryError: heap"},
            run_id="wr-1",
            run_launched=True,
        )
        assert info.platform == PLATFORM_WHEROBOTS
        assert info.state == FAILED
        assert info.classification == DOWNSTREAM_JOB
        assert info.hint.startswith("OOM:")

    def test_error_fallback_when_no_payload(self):
        info = _wherobots().describe_failure(error=RuntimeError("kaboom"))
        assert info.reason == "kaboom"
        assert info.state == FAILED
