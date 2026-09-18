"""Tests for the pre-submit stale-run guard in _glue.py.

A cleared deferred ``execute_spark_job`` fires no callback and loses its XCom
before the next try starts, so the only record of the still-running Glue run
is Glue's own run list. ``find_active_glue_runs`` / ``stop_stale_glue_runs``
walk that list for runs stamped with this task instance's key and stop them
before a new run is submitted. All Glue calls are MagicMock'd here.
"""

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock, call

import pytest

from overture_airflow_provider import _glue
from overture_airflow_provider._airflow_compat import AirflowException

JOB = "test.Job"
KEY = "dag__grp.execute_spark_job__manual__2026-01-01__-1"
# Relative to the real clock: stop_stale_glue_runs scans with the real "now",
# so fixture runs must look recent to fall inside the lookback window.
NOW = datetime.now(UTC)


def _run(run_id, state="RUNNING", key=KEY, age=timedelta(minutes=5)):
    return {
        "Id": run_id,
        "JobRunState": state,
        "StartedOn": NOW - age,
        "Arguments": {_glue.TASK_INSTANCE_ARG: key} if key else {},
    }


def _client(pages, states=None):
    """Glue client whose get_job_runs pages through ``pages`` and whose
    get_job_run answers from ``states`` (run_id -> list of successive states)."""
    client = MagicMock()
    responses = []
    for i, runs in enumerate(pages):
        resp = {"JobRuns": runs}
        if i < len(pages) - 1:
            resp["NextToken"] = f"tok{i}"
        responses.append(resp)
    client.get_job_runs.side_effect = responses
    client.batch_stop_job_run.return_value = {"SuccessfulSubmissions": [], "Errors": []}

    states = {k: list(v) for k, v in (states or {}).items()}

    def _get_job_run(JobName, RunId):
        seq = states.get(RunId, ["STOPPED"])
        state = seq.pop(0) if len(seq) > 1 else seq[0]
        return {"JobRun": {"Id": RunId, "JobRunState": state}}

    client.get_job_run.side_effect = _get_job_run
    return client


class TestFindActiveGlueRuns:
    def test_matches_active_runs_with_this_key_only(self):
        client = _client(
            [
                [
                    _run("jr_mine_running"),
                    _run("jr_mine_waiting", state="WAITING"),
                    _run("jr_mine_stopping", state="STOPPING"),
                    _run("jr_mine_done", state="SUCCEEDED"),
                    _run("jr_mine_failed", state="FAILED"),
                    _run("jr_other", key="dag__other__run__-1"),
                    _run("jr_unstamped", key=None),
                ]
            ]
        )
        found = _glue.find_active_glue_runs(client, JOB, KEY, now=NOW)
        assert [r["Id"] for r in found] == [
            "jr_mine_running",
            "jr_mine_waiting",
            "jr_mine_stopping",
        ]

    def test_paginates_with_next_token(self):
        client = _client([[_run("jr_a")], [_run("jr_b")], [_run("jr_c", state="SUCCEEDED")]])
        found = _glue.find_active_glue_runs(client, JOB, KEY, now=NOW)
        assert [r["Id"] for r in found] == ["jr_a", "jr_b"]
        assert client.get_job_runs.call_args_list == [
            call(JobName=JOB, MaxResults=200),
            call(JobName=JOB, MaxResults=200, NextToken="tok0"),
            call(JobName=JOB, MaxResults=200, NextToken="tok1"),
        ]

    def test_stops_at_lookback_boundary(self):
        client = _client(
            [
                [_run("jr_recent"), _run("jr_ancient", age=timedelta(days=3))],
                [_run("jr_never_reached")],
            ]
        )
        found = _glue.find_active_glue_runs(client, JOB, KEY, now=NOW)
        assert [r["Id"] for r in found] == ["jr_recent"]
        assert client.get_job_runs.call_count == 1

    def test_respects_page_cap(self):
        client = MagicMock()
        client.get_job_runs.return_value = {"JobRuns": [_run("jr_x")], "NextToken": "more"}
        found = _glue.find_active_glue_runs(client, JOB, KEY, now=NOW, max_pages=3)
        assert client.get_job_runs.call_count == 3
        assert len(found) == 3

    def test_tolerates_missing_fields(self):
        client = _client([[{"Id": "jr_bare", "JobRunState": "RUNNING"}, {"Id": "jr_none"}]])
        assert _glue.find_active_glue_runs(client, JOB, KEY, now=NOW) == []

    def test_naive_started_on_is_treated_as_utc(self):
        client = _client(
            [[{**_run("jr_a"), "StartedOn": (NOW - timedelta(days=3)).replace(tzinfo=None)}]]
        )
        assert _glue.find_active_glue_runs(client, JOB, KEY, now=NOW) == []


class TestStopStaleGlueRuns:
    def test_noop_when_nothing_active(self):
        client = _client([[_run("jr_done", state="SUCCEEDED")]])
        assert _glue.stop_stale_glue_runs(client, JOB, KEY, sleep=MagicMock()) == []
        client.batch_stop_job_run.assert_not_called()

    def test_stops_and_waits_for_terminal_state(self):
        sleep = MagicMock()
        client = _client(
            [[_run("jr_1"), _run("jr_2", state="WAITING")]],
            states={"jr_1": ["STOPPING", "STOPPING", "STOPPED"], "jr_2": ["STOPPED"]},
        )
        stopped = _glue.stop_stale_glue_runs(
            client, JOB, KEY, sleep=sleep, timeout_seconds=600, poll_interval_seconds=7
        )
        assert stopped == ["jr_1", "jr_2"]
        client.batch_stop_job_run.assert_called_once_with(JobName=JOB, JobRunIds=["jr_1", "jr_2"])
        # jr_1 needed two extra polls; each waits poll_interval between them.
        assert sleep.call_args_list == [call(7), call(7)]

    def test_does_not_re_stop_a_run_already_stopping(self):
        client = _client([[_run("jr_stopping", state="STOPPING"), _run("jr_running")]])
        _glue.stop_stale_glue_runs(client, JOB, KEY, sleep=MagicMock())
        client.batch_stop_job_run.assert_called_once_with(JobName=JOB, JobRunIds=["jr_running"])

    def test_only_stopping_runs_still_waits_without_batch_stop(self):
        client = _client([[_run("jr_stopping", state="STOPPING")]])
        assert _glue.stop_stale_glue_runs(client, JOB, KEY, sleep=MagicMock()) == ["jr_stopping"]
        client.batch_stop_job_run.assert_not_called()
        client.get_job_run.assert_called()

    def test_raises_when_stop_is_rejected_for_a_still_active_run(self):
        client = _client([[_run("jr_1")]], states={"jr_1": ["RUNNING"]})
        client.batch_stop_job_run.return_value = {
            "SuccessfulSubmissions": [],
            "Errors": [
                {
                    "JobRunId": "jr_1",
                    "ErrorDetail": {"ErrorCode": "AccessDenied", "ErrorMessage": "nope"},
                }
            ],
        }
        with pytest.raises(AirflowException, match="Could not stop still-active Glue run jr_1"):
            _glue.stop_stale_glue_runs(client, JOB, KEY, sleep=MagicMock())

    def test_ignores_stop_error_for_a_run_that_finished_meanwhile(self):
        client = _client([[_run("jr_1")]], states={"jr_1": ["SUCCEEDED"]})
        client.batch_stop_job_run.return_value = {
            "SuccessfulSubmissions": [],
            "Errors": [{"JobRunId": "jr_1", "ErrorDetail": {"ErrorCode": "InvalidState"}}],
        }
        assert _glue.stop_stale_glue_runs(client, JOB, KEY, sleep=MagicMock()) == ["jr_1"]

    def test_raises_when_run_does_not_stop_in_time(self, monkeypatch):
        clock = iter([0.0, 0.0, 100.0, 100.0, 700.0, 700.0, 700.0])
        monkeypatch.setattr(_glue.time, "monotonic", lambda: next(clock))
        client = _client([[_run("jr_1")]], states={"jr_1": ["STOPPING"]})
        with pytest.raises(AirflowException, match="did not stop within 600s"):
            _glue.stop_stale_glue_runs(client, JOB, KEY, sleep=MagicMock(), timeout_seconds=600)


class TestBuildGlueOperatorKwargsMarker:
    def _build(self, module_name, task_instance_key):
        from overture_airflow_provider.spark import SparkImpl

        impl = SparkImpl.from_str("GLUE_v5")
        setup_info = {
            "job_name": JOB,
            "spark_impl": impl,
            "parameters": "{}",
            "aws_region": "us-east-1",
        }
        return _glue.build_glue_operator_kwargs(
            setup_info=setup_info,
            package_info={
                "script_location": "s3://b/glue.py",
                "scala_script_location": "s3://b/glue.scala",
                "s3_bucket": "b",
                "s3_prefix": "p",
                "py_files": "s3://b/pkg.whl",
            },
            jar_info={"jars_s3": "s3://b/a.jar", "sedona_packages": "x", "sedona_module": "m"},
            module_name=module_name,
            class_name="Main",
            extra_spark_conf={},
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iam_role_name="role",
            task_id="execute_spark_job",
            task_instance_key=task_instance_key,
        )

    def test_pyspark_script_args_carry_marker(self):
        built = self._build("my_module", KEY)
        assert built["script_args"][_glue.TASK_INSTANCE_ARG] == KEY
        assert _glue.TASK_INSTANCE_ARG not in built["create_job_kwargs"]["DefaultArguments"]

    def test_scala_script_args_carry_marker(self):
        built = self._build("", KEY)
        assert built["script_args"][_glue.TASK_INSTANCE_ARG] == KEY
        assert _glue.TASK_INSTANCE_ARG not in built["create_job_kwargs"]["DefaultArguments"]

    def test_marker_omitted_when_key_missing(self):
        # The Airflow-free render preview has no task instance to stamp.
        for module_name in ("my_module", ""):
            built = self._build(module_name, None)
            assert _glue.TASK_INSTANCE_ARG not in built["script_args"]
