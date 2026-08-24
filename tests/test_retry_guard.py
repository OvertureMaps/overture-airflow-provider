"""Tests for the retry-cancel guard (_retry_guard.py).

Airflow only clears a task instance's XCom right before its *next* run
starts, not at failure time, so this module's XCom hand-off survives long
enough for an ``on_failure_callback`` to read it -- even for a zombie kill,
where the callback runs from the scheduler/DAG-processor rather than the
dead worker. These tests exercise that hand-off against a ``MagicMock`` ti,
with no real Airflow XCom backend involved.
"""

import json
from unittest.mock import MagicMock

from overture_airflow_provider import _retry_guard


def _context():
    return {"ti": MagicMock()}


class TestRecordLaunchedRun:
    def test_pushes_xcom_with_platform_and_run_id(self):
        context = _context()
        _retry_guard.record_launched_run(
            context, platform="glue", run_id="jr_123", extra={"job_name": "job"}
        )
        call = context["ti"].xcom_push.call_args
        assert call.kwargs["key"] == _retry_guard._XCOM_KEY
        payload = json.loads(call.kwargs["value"])
        assert payload == {"platform": "glue", "run_id": "jr_123", "extra": {"job_name": "job"}}

    def test_noop_when_run_id_missing(self):
        context = _context()
        _retry_guard.record_launched_run(context, platform="glue", run_id="")
        context["ti"].xcom_push.assert_not_called()

    def test_noop_without_ti(self):
        _retry_guard.record_launched_run({}, platform="glue", run_id="jr_123")  # must not raise

    def test_never_raises_when_xcom_push_fails(self):
        context = _context()
        context["ti"].xcom_push.side_effect = RuntimeError("no metadata db")
        _retry_guard.record_launched_run(context, platform="glue", run_id="jr_123")


class TestReadRecordedRun:
    def test_returns_recorded_run(self):
        context = _context()
        payload = {"platform": "wherobots", "run_id": "wb_1", "extra": {}}
        context["ti"].xcom_pull.return_value = json.dumps(payload)
        result = _retry_guard.read_recorded_run(context)
        assert result == payload
        context["ti"].xcom_pull.assert_called_once_with(key=_retry_guard._XCOM_KEY)

    def test_returns_none_when_nothing_recorded(self):
        context = _context()
        context["ti"].xcom_pull.return_value = None
        assert _retry_guard.read_recorded_run(context) is None

    def test_returns_none_on_unparseable_content(self):
        context = _context()
        context["ti"].xcom_pull.return_value = "not json"
        assert _retry_guard.read_recorded_run(context) is None

    def test_returns_none_without_ti(self):
        assert _retry_guard.read_recorded_run({}) is None

    def test_never_raises_when_xcom_pull_fails(self):
        context = _context()
        context["ti"].xcom_pull.side_effect = RuntimeError("no metadata db")
        assert _retry_guard.read_recorded_run(context) is None
