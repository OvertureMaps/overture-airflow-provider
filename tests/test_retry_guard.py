"""Tests for the retry-cancel guard (_retry_guard.py).

Airflow clears XCom before every retry, so the guard hands a launched run id
off through an Airflow Variable instead. These tests exercise that hand-off
in isolation, with ``Variable`` mocked out (never touches a real Airflow
metadata store).
"""

import json
from unittest.mock import MagicMock, patch

from overture_airflow_provider import _retry_guard


def _context(dag_id="test_dag", task_id="execute_spark_job", run_id="manual__1", map_index=-1):
    ti = MagicMock()
    ti.dag_id = dag_id
    ti.task_id = task_id
    ti.run_id = run_id
    ti.map_index = map_index
    return {"ti": ti}


class TestVariableKey:
    def test_missing_ti_yields_no_key(self):
        assert _retry_guard._variable_key({}) is None

    def test_stable_across_calls_for_same_task_instance(self):
        context = _context()
        assert _retry_guard._variable_key(context) == _retry_guard._variable_key(context)

    def test_differs_for_different_task_instances(self):
        one = _retry_guard._variable_key(_context(task_id="task_a"))
        two = _retry_guard._variable_key(_context(task_id="task_b"))
        assert one != two


class TestRecordLaunchedRun:
    def test_sets_variable_with_platform_and_run_id(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            _retry_guard.record_launched_run(
                context, platform="glue", run_id="jr_123", extra={"job_name": "job"}
            )
        key, value = mock_variable.set.call_args[0]
        payload = json.loads(value)
        assert payload == {"platform": "glue", "run_id": "jr_123", "extra": {"job_name": "job"}}

    def test_noop_when_run_id_missing(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            _retry_guard.record_launched_run(context, platform="glue", run_id="")
        mock_variable.set.assert_not_called()

    def test_never_raises_when_variable_backend_fails(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            mock_variable.set.side_effect = RuntimeError("no metadata db")
            _retry_guard.record_launched_run(context, platform="glue", run_id="jr_123")


class TestPopRecordedRun:
    def test_returns_and_clears_recorded_run(self):
        context = _context()
        payload = {"platform": "wherobots", "run_id": "wb_1", "extra": {}}
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            mock_variable.get.return_value = json.dumps(payload)
            result = _retry_guard.pop_recorded_run(context)
            mock_variable.delete.assert_called_once()
        assert result == payload

    def test_returns_none_when_nothing_recorded(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            mock_variable.get.return_value = None
            result = _retry_guard.pop_recorded_run(context)
        assert result is None

    def test_returns_none_on_unparseable_content(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            mock_variable.get.return_value = "not json"
            result = _retry_guard.pop_recorded_run(context)
        assert result is None

    def test_never_raises_when_variable_backend_fails(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            mock_variable.get.side_effect = RuntimeError("no metadata db")
            assert _retry_guard.pop_recorded_run(context) is None


class TestClearRecordedRun:
    def test_deletes_variable(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            _retry_guard.clear_recorded_run(context)
        mock_variable.delete.assert_called_once()

    def test_never_raises_when_already_absent(self):
        context = _context()
        with patch("overture_airflow_provider._retry_guard.Variable") as mock_variable:
            mock_variable.delete.side_effect = KeyError("missing")
            _retry_guard.clear_recorded_run(context)
