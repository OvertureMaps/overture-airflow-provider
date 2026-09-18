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


def _ti(dag_id="dag", task_id="grp.execute_spark_job", run_id="manual__2026-01-01", map_index=-1):
    ti = MagicMock()
    ti.dag_id = dag_id
    ti.task_id = task_id
    ti.run_id = run_id
    ti.map_index = map_index
    return ti


class TestTaskInstanceKey:
    def test_joins_dag_task_run_and_map_index(self):
        key = _retry_guard.task_instance_key({"ti": _ti()})
        assert key == "dag__grp.execute_spark_job__manual__2026-01-01__-1"

    def test_mapped_index_distinguishes_expansions(self):
        first = _retry_guard.task_instance_key({"ti": _ti(map_index=0)})
        second = _retry_guard.task_instance_key({"ti": _ti(map_index=1)})
        assert first != second
        assert first.endswith("__0") and second.endswith("__1")

    def test_excludes_try_number(self):
        ti = _ti()
        ti.try_number = 1
        first = _retry_guard.task_instance_key({"ti": ti})
        ti.try_number = 2
        assert _retry_guard.task_instance_key({"ti": ti}) == first

    def test_falls_back_to_dag_run_for_run_id(self):
        ti = _ti()
        ti.run_id = None
        dag_run = MagicMock()
        dag_run.run_id = "scheduled__2026-01-02"
        key = _retry_guard.task_instance_key({"ti": ti, "dag_run": dag_run})
        assert key == "dag__grp.execute_spark_job__scheduled__2026-01-02__-1"

    def test_missing_map_index_defaults_to_unmapped(self):
        ti = _ti()
        ti.map_index = None
        assert _retry_guard.task_instance_key({"ti": ti}).endswith("__-1")

    def test_works_with_non_dict_mapping_context(self):
        from collections.abc import Mapping

        class _Ctx(Mapping):
            def __init__(self, data):
                self._d = dict(data)

            def __getitem__(self, k):
                return self._d[k]

            def __iter__(self):
                return iter(self._d)

            def __len__(self):
                return len(self._d)

        assert _retry_guard.task_instance_key(_Ctx({"ti": _ti()})) is not None

    def test_none_without_ti(self):
        assert _retry_guard.task_instance_key({}) is None
        assert _retry_guard.task_instance_key(object()) is None

    def test_none_when_identity_is_not_strings(self):
        # A bare MagicMock ti (as most tests use) has MagicMock attributes, not
        # strings; the guard must opt out rather than stamp a garbage marker.
        assert _retry_guard.task_instance_key({"ti": MagicMock()}) is None

    def test_never_raises(self):
        class _Explodes:
            def __getattr__(self, name):
                raise RuntimeError("boom")

        assert _retry_guard.task_instance_key({"ti": _Explodes()}) is None
