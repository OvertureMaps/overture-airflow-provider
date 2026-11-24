"""Exercise real triggering, waiting, failure propagation and cleared-trigger reruns."""

import json
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest

from overture_airflow_provider.child_dag_runs import child_run_id

pytestmark = pytest.mark.e2e
CHILD = "provider_child_runs_child"


@pytest.fixture(scope="module")
def real_airflow(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("airflow")
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    env = {key: value for key, value in os.environ.items() if not key.startswith("AIRFLOW")}
    env.update(
        {
            "AIRFLOW_HOME": str(tmp_path),
            "AIRFLOW__CORE__DAGS_FOLDER": str(Path(__file__).with_name("dags")),
            "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
            "AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION": "False",
            "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
            "AIRFLOW__CORE__PARALLELISM": "2",
            "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": f"http://127.0.0.1:{port}/execution/",
            "AIRFLOW__API__BASE_URL": f"http://127.0.0.1:{port}",
            "AIRFLOW__API__HOST": "127.0.0.1",
            "AIRFLOW__API__PORT": str(port),
            "AIRFLOW__API__WORKERS": "1",
            "AIRFLOW__DAG_PROCESSOR__REFRESH_INTERVAL": "1",
            "AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC": "1",
            "PYTHONUNBUFFERED": "1",
        }
    )

    def cli(*args):
        result = subprocess.run(
            [sys.executable, "-m", "airflow", *args],
            env=env,
            capture_output=True,
            text=True,
            timeout=90,
        )
        assert result.returncode == 0, result.stdout + result.stderr

    def rows(query, parameters=()):
        with sqlite3.connect(tmp_path / "airflow.db", timeout=10) as connection:
            connection.row_factory = sqlite3.Row
            return [dict(row) for row in connection.execute(query, parameters)]

    cli("db", "migrate")
    cli("dags", "reserialize")
    log_path = tmp_path / "standalone.log"
    with log_path.open("w") as log:
        service = subprocess.Popen(
            [sys.executable, "-m", "airflow", "standalone"],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )

        def wait_for(check):
            deadline = time.monotonic() + 180
            while time.monotonic() < deadline:
                result = check()
                if result:
                    return result
                assert service.poll() is None, log_path.read_text()[-12000:]
                time.sleep(0.5)
            pytest.fail(f"Timed out waiting for Airflow:\n{log_path.read_text()[-12000:]}")

        try:
            wait_for(lambda: "Airflow is ready" in log_path.read_text())
            yield cli, rows, wait_for
        finally:
            service.send_signal(signal.SIGINT)
            try:
                service.wait(timeout=30)
            except subprocess.TimeoutExpired:
                service.kill()
                service.wait(timeout=10)


@pytest.mark.parametrize(
    "parent,waiting",
    [
        ("provider_child_runs_parent", "running"),
        ("provider_child_runs_parent_deferrable", "deferred"),
    ],
)
def test_real_child_runs_and_cleared_trigger_reruns(real_airflow, tmp_path, parent, waiting):
    cli, rows, wait_for = real_airflow
    release = tmp_path / "release"
    recovery = tmp_path / "recovery"

    def run_state(dag_id, run_id):
        runs = rows("SELECT state FROM dag_run WHERE dag_id = ? AND run_id = ?", (dag_id, run_id))
        return runs[0]["state"] if runs else None

    def task_state(dag_id, run_id, task_id):
        tasks = rows(
            "SELECT state FROM task_instance WHERE dag_id = ? AND run_id = ? AND task_id = ? "
            "ORDER BY try_number DESC LIMIT 1",
            (dag_id, run_id, task_id),
        )
        return tasks[0]["state"] if tasks else None

    for parent_run, fail, logical_date in (
        ("success", False, "2026-01-01T00:00:00+00:00"),
        ("failure", True, "2026-01-02T00:00:00+00:00"),
    ):
        conf = {
            "table": "division",
            "fail": fail,
            "release_file": str(release),
            "recovery_file": str(recovery),
        }
        expected_id = child_run_id(parent, parent_run, "load.trigger", 0, conf)
        assert expected_id.startswith(f"child__{parent}__load.trigger__0__")
        cli(
            "dags",
            "trigger",
            parent,
            "--run-id",
            parent_run,
            "--logical-date",
            logical_date,
            "--conf",
            json.dumps(conf),
        )
        if not fail:
            wait_for(lambda run_id=expected_id: task_state(CHILD, run_id, "load") == "running")
            wait_for(
                lambda run_id=parent_run: task_state(parent, run_id, "load.trigger") == waiting
            )
            assert task_state(parent, parent_run, "load.run_ids") is None
            release.touch()
        expected_state = "failed" if fail else "success"
        wait_for(lambda run_id=parent_run, state=expected_state: run_state(parent, run_id) == state)
        assert run_state(CHILD, expected_id) == expected_state
        assert task_state(parent, parent_run, "load.trigger") == expected_state
        assert task_state(parent, parent_run, "load.run_ids") == (
            "upstream_failed" if fail else "success"
        )
        before = rows(
            "SELECT MAX(try_number) AS tries FROM task_instance "
            "WHERE dag_id = ? AND run_id = ? AND task_id = 'load'",
            (CHILD, expected_id),
        )[0]["tries"]
        recovery.touch()
        # Airflow 3.3 treats the task selector as a substring, not a regular expression.
        cli(
            "tasks",
            "clear",
            parent,
            "--task-regex",
            "load.trigger",
            "--downstream",
            "--start-date",
            logical_date,
            "--end-date",
            logical_date,
            "--yes",
        )
        wait_for(
            lambda run_id=expected_id, tries=before: (
                rows(
                    "SELECT MAX(try_number) AS tries FROM task_instance "
                    "WHERE dag_id = ? AND run_id = ? AND task_id = 'load'",
                    (CHILD, run_id),
                )[0]["tries"]
                > tries
            )
        )
        wait_for(lambda run_id=parent_run: run_state(parent, run_id) == "success")
        assert run_state(CHILD, expected_id) == "success"
        assert task_state(parent, parent_run, "load.run_ids") == "success"
        recovery.unlink()

    assert {
        row["run_id"]
        for row in rows("SELECT run_id FROM dag_run WHERE dag_id = ?", (CHILD,))
        if row["run_id"].startswith(f"child__{parent}__")
    } == {
        child_run_id(
            parent,
            parent_run,
            "load.trigger",
            0,
            {
                "table": "division",
                "fail": fail,
                "release_file": str(release),
                "recovery_file": str(recovery),
            },
        )
        for parent_run, fail in (("success", False), ("failure", True))
    }
