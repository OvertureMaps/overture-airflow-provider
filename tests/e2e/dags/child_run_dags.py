"""Credential-free parent and child DAGs for real trigger integration tests."""

import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

from overture_airflow_provider import child_dag_run_task_group
from overture_airflow_provider._airflow_compat import DAG, task


@task
def requests(dag_run=None):
    return [dag_run.conf]


@task
def load(dag_run=None):
    conf = dag_run.conf
    if conf["fail"] and not Path(conf["recovery_file"]).exists():
        raise ValueError("simulated child load failure")
    deadline = time.monotonic() + 60
    while not Path(conf["release_file"]).exists():
        if time.monotonic() >= deadline:
            raise TimeoutError("test did not release the child load")
        time.sleep(0.2)
    return conf["table"]


with DAG(
    "provider_child_runs_child",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    default_args={"retries": 0},
    dagrun_timeout=timedelta(minutes=3),
) as child:
    load()


for parent_id, deferrable in (
    ("provider_child_runs_parent", False),
    ("provider_child_runs_parent_deferrable", True),
):
    with DAG(
        parent_id,
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        catchup=False,
        default_args={"retries": 0},
        dagrun_timeout=timedelta(minutes=3),
    ):
        child_dag_run_task_group(
            "load",
            trigger_dag_id=child.dag_id,
            requests=requests(),
            deferrable=deferrable,
            poke_interval=1,
            execution_timeout=timedelta(minutes=2),
        )
