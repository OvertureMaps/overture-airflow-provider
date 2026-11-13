"""Fixtures for the in-container e2e suite.

The e2e tests shell out to the real ``airflow`` CLI inside the e2e image, so they
exercise the actual scheduler parse / provider-registration paths rather than an
in-process stub. They run only when ``RUN_E2E=1`` (set by ``tests/e2e/run-e2e.sh``);
the root conftest excludes this directory otherwise.
"""

from __future__ import annotations

import shutil
import subprocess

import pytest


@pytest.fixture(scope="session", autouse=True)
def _reserialized() -> None:
    """Parse the example DAGs into the metadata DB once per session.

    ``run-e2e.sh`` already reserializes before invoking pytest; repeating it here
    (idempotently) lets ``pytest tests/e2e`` work when run directly inside the
    container too. A no-op when the Airflow CLI is absent.
    """
    if shutil.which("airflow"):
        subprocess.run(["airflow", "dags", "reserialize"], capture_output=True, text=True)


@pytest.fixture
def run_dag():
    """Return a function that registers a DAG built in a test and runs it with ``dag.test()``."""

    def run(dag):
        from airflow.dag_processing.bundles.manager import DagBundlesManager
        from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db

        DagBundlesManager().sync_bundles_to_db()
        bag = DagBag(collect_dags=False)
        bag.bag_dag(dag)
        sync_bag_to_db(bag, "dags-folder", None)
        return dag.test()

    return run
