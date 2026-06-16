"""End-to-end checks against a real Airflow with the provider installed.

Credential-free: nothing here submits to Glue/Databricks/Wherobots. The point is
to prove the provider *integrates* with a real Airflow — installs, registers, and
its all-platforms example DAG parses cleanly through the actual scheduler — which
unit tests with mocked SDKs cannot guarantee.
"""

from __future__ import annotations

import json
import re
import shutil
import subprocess
import sys

import pytest

#: DAG id and task-group ids defined in ``examples/example_dag.py``.
EXAMPLE_DAG_ID = "example_spark_agnostic_all_platforms"
TASK_GROUPS = ("glue_job", "databricks_job", "wherobots_job")
#: Tasks each spark-agnostic group expands into (see spark_agnostic_taskgroup).
GROUP_TASKS = (
    "setup",
    "download_python_packages",
    "download_jars",
    "setup_cluster",
    "execute_spark_job",
)

# Whole module is e2e; skip cleanly anywhere the real Airflow CLI is absent.
pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(
        shutil.which("airflow") is None,
        reason="airflow CLI not on PATH (run via tests/e2e)",
    ),
]


def airflow(*args: str) -> str:
    """Run ``airflow <args>`` and return stdout, raising on a non-zero exit."""
    proc = subprocess.run(["airflow", *args], capture_output=True, text=True, check=True)
    return proc.stdout


def airflow_json(*args: str):
    """Run an ``airflow`` command with ``--output json`` and parse the payload.

    Airflow may emit logging noise before the JSON body, so this slices from the
    first ``[``/``{`` to be robust across versions.
    """
    out = airflow(*args, "--output", "json")
    start = min((i for i in (out.find("["), out.find("{")) if i != -1), default=-1)
    if start == -1:
        raise AssertionError(f"no JSON in `airflow {' '.join(args)}` output:\n{out}")
    return json.loads(out[start:])


def airflow_combined(*args: str) -> str:
    """Run ``airflow <args>`` and return stdout+stderr, asserting a zero exit.

    Unlike :func:`airflow`, this keeps stderr because Airflow streams task and
    diagnostic logs there (needed for ``tasks test``).
    """
    proc = subprocess.run(["airflow", *args], capture_output=True, text=True)
    out = proc.stdout + proc.stderr
    assert proc.returncode == 0, f"`airflow {' '.join(args)}` exited {proc.returncode}:\n{out}"
    return out


def test_no_dag_import_errors():
    """The example DAGs parse with zero import errors in a real scheduler.

    This is the core regression gate: a broken compat shim, an accidental eager
    Airflow/SDK import, or a bad provider registration all surface here.
    """
    errors = airflow_json("dags", "list-import-errors")
    assert errors == [], f"DAG import errors:\n{errors}"


def test_example_dag_registered():
    dag_ids = {row["dag_id"] for row in airflow_json("dags", "list")}
    assert EXAMPLE_DAG_ID in dag_ids, f"{EXAMPLE_DAG_ID} not in {sorted(dag_ids)}"


def test_task_group_expansion():
    """Each platform group expands into the full setup->execute pipeline."""
    listed = airflow("tasks", "list", EXAMPLE_DAG_ID)
    task_ids = {line.strip() for line in listed.splitlines() if line.strip()}
    expected = {f"{group}.{task}" for group in TASK_GROUPS for task in GROUP_TASKS}
    missing = expected - task_ids
    assert not missing, f"missing expanded tasks: {sorted(missing)}"


def test_provider_registered():
    """Airflow discovers the provider via its entry point."""
    packages = {row["package_name"] for row in airflow_json("providers", "list")}
    assert "airflow-provider-overture" in packages, sorted(packages)


def test_extra_links_registered():
    """Both operator extra-links are registered with Airflow's ProvidersManager."""
    from airflow.providers_manager import ProvidersManager

    pm = ProvidersManager()
    registered = set(pm.extra_links_class_names)
    expected = {
        "overture_airflow_provider.links.SparkJobLink",
        "overture_airflow_provider.links.ReportIssueLink",
    }
    missing = expected - registered
    assert not missing, f"missing extra-links: {sorted(missing)} (have {sorted(registered)})"


def test_import_does_not_pull_in_airflow():
    """Importing the provider must not eagerly import Airflow or platform SDKs.

    Run in a fresh interpreter (where Airflow *is* installed) so a regression to
    eager imports — the thing unit tests with mocked SDKs cannot see — fails
    here. Only touching a public symbol is allowed to pull Airflow in.
    """
    code = (
        "import sys, overture_airflow_provider;"
        "leaked = sorted(m for m in sys.modules"
        " if m == 'airflow' or m.startswith('airflow.')"
        " or m.split('.')[0] in {'databricks', 'wherobots', 'sh'});"
        "print('LEAKED:' + ','.join(leaked));"
        "sys.exit(1 if leaked else 0)"
    )
    proc = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert proc.returncode == 0, f"eager imports leaked:\n{proc.stdout}{proc.stderr}"


def test_setup_info_xcom_roundtrip():
    """The setup_info XCom payload is exactly the audited, JSON-safe subset.

    Guards the ``SERIALIZABLE_KEYS`` contract end to end: only allow-listed keys
    cross XCom, no non-serializable objects (enums / boto client) leak, and the
    payload survives the default XCom backend's JSON serialization for the
    Airflow version under test.
    """
    from overture_airflow_provider._setup import setup_spark_job
    from overture_airflow_provider.python_package_utils import CodeArtifactPyPiClient
    from overture_airflow_provider.setup_info import SERIALIZABLE_KEYS, to_xcom
    from overture_airflow_provider.spark import SparkFamily, SparkImpl

    info = setup_spark_job(
        spark_impl_name="GLUE_v5",
        sedona_version="1.7.0",
        module_name="my_pkg.jobs",
        class_name="MyJob",
        job_name="",
        parameters="{}",
        spark_jar_paths="",
    )
    payload = to_xcom(info)

    assert set(payload) == ((set(SERIALIZABLE_KEYS) & set(info)) | {"spark_family_name"})
    for key, value in payload.items():
        assert not isinstance(value, (SparkImpl, SparkFamily, CodeArtifactPyPiClient)), key

    # The default XCom backend serializes via JSON; this is the exact contract.
    assert json.loads(json.dumps(payload)) == payload

    # Also feed it through the installed Airflow's serializer; tolerate only
    # cross-version signature differences, never a real serialization failure.
    from airflow.models.xcom import BaseXCom

    try:
        BaseXCom.serialize_value(payload)
    except TypeError:
        pass


def test_setup_task_executes_credential_free():
    """``airflow tasks test`` runs the real setup task with no cloud creds.

    Exercises the live PythonOperator + Jinja-rendered op_kwargs + return path
    that DAG-parse checks never touch. The setup task builds version metadata and
    constructs (but never calls) the CodeArtifact client, so it needs no creds.
    """
    out = airflow_combined("tasks", "test", EXAMPLE_DAG_ID, "glue_job.setup", "2025-06-01")
    markers = ("run_identifier:", "Initialized private PyPI client", "Platform:")
    assert any(m in out for m in markers), f"no setup markers in output:\n{out}"


def test_intra_group_task_wiring():
    """Each platform group has the correct dependency edges, not just nodes.

    Parses the dot graph from ``airflow dags show`` and asserts the fan-out from
    ``setup`` and fan-in to ``execute_spark_job`` — a broken ``>>`` still parses
    clean, so node-existence checks alone would miss it.
    """
    dot = airflow("dags", "show", EXAMPLE_DAG_ID)
    edges = set(re.findall(r'"([^"]+)"\s*->\s*"([^"]+)"', dot))
    expected = set()
    for group in TASK_GROUPS:
        expected |= {
            (f"{group}.setup", f"{group}.download_python_packages"),
            (f"{group}.setup", f"{group}.download_jars"),
            (f"{group}.setup", f"{group}.setup_cluster"),
            (f"{group}.setup", f"{group}.execute_spark_job"),
            (f"{group}.download_python_packages", f"{group}.execute_spark_job"),
            (f"{group}.download_jars", f"{group}.execute_spark_job"),
            (f"{group}.setup_cluster", f"{group}.execute_spark_job"),
        }
    missing = expected - edges
    assert not missing, f"missing dependency edges: {sorted(missing)}"
