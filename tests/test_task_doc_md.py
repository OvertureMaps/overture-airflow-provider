"""Regression guard for Airflow Task Documentation rendering smells.

``@task`` auto-populates ``doc_md`` from the function's own docstring, but
Airflow renders it verbatim without dedenting first (apache/airflow#66477
is still open). A docstring's indented continuation lines therefore land as
an accidental Markdown code block in the Task Documentation panel, and RST's
double-backtick markup (``` ``like_this`` ```) shows up literally inside it
instead of rendering as inline code. See spark_agnostic_taskgroup.py's
``_SETUP_TASK_DOC``/``_SETUP_CLUSTER_TASK_DOC`` for the fix: pass ``doc_md``
explicitly as a flush-left, single-backtick string constant instead of
relying on the docstring auto-population.
"""

import datetime
import re

from overture_airflow_provider._airflow_compat import DAG
from overture_airflow_provider.spark_agnostic_taskgroup import spark_agnostic_task_group

_INDENTED_LINE = re.compile(r"^[ \t]+\S")


def _doc_md_smells(doc_md: str) -> list[str]:
    """Flag doc_md content that Airflow's un-dedented Markdown rendering mangles."""
    smells = []
    for i, line in enumerate(doc_md.splitlines()):
        if i > 0 and _INDENTED_LINE.match(line):
            smells.append(f"line {i + 1} is indented (renders as a code block): {line!r}")
        if "``" in line:
            smells.append(
                f"line {i + 1} uses RST-style double backticks, not Markdown's single backtick: {line!r}"
            )
    return smells


def _build_group_dag():
    with DAG(
        dag_id="doc_md_smoke_probe",
        schedule=None,
        start_date=datetime.datetime(2026, 1, 1),
    ) as dag:
        spark_agnostic_task_group(
            group_id="grp",
            spark_impl_name="GLUE_v5",
            sedona_version="1.7.0",
        )
    return dag


def test_no_task_doc_md_has_rendering_smells():
    """Every task's doc_md in the group must render cleanly in the Airflow UI."""
    dag = _build_group_dag()

    checked = 0
    for task in dag.tasks:
        doc_md = getattr(task, "doc_md", None)
        if not doc_md:
            continue
        checked += 1
        smells = _doc_md_smells(doc_md)
        assert not smells, f"{task.task_id}.doc_md has rendering smells: {smells}"

    # If nobody sets doc_md any more, this test would silently stop checking
    # anything -- assert the two known doc_md tasks are still present.
    assert checked >= 2, "expected at least setup + setup_cluster to have doc_md set"
