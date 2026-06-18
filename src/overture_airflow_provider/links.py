"""Operator extra-links for ``spark_agnostic_task_group``.

The ``execute_spark_job`` task pushes a JSON blob to XCom under the key
``spark_agnostic``.  ``SparkJobLink`` reads that blob and returns the
``job_url`` field so Airflow can render a clickable link on the task-instance
detail page.

Airflow 3.x flow
----------------
After the task completes, the task runner calls ``get_link`` and pushes the
returned URL to the ``_link_SparkJobLink`` XCom key.  The web server reads
that key directly — it never calls ``get_link`` itself.

``XCom`` is imported via ``_airflow_compat`` so the correct implementation is
used in each Airflow generation:

- **Airflow 3.x**: ``airflow.sdk.execution_time.xcom.XCom`` (SUPERVISOR_COMMS
  backed, available in the task-runner context where ``get_link`` is called).
- **Airflow 2.x**: ``airflow.models.xcom.XCom`` (SQLAlchemy backed, available
  in the web-server context where ``get_link`` is called in Airflow 2.x).

Supported platforms and their link targets:
- **Glue** — AWS Glue job-run console URL
- **Databricks** — Databricks run-page URL
- **Wherobots** — Wherobots run console URL (when provided by the platform)

Usage
-----
The link is registered automatically when the provider is installed.  To wire
it into the underlying ``PythonOperator`` that TaskFlow generates you can
attach it at DAG definition time::

    from overture_airflow_provider.links import SparkJobLink
    from airflow.models import DAG

    with DAG(...) as dag:
        tg = spark_agnostic_task_group("my_job", ...)
        # Attach the link to the execute task so it appears in the UI.
        execute_task = dag.get_task("my_job.execute_spark_job")
        execute_task.operator_extra_links = (SparkJobLink(),)

Alternatively, downstream tasks can read the URL directly from XCom::

    url = context["ti"].xcom_pull(
        task_ids="my_job.execute_spark_job",
        key="spark_agnostic",
    )
    # value is a JSON string; parse it to extract job_url
    job_url = json.loads(url).get("job_url")
"""

from __future__ import annotations

import json
import logging

from overture_airflow_provider._airflow_compat import BaseOperatorLink, XCom
from overture_airflow_provider._report_issue import (
    REPORT_ISSUE_XCOM_KEY,
    IssueContext,
    get_tracker,
    parse_report_issue_xcom,
)

log = logging.getLogger(__name__)

#: XCom key written by ``execute_spark_job``.
SPARK_AGNOSTIC_XCOM_KEY = "spark_agnostic"


class SparkJobLink(BaseOperatorLink):
    """Clickable link to the platform-native Spark job console.

    Reads ``job_url`` from the ``spark_agnostic`` XCom pushed by the
    ``execute_spark_job`` task.  Returns ``""`` when the platform does not
    provide a console URL or the XCom is not yet available.
    """

    name = "Spark Job"

    def get_link(
        self,
        operator,
        *,
        ti_key=None,
        dttm=None,
    ) -> str:
        # ti_key-based lookup — Airflow 2.3+ and all of Airflow 3.x.
        if ti_key is not None:
            raw = XCom.get_value(key=SPARK_AGNOSTIC_XCOM_KEY, ti_key=ti_key)
        else:
            # Fallback for Airflow < 2.3 (no dynamic task mapping).
            assert dttm is not None
            raw = XCom.get_one(
                key=SPARK_AGNOSTIC_XCOM_KEY,
                dag_id=operator.dag_id,
                task_id=operator.task_id,
                execution_date=dttm,
            )

        if not raw:
            return ""
        try:
            if isinstance(raw, dict):
                return raw.get("job_url") or ""
            return json.loads(raw).get("job_url") or ""
        except (json.JSONDecodeError, AttributeError, TypeError):
            log.debug("SparkJobLink: could not parse spark_agnostic XCom value")
            return ""


def _read_xcom(key, operator, ti_key, dttm):
    """Fetch a task XCom value across Airflow 2.3+ and 3.x lookup styles."""
    if ti_key is not None:
        return XCom.get_value(key=key, ti_key=ti_key)
    assert dttm is not None
    return XCom.get_one(
        key=key,
        dag_id=operator.dag_id,
        task_id=operator.task_id,
        execution_date=dttm,
    )


class ReportIssueLink(BaseOperatorLink):
    """Opt-in link to file a pre-filled issue about a failed Spark job.

    Only attached when the caller passes an *enabled* ``ReportIssueConfig`` with
    a target; the operator pushes that config to the ``report_issue`` XCom at the
    start of ``execute`` so the link works even when the run later fails. The
    target tracker (GitHub built in; others pluggable via ``_report_issue``)
    turns the run context into a "create issue" URL. Returns ``""`` — so the
    button stays inert — when the config is absent or the tracker is unknown.
    """

    name = "Report Issue"

    def get_link(
        self,
        operator,
        *,
        ti_key=None,
        dttm=None,
    ) -> str:
        cfg = parse_report_issue_xcom(_read_xcom(REPORT_ISSUE_XCOM_KEY, operator, ti_key, dttm))
        target = (cfg.get("target") or "").strip()
        if not target:
            return ""
        tracker = get_tracker(cfg.get("provider") or "")
        if tracker is None:
            log.debug("ReportIssueLink: unknown provider %r", cfg.get("provider"))
            return ""

        platform, job_url = self._spark_context(operator, ti_key, dttm)
        ctx = IssueContext(
            dag_id=getattr(operator, "dag_id", "") or "",
            task_id=getattr(operator, "task_id", "") or "",
            run_id=getattr(ti_key, "run_id", "") or "",
            platform=platform,
            job_url=job_url,
            labels=tuple(cfg.get("labels") or ()),
            extra=cfg.get("extra") or {},
        )
        try:
            return tracker.build_url(target, ctx)
        except Exception:
            log.debug("ReportIssueLink: tracker %r failed to build URL", tracker.name)
            return ""

    @staticmethod
    def _spark_context(operator, ti_key, dttm) -> tuple[str, str]:
        """Best-effort platform + job-console URL from the spark_agnostic XCom."""
        try:
            raw = _read_xcom(SPARK_AGNOSTIC_XCOM_KEY, operator, ti_key, dttm)
            data = raw if isinstance(raw, dict) else (json.loads(raw) if raw else {})
        except (json.JSONDecodeError, AttributeError, TypeError):
            return "", ""
        if not isinstance(data, dict):
            return "", ""
        return data.get("spark_impl") or "", data.get("job_url") or ""
