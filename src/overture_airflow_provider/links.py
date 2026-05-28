"""Operator extra-links for ``spark_agnostic_task_group``.

The ``execute_spark_job`` task pushes a JSON blob to XCom under the key
``spark_agnostic``.  ``SparkJobLink`` reads that blob and returns the
``job_url`` field so Airflow can render a clickable link on the task-instance
detail page.

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
    job_url = json.loads(url).get("job_url")
"""

from __future__ import annotations

import json
import logging

from airflow.models.xcom import XCom

from overture_airflow_provider._airflow_compat import BaseOperatorLink

log = logging.getLogger(__name__)

#: XCom key written by ``execute_spark_job``.
SPARK_AGNOSTIC_XCOM_KEY = "spark_agnostic"


class SparkJobLink(BaseOperatorLink):
    """Clickable link to the platform-native Spark job console.

    Reads ``job_url`` from the ``spark_agnostic`` XCom pushed by the
    ``execute_spark_job`` task.  Returns ``None`` when the platform does not
    provide a console URL (e.g. the job has not run yet or the platform
    doesn't expose one).
    """

    name = "Spark Job"

    def get_link(
        self,
        operator,
        *,
        ti_key=None,
        dttm=None,
    ) -> str | None:
        # ti_key-based lookup — preferred path (Airflow 2.3+ / dynamic mapping).
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
            return None
        try:
            return json.loads(raw).get("job_url")
        except (json.JSONDecodeError, AttributeError):
            log.debug("SparkJobLink: could not parse spark_agnostic XCom value")
            return None
