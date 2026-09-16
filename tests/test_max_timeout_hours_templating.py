"""``max_timeout_hours`` must be a real, Jinja-templatable field on the execute operator."""

import datetime
from unittest import mock

from overture_airflow_provider._airflow_compat import DAG
from overture_airflow_provider.spark_agnostic_taskgroup import spark_agnostic_task_group


def _build_execute_op(max_timeout_hours):
    with DAG(
        dag_id="max_timeout_hours_templating_probe",
        schedule=None,
        start_date=datetime.datetime(2026, 1, 1),
    ) as dag:
        spark_agnostic_task_group(
            group_id="grp",
            spark_impl_name="GLUE_v5",
            sedona_version="1.7.0",
            max_timeout_hours=max_timeout_hours,
        )
    return dag, dag.get_task("grp.execute_spark_job")


def _render(dag, op, timeout_hours):
    context = {
        "ti": mock.MagicMock(),
        "task_instance": mock.MagicMock(),
        "expanded_ti_count": None,
        "var": mock.MagicMock(value=mock.MagicMock(timeout_hours=timeout_hours)),
    }
    op.render_template_fields(context, jinja_env=dag.get_template_env())


def test_max_timeout_hours_is_a_template_field():
    assert "max_timeout_hours" in type(_build_execute_op("")[1]).template_fields


def test_max_timeout_hours_reaches_the_operator_unrendered():
    _, op = _build_execute_op("{{ var.value.timeout_hours }}")
    assert op.max_timeout_hours == "{{ var.value.timeout_hours }}"


def test_max_timeout_hours_jinja_renders_at_execution():
    dag, op = _build_execute_op("{{ var.value.timeout_hours }}")
    _render(dag, op, "4")
    assert op.max_timeout_hours == "4"


def test_default_max_timeout_hours_is_empty_string():
    _, op = _build_execute_op("")
    assert op.max_timeout_hours == ""
