"""IcebergConfig Jinja must render through the task's op_kwargs.

Regression test: ``iceberg_config`` used to be closure-captured by the
``@task_group`` factory, so Airflow never templated it and Jinja such as the
S3 Tables warehouse ARN reached Spark verbatim. Its JSON fields are now
forwarded as ``op_kwargs`` strings, so Jinja renders at task execution time.
"""

import datetime
import json
from types import SimpleNamespace
from unittest import mock

from overture_airflow_provider._airflow_compat import DAG
from overture_airflow_provider.config import IcebergConfig
from overture_airflow_provider.spark_agnostic_taskgroup import (
    _select_iceberg_conf,
    spark_agnostic_task_group,
)

_WAREHOUSE_TEMPLATE = (
    "arn:aws:s3tables:us-west-2:123456789012:bucket/{{ var.value.managed_bucket_iceberg }}"
)


def _build_setup_cluster_op(iceberg_config, render_native=False):
    with DAG(
        dag_id="iceberg_templating_probe",
        schedule=None,
        start_date=datetime.datetime(2026, 1, 1),
        render_template_as_native_obj=render_native,
    ) as dag:
        spark_agnostic_task_group(
            group_id="grp",
            spark_impl_name="GLUE_v5",
            sedona_version="1.7.0",
            iceberg_config=iceberg_config,
        )
    return dag, dag.get_task("grp.setup_cluster")


def _render(dag, op, bucket):
    # ``setup_info`` is an XComArg op_kwarg whose resolution needs a task
    # instance; a mock satisfies it so the real render path runs and the
    # iceberg JSON strings get Jinja-rendered.
    context = {
        "ti": mock.MagicMock(),
        "task_instance": mock.MagicMock(),
        "var": SimpleNamespace(value=SimpleNamespace(managed_bucket_iceberg=bucket)),
    }
    op.render_template_fields(context, jinja_env=dag.get_template_env())


def test_s3tables_warehouse_jinja_renders_at_execution():
    s3tables = {
        "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables_catalog.warehouse": _WAREHOUSE_TEMPLATE,
    }
    dag, op = _build_setup_cluster_op(IcebergConfig(s3tables_spark_config=json.dumps(s3tables)))

    # The field reaches the operator as a templatable op_kwarg, not a closure.
    assert "{{ var.value.managed_bucket_iceberg }}" in op.op_kwargs["iceberg_s3tables_config"]

    _render(dag, op, "overture-managed-iceberg")

    rendered = op.op_kwargs["iceberg_s3tables_config"]
    assert "{{" not in rendered
    assert "bucket/overture-managed-iceberg" in rendered
    assert json.loads(rendered)["spark.sql.catalog.s3tables_catalog.warehouse"].endswith(
        ":bucket/overture-managed-iceberg"
    )


def test_all_iceberg_fields_are_op_kwargs():
    """Every IcebergConfig variant must be plumbed as a templatable op_kwarg."""
    dag, op = _build_setup_cluster_op(
        IcebergConfig(
            spark_config='{"primary": "{{ var.value.managed_bucket_iceberg }}"}',
            wherobots_spark_config='{"w": "{{ var.value.managed_bucket_iceberg }}"}',
            s3tables_spark_config='{"s3t": "{{ var.value.managed_bucket_iceberg }}"}',
            wherobots_s3tables_spark_config='{"ws3t": "{{ var.value.managed_bucket_iceberg }}"}',
        )
    )

    _render(dag, op, "B")

    assert json.loads(op.op_kwargs["iceberg_primary_config"])["primary"] == "B"
    assert json.loads(op.op_kwargs["iceberg_wherobots_config"])["w"] == "B"
    assert json.loads(op.op_kwargs["iceberg_s3tables_config"])["s3t"] == "B"
    assert json.loads(op.op_kwargs["iceberg_wherobots_s3tables_config"])["ws3t"] == "B"


def test_no_iceberg_config_defaults_to_empty():
    dag, op = _build_setup_cluster_op(None)

    assert op.op_kwargs["iceberg_primary_config"] == "{}"
    assert op.op_kwargs["iceberg_wherobots_config"] == "{}"
    assert op.op_kwargs["iceberg_s3tables_config"] == "{}"
    assert op.op_kwargs["iceberg_wherobots_s3tables_config"] == "{}"


def test_native_render_turns_op_kwarg_into_dict():
    """On render_template_as_native_obj=True DAGs, Airflow's NativeEnvironment
    literal_evals a rendered JSON-object op_kwarg into a dict. setup_cluster_task
    reassembles IcebergConfig from these values, so _select_iceberg_conf must
    tolerate dicts. Regression for: json.loads(dict) -> TypeError.
    """
    s3tables = {
        "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.s3tables_catalog.warehouse": _WAREHOUSE_TEMPLATE,
        "spark.sql.catalog.s3tables_catalog.http-client.apache.max-connections": 3000,
    }
    dag, op = _build_setup_cluster_op(
        IcebergConfig(spark_config=json.dumps(s3tables)), render_native=True
    )

    _render(dag, op, "overture-managed-iceberg")

    # NativeEnvironment converted the JSON-object string into a real dict.
    rendered = op.op_kwargs["iceberg_primary_config"]
    assert isinstance(rendered, dict)

    # The reassembled IcebergConfig must resolve without raising.
    cfg = IcebergConfig(spark_config=rendered)
    result = _select_iceberg_conf(cfg, "GLUE")
    assert result["spark.sql.catalog.s3tables_catalog.warehouse"].endswith(
        ":bucket/overture-managed-iceberg"
    )
    assert result["spark.sql.catalog.s3tables_catalog.http-client.apache.max-connections"] == 3000
