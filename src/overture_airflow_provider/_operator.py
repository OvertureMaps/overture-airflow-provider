"""Deferrable operator that runs the resolved Spark job.

``SparkAgnosticExecuteOperator`` replaces the old ``@task``-decorated
PythonOperator that called ``platform_operator.execute()`` directly. That
pattern could not defer: a ``TaskDeferred`` raised inside a PythonOperator
resumes by calling ``PythonOperator.execute_complete`` — which doesn't exist —
so the run hung. A real ``BaseOperator`` subclass owns ``execute_complete``, so
Airflow resumes it correctly after the platform trigger fires.

Flow:

- ``execute`` resolves the platform handler, submits the job non-blocking, and
  either defers on the provider trigger (Glue, Databricks) or returns the
  synchronous result (Wherobots).
- ``execute_complete`` resolves the deferred run into the final result and
  pushes the cross-platform ``spark_agnostic`` XCom that ``SparkJobLink`` reads.

The worker slot is released the moment the job is submitted; the Triggerer
polls via asyncio until completion, so long Spark jobs no longer pin a Celery
worker for hours.
"""

import datetime
import json

from overture_airflow_provider._airflow_compat import BaseOperator
from overture_airflow_provider.setup_info import rehydrate
from overture_airflow_provider.spark_platform_handlers import get_platform_handler


def _xcom_datetime_default(obj):
    """JSON serializer that handles datetime objects pushed through XCom."""
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def _build_agnostic_xcom(setup_info: dict, result: dict) -> dict:
    """Cross-platform XCom payload downstream tasks and ``SparkJobLink`` rely on."""
    agnostic = {
        "spark_impl": setup_info["spark_impl_name"],
        "spark_family": setup_info["spark_family_name"],
        "spark_version": setup_info["spark_version"],
        "sedona_version": setup_info["sedona_version"],
    }
    for key in ("job_url", "status"):
        if key in result:
            agnostic[key] = result[key]
    return agnostic


def _int_or_none(value: str):
    return int(value) if value else None


class SparkAgnosticExecuteOperator(BaseOperator):
    """Submit a Spark job to the resolved platform and defer until it finishes."""

    template_fields = (
        "setup_info",
        "package_info",
        "jar_info",
        "cluster_info",
        "module_name",
        "class_name",
        "parameters",
        "extra_spark_env_vars",
        "spark_cluster_size_name",
        "spark_cluster_desired_worker_cores",
        "spark_cluster_desired_workers",
    )

    def __init__(
        self,
        *,
        setup_info,
        package_info=None,
        jar_info=None,
        cluster_info=None,
        module_name: str = "",
        class_name: str = "",
        parameters: str = "{}",
        extra_spark_env_vars: str = "{}",
        spark_cluster_size_name: str = "",
        spark_cluster_desired_worker_cores: str = "",
        spark_cluster_desired_workers: str = "",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.setup_info = setup_info
        self.package_info = package_info
        self.jar_info = jar_info
        self.cluster_info = cluster_info
        self.module_name = module_name
        self.class_name = class_name
        self.parameters = parameters
        self.extra_spark_env_vars = extra_spark_env_vars
        self.spark_cluster_size_name = spark_cluster_size_name
        self.spark_cluster_desired_worker_cores = spark_cluster_desired_worker_cores
        self.spark_cluster_desired_workers = spark_cluster_desired_workers

    def execute(self, context):
        full = rehydrate(self.setup_info)
        merged_spark_conf = (self.cluster_info or {}).get("merged_spark_conf", {})
        handler = get_platform_handler(full["spark_family"], full)

        submitted = handler.submit_job(
            package_info=self.package_info,
            jar_info=self.jar_info,
            cluster_info=self.cluster_info,
            module_name=self.module_name,
            class_name=self.class_name,
            parameters=self.parameters,
            extra_spark_conf=merged_spark_conf,
            extra_spark_env_vars=self.extra_spark_env_vars,
            spark_cluster_size_name=self.spark_cluster_size_name,
            spark_cluster_desired_worker_cores=_int_or_none(
                self.spark_cluster_desired_worker_cores
            ),
            spark_cluster_desired_workers=_int_or_none(self.spark_cluster_desired_workers),
            iam_role_name=self.setup_info.get("iam_role_name", "AWSGlueServiceRole"),
            wherobots_role_arn=self.setup_info.get("wherobots_role_arn", ""),
            task_id=self.task_id,
            context=context,
        )

        trigger = submitted.get("trigger")
        if trigger is None:
            # Synchronous platform (Wherobots): result is already final.
            return self._finalize(context, submitted["result"])

        self.defer(trigger=trigger, method_name="execute_complete")

    def execute_complete(self, context, event=None):
        full = rehydrate(self.setup_info)
        handler = get_platform_handler(full["spark_family"], full)
        result = handler.complete_job(event, context, cluster_info=self.cluster_info)
        return self._finalize(context, result)

    def _finalize(self, context, result: dict) -> dict:
        agnostic_xcom = _build_agnostic_xcom(self.setup_info, result)
        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is not None and callable(getattr(ti, "xcom_push", None)):
            ti.xcom_push(
                key="spark_agnostic",
                value=json.dumps(agnostic_xcom, default=_xcom_datetime_default),
            )
        return agnostic_xcom
