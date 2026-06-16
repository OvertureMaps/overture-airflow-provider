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

from overture_airflow_provider._airflow_compat import (
    AirflowException,
    BaseOperator,
    TaskDeferred,
)
from overture_airflow_provider._failures import format_failure
from overture_airflow_provider._report_issue import REPORT_ISSUE_XCOM_KEY
from overture_airflow_provider.links import (
    SPARK_AGNOSTIC_XCOM_KEY,
    ReportIssueLink,
)
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
        report_issue_config=None,
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
        self.report_issue_config = report_issue_config or None
        # Opt-in: only surface the "Report Issue" link when a target is configured.
        if self.report_issue_config and self.report_issue_config.get("target"):
            self.operator_extra_links = (ReportIssueLink(),)

    def execute(self, context):
        self._push_report_issue_config(context)
        full = rehydrate(self.setup_info)
        merged_spark_conf = (self.cluster_info or {}).get("merged_spark_conf", {})
        handler = get_platform_handler(full["spark_family"], full)

        try:
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
        except TaskDeferred:
            # A deferral isn't a failure; let Airflow handle resumption.
            raise
        except Exception as exc:
            # A failure on the submit/synchronous path. Whether the job actually
            # launched (early run-id XCom present) decides downstream-job vs
            # submit/config; describe_failure + the classifier handle the rest.
            info = handler.describe_failure(
                error=exc,
                run_launched=self._run_launched(context),
            )
            raise AirflowException(format_failure(info)) from None

        trigger = submitted.get("trigger")
        if trigger is None:
            # No trigger -> the run already finished (Wherobots always; Databricks
            # when it reached a terminal state within the submit window). Finalize
            # the result directly instead of deferring.
            return self._finalize(context, submitted["result"])

        self.defer(trigger=trigger, method_name="execute_complete")

    def execute_complete(self, context, event=None):
        full = rehydrate(self.setup_info)
        handler = get_platform_handler(full["spark_family"], full)
        result = handler.complete_job(event, context, cluster_info=self.cluster_info)
        return self._finalize(context, result)

    def resume_execution(self, next_method, next_kwargs, context):
        """Enrich deferral/trigger (Triggerer) failures before they surface.

        When a provider trigger crashes mid-poll (network drop, expired auth,
        Triggerer restart), Airflow resumes the task with the ``__fail__``
        sentinel for ``next_method`` instead of the normal completion callback.
        The default ``resume_execution`` would raise a bare ``TaskDeferralError``
        whose root cause is buried in the Triggerer logs -- exactly the
        unclassified noise this provider exists to tame.

        We intercept that sentinel (the documented mechanism on both Airflow 2
        and 3) and route the error through the same per-platform
        ``describe_failure`` seam used by the submit and complete paths, tagged
        ``is_trigger_failure`` so the classifier buckets it as ``trigger/polling``.
        Every other resume (a normal trigger event) is delegated untouched to the
        base implementation, which dispatches to ``execute_complete``.
        """
        if next_method == "__fail__":
            next_kwargs = next_kwargs or {}
            traceback = next_kwargs.get("traceback")
            if traceback:
                self.log.error("Trigger failed:\n%s", "\n".join(traceback))
            error = next_kwargs.get("error", "Trigger failed")
            full = rehydrate(self.setup_info)
            handler = get_platform_handler(full["spark_family"], full)
            info = handler.describe_failure(
                error=error if isinstance(error, BaseException) else Exception(str(error)),
                run_launched=True,
                is_trigger_failure=True,
            )
            raise AirflowException(format_failure(info)) from None
        return super().resume_execution(next_method, next_kwargs, context)

    def _push_report_issue_config(self, context) -> None:
        """Push the report-issue config to XCom so the link works even on failure.

        Done at the very start of ``execute`` (before submit) so the "Report
        Issue" link has its target the moment the task runs, regardless of how
        the run later ends.
        """
        if not self.report_issue_config:
            return
        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is None or not callable(getattr(ti, "xcom_push", None)):
            return
        try:
            ti.xcom_push(
                key=REPORT_ISSUE_XCOM_KEY,
                value=json.dumps(self.report_issue_config),
            )
        except Exception:
            # The link is a convenience; never let it break task execution.
            pass

    def _run_launched(self, context) -> bool:
        """True if the early ``spark_agnostic`` XCom was pushed (job launched).

        Drives failure classification on the submit path: present means the run
        reached the platform (downstream-job fault); absent means it never
        launched (submit/config fault).
        """
        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is None or not callable(getattr(ti, "xcom_pull", None)):
            return False
        try:
            return bool(ti.xcom_pull(task_ids=self.task_id, key=SPARK_AGNOSTIC_XCOM_KEY))
        except Exception:
            return False

    def _finalize(self, context, result: dict) -> dict:
        agnostic_xcom = _build_agnostic_xcom(self.setup_info, result)
        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is not None and callable(getattr(ti, "xcom_push", None)):
            ti.xcom_push(
                key="spark_agnostic",
                value=json.dumps(agnostic_xcom, default=_xcom_datetime_default),
            )
        return agnostic_xcom
