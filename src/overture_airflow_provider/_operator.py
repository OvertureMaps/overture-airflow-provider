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
import re

from overture_airflow_provider._airflow_compat import (
    AirflowException,
    AirflowFailException,
    BaseOperator,
    TaskDeferred,
)
from overture_airflow_provider._failures import format_failure
from overture_airflow_provider._report_issue import REPORT_ISSUE_XCOM_KEY
from overture_airflow_provider._retry_guard import clear_recorded_run, pop_recorded_run
from overture_airflow_provider.links import (
    SPARK_AGNOSTIC_XCOM_KEY,
    ReportIssueLink,
)
from overture_airflow_provider.setup_info import rehydrate
from overture_airflow_provider.spark_platform_handlers import get_platform_handler

_GLUE_RUN_ID_RE = re.compile(r"\bjr_[0-9a-f]{16,}\b")


def _terminal_run_id(error_text: str) -> str | None:
    """Recover a Glue run id from a trigger error reporting a terminal state.

    The Glue trigger raises ``... Job jr_<hex> Run State: FAILED`` (or
    STOPPED/TIMEOUT) on a terminal state. A message with both a run id and a
    run state is resolvable; a genuine Triggerer crash carries neither, so
    this returns ``None`` and the caller keeps the generic classification.
    """
    if "Run State" not in error_text:
        return None
    match = _GLUE_RUN_ID_RE.search(error_text)
    return match.group(0) if match else None


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
        self._cancel_prior_run_if_retried(context, handler)

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
            run_launched = self._run_launched(context)
            info = handler.describe_failure(
                error=exc,
                run_launched=run_launched,
            )
            # Launched-and-failed is a real job/data bug -- retrying just burns
            # another full run at cost, so it's never retryable. Never-launched
            # is a submit/config fault (e.g. a worker recycle mid-submit) that a
            # caller-configured retry can safely resolve.
            exc_cls = AirflowFailException if run_launched else AirflowException
            if exc_cls is AirflowFailException:
                # Non-retryable: nothing will retry-cancel this run, so drop
                # the recorded run id instead of leaving it to leak.
                clear_recorded_run(context)
            raise exc_cls(format_failure(info)) from None

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

        Airflow resumes a deferred task with the ``__fail__`` sentinel in two
        cases:

        1. A finished job that reached a terminal non-success state. Some
           triggers (notably AWS Glue) raise on FAILED/STOPPED/TIMEOUT, so a
           finished-but-failed run reaches ``__fail__`` too. This recovers the
           run id and resolves it through the same ``complete_job`` path
           ``execute_complete`` uses, so the task log names the real platform
           error instead of a generic "trigger failure" with ``run: <unknown>``.

        2. A genuine Triggerer crash mid-poll, which carries no resolvable
           run. This goes through the per-platform ``describe_failure`` seam
           tagged ``is_trigger_failure`` and raises the non-retryable
           ``AirflowFailException``, since a crash mid-poll doesn't mean the
           job is safe to resubmit.

        Every other resume is delegated to the base implementation.
        """
        if next_method == "__fail__":
            next_kwargs = next_kwargs or {}
            traceback = next_kwargs.get("traceback")
            if traceback:
                self.log.error("Trigger failed:\n%s", "\n".join(traceback))
            error = next_kwargs.get("error", "Trigger failed")
            full = rehydrate(self.setup_info)
            handler = get_platform_handler(full["spark_family"], full)

            # error and traceback can carry the run id independently of each
            # other depending on the trigger, so check both.
            error_text = "\n".join([*(traceback or []), str(error)])
            run_id = _terminal_run_id(error_text)
            if run_id is not None:
                try:
                    result = handler.complete_job(
                        {"run_id": run_id}, context, cluster_info=self.cluster_info
                    )
                except AirflowFailException:
                    # Already the classified, non-retryable failure; propagate unchanged.
                    clear_recorded_run(context)
                    raise
                except AirflowException as resolve_exc:
                    # complete_job only ever raises plain AirflowException, but a
                    # resolved run_id means the job launched -- never retryable,
                    # same as the launched-and-failed case in execute() above.
                    clear_recorded_run(context)
                    raise AirflowFailException(str(resolve_exc)) from None
                except Exception as resolve_exc:  # noqa: BLE001
                    # The run couldn't be resolved, so this falls through to the generic classification.
                    self.log.warning("Could not resolve terminal run %s: %s", run_id, resolve_exc)
                else:
                    # The run succeeded despite __fail__ (rare), so this finalizes normally.
                    return self._finalize(context, result)

            info = handler.describe_failure(
                error=error if isinstance(error, BaseException) else Exception(str(error)),
                run_launched=True,
                is_trigger_failure=True,
            )
            clear_recorded_run(context)
            raise AirflowFailException(format_failure(info)) from None
        return super().resume_execution(next_method, next_kwargs, context)

    def _cancel_prior_run_if_retried(self, context, handler) -> None:
        """Cancel a run recorded by a zombie-killed previous try, if any.

        XCom is cleared before every retry, but the Airflow Variable used by
        the retry guard isn't -- so a prior try's run id survives here even
        when the scheduler failed that try externally (a zombie kill) without
        ever running the operator's own exception handling or ``on_kill``.
        Best-effort: a failure here never blocks this try's own submission.
        """
        ti = context.get("ti") if hasattr(context, "get") else None
        try_number = getattr(ti, "try_number", None) if ti is not None else None
        if not try_number or try_number <= 1:
            return
        prior = pop_recorded_run(context)
        if not prior or not prior.get("run_id"):
            return
        try:
            handler.cancel_run(prior["run_id"], prior.get("extra"))
        except Exception:
            self.log.warning(
                "Retry %s: could not cancel prior %s run %s left over from the "
                "previous try; it may still be writing output concurrently "
                "with this retry.",
                try_number,
                prior.get("platform"),
                prior.get("run_id"),
                exc_info=True,
            )
        else:
            self.log.warning(
                "Retry %s: cancelled prior %s run %s left over from a "
                "zombie-killed try to avoid two concurrent runs writing the "
                "same output.",
                try_number,
                prior.get("platform"),
                prior["run_id"],
            )

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
        # This try reached a terminal (success) state on its own -- nothing
        # left for a retry to cancel.
        clear_recorded_run(context)
        agnostic_xcom = _build_agnostic_xcom(self.setup_info, result)
        ti = context.get("ti") if hasattr(context, "get") else None
        if ti is not None and callable(getattr(ti, "xcom_push", None)):
            ti.xcom_push(
                key="spark_agnostic",
                value=json.dumps(agnostic_xcom, default=_xcom_datetime_default),
            )
        return agnostic_xcom
