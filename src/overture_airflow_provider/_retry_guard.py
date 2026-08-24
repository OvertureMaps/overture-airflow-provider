"""Best-effort cancellation of a prior try's remote run before it retries.

A zombie-killed try can leave a Glue/Databricks/Wherobots run alive: the
scheduler fails the task instance externally (stale heartbeat), so the
operator's own exception handling and ``on_kill`` never run, and the launched
remote run keeps writing. The next try then submits a second run against the
same output path, and both write concurrently.

XCom can't carry the prior try's run id forward: Airflow clears every XCom
value for a task instance immediately before each retry, specifically so
retries stay idempotent. Airflow Variables aren't cleared on retry, so this
module uses one, scoped to a single task instance, as the hand-off: the
platform submit path records the run id there the moment it's known, and the
next try reads (and clears) it before submitting a new run, cancelling
whatever it finds.
"""

import json
import logging

from overture_airflow_provider._airflow_compat import Variable

log = logging.getLogger(__name__)

_VARIABLE_PREFIX = "_overture_spark_agnostic_launched_run"


def _ti_from(context) -> object | None:
    return context.get("ti") if hasattr(context, "get") else None


def _variable_key(context) -> str | None:
    """A key unique to this task instance (not this try) across a DAG run."""
    ti = _ti_from(context)
    if ti is None:
        return None
    dag_id = getattr(ti, "dag_id", None)
    task_id = getattr(ti, "task_id", None)
    run_id = getattr(ti, "run_id", None)
    map_index = getattr(ti, "map_index", -1)
    if not dag_id or not task_id or not run_id:
        return None
    return f"{_VARIABLE_PREFIX}::{dag_id}::{task_id}::{run_id}::{map_index}"


def record_launched_run(context, *, platform: str, run_id: str, extra: dict | None = None) -> None:
    """Persist the just-launched run id so a retry can cancel it if needed.

    Called from the platform submit path as soon as the remote run id is
    known -- before any polling/deferral -- so a zombie kill anywhere after
    this point still leaves a recoverable trail. Never raises: this is a
    safety net, not something that should fail the job it's protecting.
    """
    if not run_id:
        return
    key = _variable_key(context)
    if key is None:
        return
    try:
        Variable.set(
            key,
            json.dumps({"platform": platform, "run_id": run_id, "extra": extra or {}}),
        )
    except Exception:
        log.warning(
            "Could not record launched run %s for retry-cancel guard", run_id, exc_info=True
        )


def pop_recorded_run(context) -> dict | None:
    """Read and clear the run recorded for this task instance, if any."""
    key = _variable_key(context)
    if key is None:
        return None
    try:
        raw = Variable.get(key, default_var=None)
    except Exception:
        log.warning("Could not read retry-cancel guard variable", exc_info=True)
        return None
    if raw is None:
        return None
    clear_recorded_run(context)
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        log.warning("Retry-cancel guard variable had unparseable content: %r", raw)
        return None


def clear_recorded_run(context) -> None:
    """Drop the recorded run once it's no longer needed (success, or a
    non-retryable failure -- there's nothing left for a retry to cancel)."""
    key = _variable_key(context)
    if key is None:
        return
    try:
        Variable.delete(key)
    except Exception:
        # Already absent, or the backend is unavailable -- either way this is
        # cleanup, not correctness-critical.
        pass
