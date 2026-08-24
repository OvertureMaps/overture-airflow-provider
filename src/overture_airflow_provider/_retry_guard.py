"""Best-effort cancellation of a run left behind by a failed try, before a
retry resubmits.

A zombie-killed try can leave a Glue/Databricks/Wherobots run alive: the
scheduler fails the task instance externally (stale heartbeat), so the
operator's own exception handling and ``on_kill`` never run, and the launched
remote run keeps writing. A retry then submits a second run against the same
output path, and both write concurrently.

This doesn't need any state to survive past the failure, so it doesn't need
a global store like an Airflow ``Variable``: Airflow only clears a task
instance's XCom right before that instance's *next* run starts, not at
failure time, and it invokes ``on_failure_callback`` at failure-detection
time for a zombie kill too (that's a documented scheduler-side path,
distinct from ``on_kill``, precisely because the worker process is already
gone -- see apache/airflow#65400). So the callback can still read the run id
this task instance pushed to its own XCom before it died, and cancel it
there, before Airflow ever clears it for the next try.
"""

import json
import logging

log = logging.getLogger(__name__)

_XCOM_KEY = "_overture_spark_agnostic_launched_run"


def record_launched_run(context, *, platform: str, run_id: str, extra: dict | None = None) -> None:
    """Push the just-launched run id to this task instance's own XCom.

    Called from the platform submit path as soon as the remote run id is
    known -- before any polling/deferral -- so a zombie kill anywhere after
    this point still leaves a recoverable trail for ``read_recorded_run``.
    Never raises: this is a safety net, not something that should fail the
    job it's protecting.
    """
    if not run_id:
        return
    ti = context.get("ti") if hasattr(context, "get") else None
    if ti is None or not callable(getattr(ti, "xcom_push", None)):
        return
    try:
        ti.xcom_push(
            key=_XCOM_KEY,
            value=json.dumps({"platform": platform, "run_id": run_id, "extra": extra or {}}),
        )
    except Exception:
        log.warning(
            "Could not record launched run %s for retry-cancel guard", run_id, exc_info=True
        )


def read_recorded_run(context) -> dict | None:
    """Read the run this task instance recorded, if any.

    Meant to be called from an ``on_failure_callback`` at failure-detection
    time, before Airflow clears this task instance's XCom ahead of the next
    try. Never raises.
    """
    ti = context.get("ti") if hasattr(context, "get") else None
    if ti is None or not callable(getattr(ti, "xcom_pull", None)):
        return None
    try:
        raw = ti.xcom_pull(key=_XCOM_KEY)
    except Exception:
        log.warning("Could not read retry-cancel guard XCom", exc_info=True)
        return None
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (TypeError, ValueError):
        log.warning("Retry-cancel guard XCom had unparseable content: %r", raw)
        return None
