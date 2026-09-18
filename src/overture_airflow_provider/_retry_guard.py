"""Guards against a fresh try racing a remote run left behind by an earlier
try of the same task instance.

A try can leave a Glue/Databricks/Wherobots run alive in several ways: a
zombie kill (the scheduler fails the task instance externally on a stale
heartbeat, so the operator's own exception handling never runs), a SIGTERM
while still on the worker, or -- for a *deferred* task -- a plain clear, which
isn't a failure at all: there's no worker process to signal, the trigger is
simply dropped, and the task instance goes back to ``None``. In every case a
retry or re-run then submits a second run against the same output path, and
both write concurrently.

Three layers cover those paths:

1. ``record_launched_run`` / ``read_recorded_run`` -- the run id is pushed to
   this task instance's own XCom the moment it's known. Airflow only clears a
   task instance's XCom right before that instance's *next* run starts, not
   at failure time, and it invokes ``on_failure_callback`` at
   failure-detection time for a zombie kill too (a scheduler-side path,
   distinct from ``on_kill``, precisely because the worker process is
   already gone -- see apache/airflow#65400). So the operator's
   ``on_failure_callback`` and ``on_kill`` can still read the run id this try
   pushed and cancel it, before Airflow clears it for the next try.
2. ``task_instance_key`` -- a stable, collision-free identity (a digest of
   ``dag_id``/``task_id``/``run_id``/``map_index``) for the task instance
   across all its tries. Glue stamps it onto every run's ``Arguments``, and
   ``_glue.stop_stale_glue_runs`` scans for still-active runs carrying it
   right before submitting a new one. That's what catches the clear of a
   deferred task, where no callback fires and the XCom from (1) is already
   gone by the time the fresh try starts; the platform's own run list is the
   only state that survives.

Neither layer needs a global store like an Airflow ``Variable``.
"""

import hashlib
import json
import logging

log = logging.getLogger(__name__)

_XCOM_KEY = "_overture_spark_agnostic_launched_run"


def _task_instance_identity(context) -> tuple[str, str, str, int] | None:
    """``(dag_id, task_id, run_id, map_index)`` of the task instance in ``context``.

    ``None`` when the context carries no task instance (e.g. the Airflow-free
    ``render`` preview) or its identity fields aren't the expected types (a
    bare ``MagicMock`` ti). Never raises.
    """
    ti = context.get("ti") if hasattr(context, "get") else None
    if ti is None:
        return None
    try:
        dag_id = getattr(ti, "dag_id", None)
        task_id = getattr(ti, "task_id", None)
        run_id = getattr(ti, "run_id", None)
        map_index = getattr(ti, "map_index", None)
        if run_id is None:
            dag_run = context.get("dag_run")
            run_id = getattr(dag_run, "run_id", None) or context.get("run_id")
        if map_index is None:
            map_index = -1
        if not (isinstance(dag_id, str) and isinstance(task_id, str) and isinstance(run_id, str)):
            return None
        if not isinstance(map_index, int):
            return None
        return dag_id, task_id, run_id, map_index
    except Exception:
        log.warning("Could not build task-instance key for stale-run guard", exc_info=True)
        return None


def task_instance_key(context) -> str | None:
    """Stable, collision-free identity of this task instance across its tries.

    The SHA-256 hex digest of the canonical JSON encoding of
    ``[dag_id, task_id, run_id, map_index]``. It deliberately excludes
    ``try_number`` so a retry or a cleared-and-rerun try shares the key with
    the try whose run it must supersede. Two different DAG runs of the same
    task get different keys, even when they write the same output; that
    coordination stays with the caller.

    Hashing (rather than joining the fields with a delimiter) matters because
    Airflow allows ``_`` in task ids and arbitrary characters in custom run
    ids, so no delimiter is safe: ``("a", "b__c")`` and ``("a__b", "c")``
    must not collide, or the stale-run scan could stop another task
    instance's run. A fixed 64-char digest also fits any argument-length
    limit and is safe to pass on a Glue command line. Use
    ``task_instance_label`` for a human-readable form in logs.

    Returns ``None`` when the context carries no task instance (e.g. the
    Airflow-free ``render`` preview), so callers can skip the guard rather
    than stamp a bogus marker. Never raises.
    """
    identity = _task_instance_identity(context)
    if identity is None:
        return None
    canonical = json.dumps(list(identity), separators=(",", ":"), ensure_ascii=True)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def task_instance_label(context) -> str | None:
    """Human-readable ``dag_id/task_id/run_id[map_index]`` for logging.

    Companion to ``task_instance_key``: the key is an opaque digest, so
    callers log this next to it to make a marker traceable back to its task
    instance. ``None`` under the same conditions as ``task_instance_key``.
    """
    identity = _task_instance_identity(context)
    if identity is None:
        return None
    dag_id, task_id, run_id, map_index = identity
    return f"{dag_id}/{task_id}/{run_id}[{map_index}]"


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

    Meant to be called from the operator's ``on_failure_callback`` (at
    failure-detection time) or ``on_kill`` (on SIGTERM), both before Airflow
    clears this task instance's XCom ahead of the next try. Never raises.
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
