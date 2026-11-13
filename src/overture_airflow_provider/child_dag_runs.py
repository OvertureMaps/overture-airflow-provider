"""Trigger child DAG runs with stable run IDs and confirm that every run succeeded."""

import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Any

from overture_airflow_provider._airflow_compat import (
    AirflowFailException,
    TaskGroup,
    TriggerDagRunOperator,
    TriggerRule,
    XComArg,
    task,
)

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChildDagRuns:
    """Graph handles: gate with ``upstream >> runs.group`` and consume ``runs.run_ids``."""

    group: TaskGroup
    run_ids: XComArg


def child_run_id(dag_id: str, run_id: str, task_id: str, map_index: int, conf: Any) -> str:
    identity = json.dumps([dag_id, run_id, task_id, map_index, conf], sort_keys=True, default=str)
    return "child__" + hashlib.sha256(identity.encode()).hexdigest()[:40]


class ChildDagRunOperator(TriggerDagRunOperator):
    """Trigger a child run whose ID is fixed by the parent task instance and its conf.

    With ``reset_dag_run=True`` a cleared or retried trigger reruns that same
    child run instead of failing on the existing ID or starting a second run.
    """

    def execute(self, context):
        ti = context["ti"]
        self.trigger_run_id = child_run_id(
            ti.dag_id, ti.run_id, ti.task_id, ti.map_index, self.conf
        )
        return super().execute(context)


# Also runs when the request list is empty and the mapped trigger was therefore skipped.
@task(
    task_id="run_ids",
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    multiple_outputs=False,
)
def _run_ids(trigger_dag_id, requests, runs):
    requests, runs = list(requests or []), list(runs or [])
    if len(runs) != len(requests) or any(not isinstance(run, str) or not run for run in runs):
        raise AirflowFailException(
            f"Expected {len(requests)} completed child runs of {trigger_dag_id}, found {runs}"
        )
    log.info("Child runs of %s: %s", trigger_dag_id, runs)
    return runs


def child_dag_run_task_group(
    group_id: str,
    *,
    trigger_dag_id: str,
    requests: Any,
    retries: int = 0,
    max_active_tis_per_dagrun: int | None = None,
    **trigger_kwargs: Any,
) -> ChildDagRuns:
    """Trigger one child DAG run per request, wait for each, and return their run IDs.

    ``requests`` is a list of ``conf`` mappings, or a task output resolving to
    one. An empty list from a task triggers nothing and yields an empty list.
    ``run_ids`` holds one ID per request and fails unless every trigger
    completed; a failed child run fails its trigger.

    Each child run ID is derived from the parent DAG run, the trigger task, the
    request's position and its ``conf``. Clearing a trigger, or retrying it when
    ``retries`` is raised, clears and reruns that same child run.

    ``max_active_tis_per_dagrun=1`` runs the requests one at a time. Other
    keyword arguments (``pool``, ``deferrable``, ``poke_interval``, ...) go to
    the trigger operator. A deferred trigger releases its pool slot unless the
    pool counts deferred tasks.
    """
    if not isinstance(requests, XComArg) and not requests:
        raise ValueError("requests must not be empty; pass a task output to decide at run time")
    with TaskGroup(group_id=group_id, add_suffix_on_collision=True) as group:
        trigger = ChildDagRunOperator.partial(
            task_id="trigger",
            trigger_dag_id=trigger_dag_id,
            wait_for_completion=True,
            reset_dag_run=True,
            retries=retries,
            max_active_tis_per_dagrun=max_active_tis_per_dagrun,
            **trigger_kwargs,
        ).expand(conf=requests)
        run_ids = _run_ids(trigger_dag_id, requests, XComArg(trigger, key="trigger_run_id"))
    return ChildDagRuns(group, run_ids)
