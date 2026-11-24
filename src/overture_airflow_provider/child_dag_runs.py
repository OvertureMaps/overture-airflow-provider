"""Trigger child DAG runs with stable run IDs and confirm that every run succeeded."""

import hashlib
import json
import logging
import re
from collections.abc import Sequence
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


# Airflow's default ``allowed_run_id_pattern`` and the length of its run ID column.
_NOT_ALLOWED_IN_A_RUN_ID = re.compile(r"[^A-Za-z0-9_.~:+-]")
_RUN_ID_LENGTH = 250
_DIGEST_LENGTH = 12


def child_run_id(dag_id: str, run_id: str, task_id: str, map_index: int, conf: Any) -> str:
    """Name the parent trigger readably; the digest of its run and ``conf`` keeps the ID unique."""
    identity = json.dumps([dag_id, run_id, task_id, map_index, conf], sort_keys=True, default=str)
    digest = hashlib.sha256(identity.encode()).hexdigest()[:_DIGEST_LENGTH]
    origin = _NOT_ALLOWED_IN_A_RUN_ID.sub("_", f"child__{dag_id}__{task_id}__{map_index}")
    return f"{origin[: _RUN_ID_LENGTH - _DIGEST_LENGTH - 2]}__{digest}"


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


# The default rule is set explicitly on the group's tasks, so that a ``trigger_rule`` in
# the DAG's ``default_args`` cannot let them run after a skipped gate.
@task(task_id="requests", trigger_rule=TriggerRule.ALL_SUCCESS, multiple_outputs=False)
def _requests(requests):
    """Stage the requests inside the group, as a plain list.

    As the group's only root, it lets a gate on the group skip every task in it. As
    a successful upstream of ``run_ids``, it lets that task run, and fail, when the
    triggers were skipped.
    """
    # A mapped task's output arrives as a lazily read sequence, not a list.
    if isinstance(requests, Sequence) and not isinstance(requests, (str, bytes)):
        confs = list(requests)
        if all(isinstance(conf, dict) for conf in confs):
            return confs
    raise AirflowFailException("requests must be a list of conf mappings")


# Also runs when there are no requests and the mapped trigger was therefore skipped.
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
    **trigger_kwargs: Any,
) -> ChildDagRuns:
    """Trigger one child DAG run per request, wait for each, and return their run IDs.

    ``requests`` is a list of ``conf`` mappings, or a task output resolving to
    one, a mapped task's output included. An empty list from a task triggers
    nothing and yields an empty list; anything other than a list of mappings
    fails the group without retries. ``run_ids`` holds one ID per request and
    fails unless every trigger completed; a failed child run fails its trigger.
    A gate placed before ``group`` skips every task in it, wherever the requests
    were built.

    Each child run ID names the parent DAG, the trigger task and the request's
    position, and ends in a digest of those, the parent run and the ``conf``.
    Clearing a trigger, or retrying it when ``retries`` is raised, clears and
    reruns that same child run.

    Other keyword arguments (``pool``, ``deferrable``, ``poke_interval``,
    ``max_active_tis_per_dagrun``, ...) go to the trigger operator. A deferred
    trigger releases its pool slot unless the pool counts deferred tasks.
    """
    if not isinstance(requests, XComArg):
        if not isinstance(requests, (list, tuple)):
            raise TypeError("requests must be a list of conf mappings or a task output")
        if not requests:
            raise ValueError("requests must not be empty; pass a task output to decide at run time")
    with TaskGroup(group_id=group_id, add_suffix_on_collision=True) as group:
        staged = _requests(requests)
        trigger = ChildDagRunOperator.partial(
            task_id="trigger",
            trigger_dag_id=trigger_dag_id,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            wait_for_completion=True,
            reset_dag_run=True,
            retries=retries,
            **trigger_kwargs,
        ).expand(conf=staged)
        run_ids = _run_ids(trigger_dag_id, staged, XComArg(trigger, key="trigger_run_id"))
    return ChildDagRuns(group, run_ids)
