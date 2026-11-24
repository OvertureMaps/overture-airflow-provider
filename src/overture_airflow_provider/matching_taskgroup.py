"""Instance-local matching workflow assembled from caller-supplied task factories."""

import functools
import json
import logging
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from overture_airflow_provider._airflow_compat import (
    AirflowFailException,
    EmptyOperator,
    TaskGroup,
    TriggerRule,
    XComArg,
    task,
)

log = logging.getLogger(__name__)

# Builds the tasks that resolve a ``{table: path}`` map and returns their output.
PathsTaskFactory = Callable[[], XComArg]
# A ``{table: path}`` map, its JSON string, a task output resolving to either, or a factory.
Paths = Mapping[str, str] | str | XComArg | PathsTaskFactory


class MatcherTaskFactory(Protocol):
    """Build the matcher job and return the task output holding its result paths.

    The output must resolve to a ``{table: path}`` mapping, or its JSON string,
    covering exactly the requested tables.
    """

    def __call__(self, *, input_paths: XComArg, baseline_paths: XComArg) -> XComArg: ...


class PreMatchTaskFactory(Protocol):
    """Build the tasks that must succeed before the matcher runs."""

    def __call__(self, *, input_paths: XComArg, baseline_paths: XComArg) -> Any: ...


class ProvenanceTaskFactory(Protocol):
    """Build the tasks that publish the matching metadata."""

    def __call__(
        self, *, input_paths: XComArg, baseline_paths: XComArg, result_paths: XComArg
    ) -> Any: ...


class SinkTaskFactory(Protocol):
    """Build the tasks that store the results."""

    def __call__(self, *, result_paths: XComArg) -> Any: ...


@dataclass(frozen=True)
class MatchingTasks:
    """Graph handles: ``upstream >> matching.group >> finalize``.

    ``group`` begins at its ``start`` task and ends at its ``complete`` task,
    which succeeds only when every step did. ``result_paths`` is the checked
    ``{table: path}`` map of the matcher's results; on its own it does not show
    that the provenance or sink step finished.
    """

    group: TaskGroup
    result_paths: XComArg


def _table_names(tables: Sequence[str]) -> list[str]:
    names = [] if isinstance(tables, str) else list(tables)
    if not names or any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("tables must be a non-empty sequence of table names")
    if len(set(names)) != len(names):
        raise ValueError("tables must not contain duplicates")
    return names


def _path_map(value: Any, tables: Sequence[str], label: str) -> dict[str, str]:
    """Return ``value`` as a ``{table: path}`` map covering exactly ``tables``."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, Mapping):
        raise ValueError(f"{label} must be a mapping or JSON object")
    if set(value) != set(tables):
        raise ValueError(
            f"{label} must cover exactly the tables {list(tables)}, found {list(value)}"
        )
    if any(not isinstance(path, str) or not path.strip() for path in value.values()):
        raise ValueError(f"{label} contains an empty or non-string path")
    return dict(value)


def _permanent(function):
    """Fail a contract violation without retries; another attempt cannot change it."""

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (ValueError, TypeError) as error:
            raise AirflowFailException(str(error)) from error

    return wrapper


# The group's own tasks set their rule explicitly, so that a ``trigger_rule`` in the DAG's
# ``default_args`` cannot let them run after a skipped or failed step.
@task(task_id="paths", trigger_rule=TriggerRule.ALL_SUCCESS, multiple_outputs=False)
@_permanent
def _paths(value, tables, label):
    return _path_map(value, tables, label)


@task(task_id="result_paths", trigger_rule=TriggerRule.ALL_SUCCESS, multiple_outputs=False)
@_permanent
def _result_paths(value, tables):
    paths = _path_map(value, tables, "matching results")
    for table, path in paths.items():
        log.info("Matching output for %s: %s", table, path)
    return paths


def _require_built(operators, group, factory_name):
    """Reject tasks built elsewhere: they would not wait for earlier steps or the group's gate."""
    built = {id(operator) for operator in group.iter_tasks()}
    if not built or any(id(operator) not in built for operator in operators):
        raise ValueError(f"{factory_name} must build its tasks when it is called")


def _step_output(value, group, factory_name):
    if not isinstance(value, XComArg):
        raise TypeError(f"{factory_name} must return a task output")
    _require_built([operator for operator, _ in value.iter_references()], group, factory_name)
    return value


def _resolve_paths(value, tables, label):
    """Build the ``label`` subgroup and return it with the output of its ``paths`` task."""
    if isinstance(value, Mapping):
        _path_map(value, tables, f"{label} paths")
    with TaskGroup(group_id=label) as group:
        if callable(value):
            value = _step_output(value(), group, f"The {label} factory")
        paths = _paths(value, tables, f"{label} paths")
    return group, paths


def _step(group_id, factory, **outputs):
    """Call ``factory`` inside its own subgroup and return that subgroup."""
    with TaskGroup(group_id=group_id) as group:
        factory(**outputs)
        _require_built([], group, f"{group_id}_task_factory")
    return group


def _require_gated(group, start):
    """Reject a task that a skipped gate or step would not stop.

    A skip reaches a task through the non-teardown tasks of the group that it
    waits for. Under ``all_success`` one of them is enough. Under
    ``none_failed_min_one_success``, the rule of a join after a branch, it takes
    all of them. Any other rule lets the task run regardless.
    """
    work = {
        operator.task_id: operator for operator in group.iter_tasks() if not operator.is_teardown
    }
    for task_id, operator in work.items():
        if operator is start:
            continue
        rule = TriggerRule(operator.trigger_rule)
        if rule not in (TriggerRule.ALL_SUCCESS, TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS):
            raise ValueError(
                f"{task_id} has trigger rule '{rule.value}'. A task in a matching group uses "
                "'all_success', or 'none_failed_min_one_success' to join branches, so that a "
                "skipped gate or step stops it. Run cleanup as a teardown (as_teardown())."
            )
        joins = rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        gated = operator.upstream_task_ids & work.keys()
        ungated = operator.upstream_task_ids - gated
        if not gated or (joins and ungated):
            raise ValueError(
                f"{task_id} could run after a skipped gate: a gate on the matching group does "
                f"not skip {sorted(ungated)}. With '{rule.value}' it must wait for "
                + ("only non-teardown tasks" if joins else "a non-teardown task")
                + " of the group."
            )


def _failure_barrier(group):
    """Return a task that succeeds only if no non-teardown task of ``group`` failed.

    Every step's last tasks succeeding does not imply it. Airflow skips an
    ``all_success`` task as soon as one upstream is skipped, so a failure that
    arrives later on another upstream can sit behind a join that succeeds on
    its other branch. Skipped tasks pass, for the branches a factory did not take.
    """
    work = [operator for operator in group.iter_tasks() if not operator.is_teardown]
    barrier = EmptyOperator(
        task_id="work_succeeded", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    for operator in work:
        if operator.is_setup:
            # Airflow only lets `all_success` tasks follow a setup, so follow it through a relay.
            name = operator.task_id.removeprefix(f"{group.group_id}.").replace(".", "_")
            relay = EmptyOperator(task_id=f"{name}_succeeded", trigger_rule=TriggerRule.ALL_SUCCESS)
            operator >> relay
            operator = relay
        operator >> barrier
    return barrier


def matching_task_group(
    group_id: str,
    *,
    tables: Sequence[str],
    input: Paths,
    baseline: Paths,
    matcher_task_factory: MatcherTaskFactory,
    pre_match_task_factory: PreMatchTaskFactory | None = None,
    provenance_task_factory: ProvenanceTaskFactory | None = None,
    sink_task_factory: SinkTaskFactory | None = None,
) -> MatchingTasks:
    """Build an independent matching workflow from task factories.

    The steps run in order: resolve the input and baseline paths, an optional
    pre-match check, the matcher, a check of its result paths, an optional
    provenance step and an optional sink. Each factory is called once, at DAG
    construction, inside its own subgroup, and must build its tasks in that
    call. A step waits for every task of the step before it.

    ``input`` and ``baseline`` are a ``{table: path}`` map (a mapping, its JSON
    string or a task output) or a factory that builds the tasks producing one.
    Both maps and the matcher's results must cover exactly ``tables``.

    Use ``upstream >> result.group >> finalize``. A gate before the group skips
    every task in it, teardowns aside. The group's last task, ``complete``,
    succeeds only when every step succeeded and no task failed, so a failed or
    skipped step is never a completion. Factory tasks default to ``all_success``
    whatever the DAG's ``default_args`` say, and may only set
    ``none_failed_min_one_success``, to join branches of their own.

    The caller owns everything else: where inputs come from, how the matcher is
    configured, what metadata is published, and where the results go.
    """
    names = _table_names(tables)
    with TaskGroup(
        group_id=group_id,
        add_suffix_on_collision=True,
        default_args={"trigger_rule": TriggerRule.ALL_SUCCESS},
    ) as group:
        # The group's only root, so a gate on the group is a gate on this task.
        start = EmptyOperator(task_id="start", trigger_rule=TriggerRule.ALL_SUCCESS)
        input_group, inputs = _resolve_paths(input, names, "input")
        baseline_group, baselines = _resolve_paths(baseline, names, "baseline")
        paths = {"input_paths": inputs, "baseline_paths": baselines}
        steps = []
        if pre_match_task_factory is not None:
            steps.append(_step("pre_match", pre_match_task_factory, **paths))
        with TaskGroup(group_id="matcher") as matcher:
            raw_paths = _step_output(matcher_task_factory(**paths), matcher, "matcher_task_factory")
        results = _result_paths(raw_paths, names)
        steps += [matcher, results]
        if provenance_task_factory is not None:
            steps.append(
                _step("provenance", provenance_task_factory, **paths, result_paths=results)
            )
        if sink_task_factory is not None:
            steps.append(_step("sink", sink_task_factory, result_paths=results))
        # A step waits for the last tasks of the one before it, and so for all of that step,
        # whether or not its factory consumed their outputs or chained its own tasks.
        previous = [input_group, baseline_group]
        start >> previous
        for step in steps:
            previous >> step
            previous = step
        no_failures = _failure_barrier(group)
        complete = EmptyOperator(task_id="complete", trigger_rule=TriggerRule.ALL_SUCCESS)
        [input_group, baseline_group, *steps, no_failures] >> complete
        _require_gated(group, start)
    return MatchingTasks(group, results)
