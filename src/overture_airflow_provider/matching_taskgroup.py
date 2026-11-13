"""Instance-local matching workflow assembled from caller-supplied task factories."""

import functools
import json
import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from overture_airflow_provider._airflow_compat import (
    AirflowFailException,
    TaskGroup,
    XComArg,
    task,
)

log = logging.getLogger(__name__)

# Builds the tasks that resolve a ``{table: path}`` map and returns their output.
PathsTaskFactory = Callable[[], XComArg]


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
    """Build the task that publishes matching metadata and return its output."""

    def __call__(
        self, *, input_paths: XComArg, baseline_paths: XComArg, result_paths: XComArg
    ) -> XComArg: ...


class SinkTaskFactory(Protocol):
    """Build the tasks that store the results and return the output of the last one."""

    def __call__(self, *, result_paths: XComArg) -> XComArg: ...


@dataclass(frozen=True)
class MatchingTasks:
    """Public graph handles; chain finalization after ``complete``.

    ``complete`` is the output of the last configured step: the sink, else the
    provenance task, else the validated result paths.
    """

    group: TaskGroup
    matcher: TaskGroup
    result_paths: XComArg
    complete: XComArg


def table_names(tables: str | Sequence[str]) -> list[str]:
    names = tables.split(",") if isinstance(tables, str) else list(tables)
    if not names or any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("tables must contain non-empty table names")
    names = [name.strip() for name in names]
    if len(set(names)) != len(names):
        raise ValueError("tables must not contain duplicates")
    return names


def path_map(value: Any, tables: Sequence[str], label: str) -> dict[str, str]:
    """Return ``value`` as a ``{table: path}`` map covering exactly ``tables``."""
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping or JSON object")
    if set(value) != set(tables):
        raise ValueError(f"{label} must contain exactly the tables {list(tables)}")
    if any(not isinstance(path, str) or not path.strip() for path in value.values()):
        raise ValueError(f"{label} contains an empty or non-string path")
    return value


def _permanent(function):
    """Fail a contract violation without retries; another attempt cannot change it."""

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (ValueError, TypeError) as error:
            raise AirflowFailException(str(error)) from error

    return wrapper


@task(task_id="paths", multiple_outputs=False)
@_permanent
def _paths(value, tables, label):
    return path_map(value, tables, label)


@task(task_id="result_paths", multiple_outputs=False)
@_permanent
def _result_paths(value, tables):
    paths = path_map(value, tables, "matching results")
    for table, path in paths.items():
        log.info("Matching output for %s: %s", table, path)
    return paths


def _task_output(value, factory_name):
    if not isinstance(value, XComArg):
        raise TypeError(f"{factory_name} must return a task output")
    return value


def _require_built(operators, group, factory_name):
    """Reject a step whose tasks were built elsewhere and would not wait for earlier steps."""
    built = {id(operator) for operator in group.iter_tasks()}
    if not built or any(id(operator) not in built for operator in operators):
        raise ValueError(f"{factory_name} must build its tasks when it is called")


def _step_output(value, group, factory_name):
    value = _task_output(value, factory_name)
    _require_built([operator for operator, _ in value.iter_references()], group, factory_name)
    return value


def _resolve_paths(value, tables, label):
    if callable(value):
        value = _task_output(value(), f"The {label} factory")
    return _paths(value, tables, f"{label} paths")


def matching_task_group(
    group_id: str,
    *,
    tables: str | Sequence[str],
    input: Any,
    baseline: Any,
    matcher_task_factory: MatcherTaskFactory,
    pre_match_task_factory: PreMatchTaskFactory | None = None,
    provenance_task_factory: ProvenanceTaskFactory | None = None,
    sink_task_factory: SinkTaskFactory | None = None,
) -> MatchingTasks:
    """Build an independent matching workflow from task factories.

    The steps run in order: resolve the input and baseline paths, an optional
    pre-match check, the matcher, an optional provenance task, and an optional
    sink. Each factory is called once, at DAG construction, inside its own
    subgroup, and must build its tasks in that call. A step waits for every task
    of the steps before it, whether or not its own tasks consume their outputs,
    and a failed or skipped step stops the ones after it.

    ``input`` and ``baseline`` are a ``{table: path}`` map (a mapping, its JSON
    string or a task output) or a factory that builds the tasks producing one.
    Both maps and the matcher's results must cover exactly ``tables``.

    The caller owns everything else: where inputs come from, how the matcher is
    configured, what metadata is published, and where the results go. Use
    ``upstream >> result.group`` and ``result.complete >> finalize``.
    """
    names = table_names(tables)
    with TaskGroup(group_id=group_id, add_suffix_on_collision=True) as group:
        with TaskGroup(group_id="input"):
            inputs = _resolve_paths(input, names, "input")
        with TaskGroup(group_id="baseline"):
            baselines = _resolve_paths(baseline, names, "baseline")
        paths = {"input_paths": inputs, "baseline_paths": baselines}
        pre_match = None
        if pre_match_task_factory is not None:
            with TaskGroup(group_id="pre_match") as pre_match:
                pre_match_task_factory(**paths)
                _require_built([], pre_match, "pre_match_task_factory")
            [inputs, baselines] >> pre_match
        with TaskGroup(group_id="matcher") as matcher:
            raw_paths = _step_output(matcher_task_factory(**paths), matcher, "matcher_task_factory")
        [inputs, baselines] >> matcher
        if pre_match is not None:
            pre_match >> matcher
        results = _result_paths(raw_paths, names)
        matcher >> results
        complete, last_step = results, results
        if provenance_task_factory is not None:
            with TaskGroup(group_id="provenance") as provenance:
                complete = _step_output(
                    provenance_task_factory(**paths, result_paths=results),
                    provenance,
                    "provenance_task_factory",
                )
            results >> provenance
            last_step = provenance
        if sink_task_factory is not None:
            with TaskGroup(group_id="sink") as sink:
                complete = _step_output(
                    sink_task_factory(result_paths=results), sink, "sink_task_factory"
                )
            last_step >> sink
    return MatchingTasks(group, matcher, results, complete)
