"""Instance-local matching, provenance publication and optional Corpus loading."""

import functools
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from overture_airflow_provider._airflow_compat import (
    AirflowFailException,
    TaskGroup,
    TriggerDagRunOperator,
    TriggerRule,
    XCom,
    XComArg,
    task,
)
from overture_airflow_provider._matching import (
    child_run_id,
    export_paths,
    load_uri,
    matcher_parameters,
    optional_value,
    output_root,
    path_map,
    selection,
    table_names,
)
from overture_airflow_provider.matching_config import CorpusDagConfig

log = logging.getLogger(__name__)


class MatcherTaskFactory(Protocol):
    """Build the matcher job and return the task output holding its result paths.

    ``parameters`` resolves to the matcher's argument mapping. The returned
    output must resolve to a ``{table: path}`` mapping, or its JSON string,
    covering exactly the requested tables.
    """

    def __call__(self, *, parameters: XComArg) -> XComArg: ...


class ProvenanceTaskFactory(Protocol):
    """Build the task that publishes matching metadata before any Corpus load.

    The task must write its metadata at ``metadata_root`` and return that root.
    A failed or skipped publication blocks loading.
    """

    def __call__(
        self,
        *,
        input_paths: XComArg,
        baseline_paths: XComArg,
        result_paths: XComArg,
        metadata_root: XComArg,
        parameters: XComArg,
    ) -> XComArg: ...


class PreMatchTaskFactory(Protocol):
    """Build a task or group that must succeed before the matcher runs."""

    def __call__(self, *, input_paths: XComArg, baseline_paths: XComArg) -> XComArg | TaskGroup: ...


@dataclass(frozen=True)
class MatchingTasks:
    """Public graph handles; chain finalization after ``complete``."""

    group: TaskGroup
    matcher: TaskGroup
    result_paths: XComArg
    complete: XComArg


def _permanent(function):
    """Fail a contract violation without retries; another attempt cannot change it."""

    @functools.wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (ValueError, TypeError) as error:
            raise AirflowFailException(str(error)) from error

    return wrapper


def _trigger_kwargs(conf: dict, operation: str, context) -> dict:
    ti = context["ti"]
    return {
        "conf": conf,
        "trigger_run_id": child_run_id(ti.dag_id, ti.run_id, ti.task_id, operation, conf),
    }


def _spark_conf(spark_impl_name):
    return {"SparkImpl": spark_impl_name} if spark_impl_name else {}


class ChildDagRunOperator(TriggerDagRunOperator):
    """Trigger a child run from a request that an upstream task already built.

    The ``conf`` is final data, so it is not templated again. Rendering it would
    let a DAG with ``render_template_as_native_obj=True`` turn a version such as
    ``"123"`` into an int, which the Corpus DAGs reject.
    """

    template_fields = tuple(
        field for field in TriggerDagRunOperator.template_fields if field != "conf"
    )


def _child_trigger(task_id, dag_id, requests, corpus, pool, **kwargs):
    """Trigger one waited-on child run per request; an empty list triggers nothing."""
    return ChildDagRunOperator.partial(
        task_id=task_id,
        trigger_dag_id=dag_id,
        wait_for_completion=True,
        reset_dag_run=True,
        deferrable=corpus.deferrable,
        pool=pool,
        retries=0,
        **kwargs,
    ).expand_kwargs(requests)


def _child_runs(task_id, count, context):
    ti = context["ti"]
    runs = [
        ti.xcom_pull(task_ids=task_id, key="trigger_run_id", map_indexes=index)
        for index in range(count)
    ]
    if any(not isinstance(run, str) or not run for run in runs) or len(set(runs)) != count:
        raise ValueError(f"Missing or duplicate triggered child run IDs from {task_id}")
    states = ti.get_task_states(dag_id=ti.dag_id, task_ids=[task_id], run_ids=[ti.run_id]).get(
        ti.run_id, {}
    )
    if len(states) != count or any(state != "success" for state in states.values()):
        raise ValueError(f"Every mapped child trigger must succeed: {task_id}: {states}")
    log.info("Child runs triggered by %s: %s", task_id, runs)
    return runs


@task(multiple_outputs=False)
@_permanent
def _parse_selection(value, tables, scenario):
    return selection(value, tables, scenario)


@task(multiple_outputs=False)
@_permanent
def _require_direct(parsed):
    if not parsed["paths"]:
        raise ValueError("A CorpusDagConfig is required to export a branch")
    return parsed["paths"]


@task(multiple_outputs=False)
def _export_parameters(
    parsed, tables, theme, export_format, combine_output, spark_impl_name, **context
):
    if parsed["paths"]:
        return []
    return [
        _trigger_kwargs(
            {
                "ThemeName": theme,
                "TableName": table,
                "BranchName": parsed["branch"],
                "Version": parsed["version"],
                "DataFormat": export_format,
                "CombineOutput": combine_output,
                **_spark_conf(spark_impl_name),
            },
            f"export:{table}",
            context,
        )
        for table in tables
    ]


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
@_permanent
def _exported_paths(export_task_id, child_dag_id, child_task_id, child_key, tables, **context):
    runs = _child_runs(export_task_id, len(tables), context)
    outputs = [
        XCom.get_one(dag_id=child_dag_id, run_id=run, task_id=child_task_id, key=child_key)
        for run in runs
    ]
    paths = export_paths(outputs, tables)
    log.info("Corpus export paths: %s", paths)
    return paths


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
@_permanent
def _selected_paths(parsed, exported):
    if bool(parsed["paths"]) == bool(exported):
        raise ValueError("Exactly one of override_paths or Corpus exports must supply the paths")
    return parsed["paths"] or exported


def _resolve_selection(value, tables, scenario, theme, corpus):
    """Return the selection's table paths and its parsed branch/version."""
    parsed = _parse_selection(value, tables, scenario)
    if corpus is None:
        return _require_direct(parsed), parsed
    requests = _export_parameters(
        parsed, tables, theme, corpus.export_format, corpus.combine_output, corpus.spark_impl_name
    )
    export = _child_trigger(
        "export",
        corpus.export_dag_id,
        requests,
        corpus,
        corpus.export_pool,
        max_active_tis_per_dagrun=1,
    )
    exported = _exported_paths(
        export.task_id,
        corpus.export_dag_id,
        corpus.export_output_task_id,
        corpus.export_output_key,
        tables,
    )
    export >> exported
    return _selected_paths(parsed, exported), parsed


@task(multiple_outputs=False)
@_permanent
def _build_parameters(input_paths, baseline_paths, options):
    return matcher_parameters(input_paths=input_paths, baseline_paths=baseline_paths, **options)


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
@_permanent
def _results(value, tables, **context):
    paths = path_map(value, tables, "matching results")
    root = output_root(paths)
    for table, path in paths.items():
        log.info("Matching output for %s: %s", table, path)
    log.info("Matching metadata root: %s", root)
    context["ti"].xcom_push(key="metadata_root", value=root)
    return paths


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
@_permanent
def _provenance_ready(published, root):
    if published != root:
        raise ValueError(f"Provenance publication must return the metadata root it wrote: {root}")
    return root


def _load_branch(scenario, branch, provenance):
    """Return the Corpus branch to load into, or ``None`` for a match-only run."""
    if scenario.lower() != "overture" or not optional_value(branch):
        return None
    if not isinstance(branch, str):
        raise ValueError("output_corpus_branch must be a string")
    # The scenario is known only now, so this is where the requirement can be enforced.
    if not provenance:
        raise ValueError("Corpus loading requires provenance_task_factory")
    return branch


@task(multiple_outputs=False)
@_permanent
def _create_requests(
    tables,
    baseline,
    scenario,
    branch,
    theme,
    default_branch,
    spark_impl_name,
    provenance,
    **context,
):
    branch = _load_branch(scenario, branch, provenance)
    if branch is None or branch == default_branch:
        return []
    conf = {
        "ThemeName": theme,
        "TableNames": ",".join(tables),
        "BranchName": branch,
        "BaseBranchName": baseline["branch"] or default_branch,
        "BaseVersion": baseline["version"],
        **_spark_conf(spark_impl_name),
    }
    return [_trigger_kwargs(conf, "create", context)]


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
@_permanent
def _branch_ready(requests, create_task_id, **context):
    """Release the loads only when every requested branch creation succeeded."""
    if requests:
        _child_runs(create_task_id, len(requests), context)


@task(multiple_outputs=False)
@_permanent
def _load_requests(
    paths,
    scenario,
    branch,
    provider,
    theme,
    expiration_period_days,
    spark_impl_name,
    provenance,
    **context,
):
    branch = _load_branch(scenario, branch, provenance)
    expiration = int(expiration_period_days)
    if expiration < 0:
        raise ValueError("expiration_period_days must be non-negative")
    if branch is None:
        return []
    return [
        _trigger_kwargs(
            {
                "ThemeName": theme,
                "TableName": table,
                "BranchName": branch,
                "InputPath": load_uri(path),
                "IdField": "id",
                "Source": provider,
                "ExpirationPeriodDays": expiration,
                **_spark_conf(spark_impl_name),
            },
            f"load:{table}",
            context,
        )
        for table, path in paths.items()
    ]


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
@_permanent
def _complete(paths, requests, load_task_id, **context):
    if requests is None:
        raise ValueError("Corpus load requests are missing")
    runs = _child_runs(load_task_id, len(requests), context) if requests else []
    return {"loaded": bool(requests), "result_paths": paths, "corpus_run_ids": runs}


@task(multiple_outputs=False)
def _match_only(paths):
    return {"loaded": False, "result_paths": paths, "corpus_run_ids": []}


def _load_results(
    ready,
    paths,
    tables,
    baseline,
    scenario,
    branch,
    provider,
    theme,
    expiration,
    corpus,
    provenance,
):
    requests = _load_requests(
        paths, scenario, branch, provider, theme, expiration, corpus.spark_impl_name, provenance
    )
    ready >> requests
    loads = _child_trigger("load", corpus.load_dag_id, requests, corpus, corpus.load_pool)
    if corpus.create_branch:
        creates = _create_requests(
            tables,
            baseline,
            scenario,
            branch,
            theme,
            corpus.default_branch,
            corpus.spark_impl_name,
            provenance,
        )
        ready >> creates
        create = _child_trigger(
            "create_branch", corpus.create_branch_dag_id, creates, corpus, corpus.load_pool
        )
        # Passes when no creation was needed; a failed or skipped creation blocks the loads.
        branch_ready = _branch_ready(creates, create.task_id)
        create >> branch_ready >> loads
    complete = _complete(paths, requests, loads.task_id)
    loads >> complete
    return complete


# An early check only: ``_load_branch`` enforces the requirement once the scenario is rendered.
_JINJA_MARKERS = ("{{", "{%", "{#")


def matching_task_group(
    group_id: str,
    *,
    scenario: str,
    theme: str,
    provider: Any,
    input: Any,
    baseline: Any,
    output_path: Any,
    run_id: Any,
    tables: str | Sequence[str],
    matcher_env: Any,
    matcher_task_factory: MatcherTaskFactory,
    corpus: CorpusDagConfig | None = None,
    output_corpus_branch: Any = "",
    provenance_task_factory: ProvenanceTaskFactory | None = None,
    pre_match_task_factory: PreMatchTaskFactory | None = None,
    override_config_path: Any = "",
    embeddings_cache_table: Any = None,
    match_history: Any = None,
    output_debug: Any = None,
    previous_metrics_path: Any = None,
    expiration_period_days: Any = 0,
) -> MatchingTasks:
    """Build an independent matching workflow with optional Corpus exports/loads.

    Input selections accept native or JSON mappings with ``branch``/``version``
    or ``override_paths``. Every runtime value is passed as a templated task
    argument.

    ``matcher_task_factory`` builds the matcher job at DAG construction; use
    ``spark_matcher_task_factory`` for the published Scala matcher. A provenance
    factory is required whenever Corpus loading can be enabled, and must publish
    matching metadata before returning its root. An optional pre-match factory
    adds a check that must pass before the matcher runs. No factory is
    serialized in XCom.

    The caller owns output namespace isolation, source gates and completion
    markers. Use ``upstream >> result.group`` and
    ``result.complete >> finalize_source``. No source marker is written here.
    """
    names = table_names(tables)
    can_load = scenario.lower() == "overture" or any(
        marker in scenario for marker in _JINJA_MARKERS
    )
    if (
        can_load
        and optional_value(output_corpus_branch)
        and (corpus is None or provenance_task_factory is None)
    ):
        raise ValueError("Corpus loading requires corpus and provenance_task_factory")

    with TaskGroup(group_id=group_id, add_suffix_on_collision=True) as group:
        with TaskGroup(group_id="input"):
            inputs, _ = _resolve_selection(input, names, scenario, theme, corpus)
        with TaskGroup(group_id="baseline"):
            baselines, baseline_selection = _resolve_selection(
                baseline, names, scenario, theme, corpus
            )
        pre_match = None
        if pre_match_task_factory is not None:
            with TaskGroup(group_id="pre_match"):
                pre_match = pre_match_task_factory(input_paths=inputs, baseline_paths=baselines)
                if not isinstance(pre_match, (XComArg, TaskGroup)):
                    raise TypeError("pre_match_task_factory must return a task output or TaskGroup")
                [inputs, baselines] >> pre_match
        parameters = _build_parameters(
            inputs,
            baselines,
            options={
                "scenario": scenario,
                "theme": theme,
                "provider": provider,
                "matcher_env": matcher_env,
                "output_path": output_path,
                "run_id": run_id,
                "override_config_path": override_config_path,
                "embeddings_cache_table": embeddings_cache_table,
                "match_history": match_history,
                "output_debug": output_debug,
                "previous_metrics_path": previous_metrics_path,
            },
        )
        if pre_match is not None:
            pre_match >> parameters
        with TaskGroup(group_id="matcher") as matcher:
            raw_paths = matcher_task_factory(parameters=parameters)
            if not isinstance(raw_paths, XComArg):
                raise TypeError(
                    "matcher_task_factory must return the task output holding the result paths"
                )
        # The matcher must wait for input validation and the pre-match check even
        # when its factory builds a job that does not consume ``parameters``.
        parameters >> matcher
        results = _results(raw_paths, names)
        # Detect unexpected skips without waking an entirely gated-off provider.
        [inputs, baselines, parameters] >> results
        ready = results
        if provenance_task_factory is not None:
            with TaskGroup(group_id="provenance"):
                root = results["metadata_root"]
                published = provenance_task_factory(
                    input_paths=inputs,
                    baseline_paths=baselines,
                    result_paths=results,
                    metadata_root=root,
                    parameters=parameters,
                )
                if not isinstance(published, XComArg):
                    raise TypeError(
                        "provenance_task_factory must return the publishing task's output"
                    )
                results >> published
                ready = _provenance_ready(published, root)
        with TaskGroup(group_id="load_results"):
            if corpus is not None:
                complete = _load_results(
                    ready,
                    results,
                    names,
                    baseline_selection,
                    scenario,
                    output_corpus_branch,
                    provider,
                    theme,
                    expiration_period_days,
                    corpus,
                    provenance_task_factory is not None,
                )
            else:
                complete = _match_only(results)
                ready >> complete
    return MatchingTasks(group, matcher, results, complete)
