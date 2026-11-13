"""Instance-local matching, provenance publication and optional Corpus loading."""

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

from overture_airflow_provider._airflow_compat import (
    TaskGroup,
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
    validate_provenance,
)
from overture_airflow_provider.matching_config import (
    CorpusDagConfig,
    DatabricksMatchingConfig,
    GlueMatchingConfig,
)


class ProvenanceTaskFactory(Protocol):
    """Build a task that publishes metadata and returns a validated lineage receipt.

    The receipt contains ``metadata_root`` and ``inputs``/``baselines`` lists of
    ``{"path": concrete_table_path, "metadata": non_empty_upstream_metadata}``.
    Publication/read errors must raise; the provider also rejects error entries,
    unresolved wildcards and incomplete path coverage.
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


class ValidationTaskFactory(Protocol):
    """Build an optional pre-match task/group from this invocation's inputs."""

    def __call__(
        self,
        *,
        input_paths: XComArg,
        baseline_paths: XComArg,
        embeddings_cache_table: Any,
    ) -> XComArg | TaskGroup: ...


@dataclass(frozen=True)
class MatchingTasks:
    """Public graph handles; chain finalization after ``complete``."""

    group: TaskGroup
    result_paths: XComArg
    complete: XComArg


@task(multiple_outputs=False)
def _parse_selection(value, tables, scenario):
    return selection(value, tables, scenario)


@task(multiple_outputs=False)
def _direct_paths(parsed):
    return parsed["paths"]


@task.branch
def _choose_selection(parsed, direct_task_id, export_task_id):
    return direct_task_id if parsed["paths"] else export_task_id


def _trigger_kwargs(conf: dict, operation: str, context) -> dict:
    ti = context["ti"]
    return {
        "conf": conf,
        "trigger_run_id": child_run_id(ti.dag_id, ti.run_id, ti.task_id, operation, conf),
    }


def _spark_conf(spark_impl_name):
    return {"SparkImpl": spark_impl_name} if spark_impl_name else {}


@task(multiple_outputs=False)
def _export_parameters(
    parsed, tables, theme, export_format, combine_output, spark_impl_name, **context
):
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
    return runs


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
def _exported_paths(
    export_task_id, child_dag_id, child_task_id, child_key, tables, requests, **context
):
    if len(requests) != len(tables):
        raise ValueError("Corpus export requests must cover every required table")
    runs = _child_runs(export_task_id, len(tables), context)
    outputs = [
        XCom.get_one(dag_id=child_dag_id, run_id=run, task_id=child_task_id, key=child_key)
        for run in runs
    ]
    paths = export_paths(outputs, tables)
    for table, output in zip(tables, outputs, strict=True):
        if set(export_paths([output], [table])) != {table}:
            raise ValueError("Corpus export table does not match its mapped request")
    return paths


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
def _selected_paths(direct, exported):
    if bool(direct) == bool(exported):
        raise ValueError("Exactly one input resolution branch must supply paths")
    return direct or exported


def _resolve_selection(value, tables, scenario, theme, corpus):
    parsed = _parse_selection(value, tables, scenario)
    direct = _direct_paths(parsed)
    if corpus is None:
        return _require_direct(parsed, direct)
    params = _export_parameters(
        parsed, tables, theme, corpus.export_format, corpus.combine_output, corpus.spark_impl_name
    )
    trigger = TriggerDagRunOperator.partial(
        task_id="export",
        trigger_dag_id=corpus.export_dag_id,
        wait_for_completion=True,
        deferrable=corpus.deferrable,
        pool=corpus.export_pool,
        retries=0,
    ).expand_kwargs(params)
    exported = _exported_paths(
        trigger.task_id,
        corpus.export_dag_id,
        corpus.export_output_task_id,
        corpus.export_output_key,
        tables,
        params,
    )
    choose = _choose_selection(parsed, direct.operator.task_id, params.operator.task_id)
    choose >> [direct, params]
    trigger >> exported
    return _selected_paths(direct, exported)


@task(multiple_outputs=False)
def _require_direct(parsed, paths):
    if not parsed["paths"]:
        raise ValueError("A CorpusDagConfig is required to export a branch")
    return paths


@task(multiple_outputs=False)
def _build_parameters(input_paths, baseline_paths, options):
    return matcher_parameters(input_paths=input_paths, baseline_paths=baseline_paths, **options)


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
def _results(value, tables):
    return path_map(value, tables, "matching results")


@task
def _metadata_root(paths):
    return output_root(paths)


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
def _provenance_ready(receipt, root, inputs, baseline):
    return validate_provenance(receipt, root, inputs, baseline)


@task(multiple_outputs=True)
def _load_plan(
    paths,
    baseline,
    scenario,
    branch,
    provider,
    theme,
    expiration_period_days,
    default_branch,
    create_branch,
    spark_impl_name,
    **context,
):
    load = scenario.lower() == "overture" and optional_value(branch)
    if load and not isinstance(branch, str):
        raise ValueError("output_corpus_branch must be a string")
    expiration = int(expiration_period_days)
    if expiration < 0:
        raise ValueError("expiration_period_days must be non-negative")
    common = _spark_conf(spark_impl_name)
    create_conf = {
        "ThemeName": theme,
        "TableNames": ",".join(paths),
        "BranchName": branch,
        "BaseBranchName": baseline["branch"] or default_branch,
        "BaseVersion": baseline["version"],
        **common,
    }
    create_kwargs = _trigger_kwargs(create_conf, "create", context)
    return {
        "load": load,
        "create": load and create_branch and branch != default_branch,
        "create_conf": create_conf,
        "create_run_id": create_kwargs["trigger_run_id"],
        "loads": [
            _trigger_kwargs(
                {
                    "ThemeName": theme,
                    "TableName": table,
                    "BranchName": branch,
                    "InputPath": load_uri(path),
                    "IdField": "id",
                    "Source": provider,
                    "ExpirationPeriodDays": expiration,
                    **common,
                },
                f"load:{table}",
                context,
            )
            for table, path in paths.items()
        ]
        if load
        else [],
        "result_paths": paths,
    }


@task.branch
def _choose_load(plan, load_task_id, no_load_task_id, loaded_task_id):
    return [load_task_id, loaded_task_id] if plan["load"] else no_load_task_id


@task.branch
def _choose_create(plan, create_task_id, existing_task_id):
    return create_task_id if plan["create"] else existing_task_id


@task(multiple_outputs=False)
def _load_parameters(plan):
    if not plan["loads"]:
        raise ValueError("Corpus loading requires non-empty table inputs")
    return plan["loads"]


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
def _loaded(plan, load_task_id, **context):
    return {
        "loaded": True,
        "result_paths": plan["result_paths"],
        "corpus_run_ids": _child_runs(load_task_id, len(plan["loads"]), context),
    }


@task(multiple_outputs=False)
def _not_loaded(plan):
    return {"loaded": False, "result_paths": plan["result_paths"], "corpus_run_ids": []}


@task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS, multiple_outputs=False)
def _complete(loaded, not_loaded):
    if (loaded is None) == (not_loaded is None):
        raise ValueError("Exactly one matching completion branch must succeed")
    return loaded if loaded is not None else not_loaded


def _load_results(paths, baseline, scenario, branch, provider, theme, expiration, corpus):
    plan = _load_plan(
        paths,
        baseline,
        scenario,
        branch,
        provider,
        theme,
        expiration,
        corpus.default_branch,
        corpus.create_branch,
        corpus.spark_impl_name,
    )
    start = EmptyOperator(task_id="start")
    no_load = _not_loaded(plan)
    existing_branch = EmptyOperator(task_id="existing_branch")
    if corpus.create_branch:
        create_branch = TriggerDagRunOperator(
            task_id="create_branch",
            trigger_dag_id=corpus.create_branch_dag_id,
            conf=plan["create_conf"],
            trigger_run_id=plan["create_run_id"],
            wait_for_completion=True,
            deferrable=corpus.deferrable,
            retries=0,
            pool=corpus.load_pool,
        )
        choose_create = _choose_create(plan, create_branch.task_id, existing_branch.task_id)
        start >> choose_create >> [create_branch, existing_branch]
        branch_ready = EmptyOperator(
            task_id="branch_ready", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
        )
        [create_branch, existing_branch] >> branch_ready
        ready = branch_ready
    else:
        start >> existing_branch
        ready = existing_branch
    params = _load_parameters(plan)
    ready >> params
    loads = TriggerDagRunOperator.partial(
        task_id="load",
        trigger_dag_id=corpus.load_dag_id,
        wait_for_completion=True,
        deferrable=corpus.deferrable,
        pool=corpus.load_pool,
        retries=0,
    ).expand_kwargs(params)
    loaded = _loaded(plan, loads.task_id)
    loads >> loaded
    choose = _choose_load(plan, start.task_id, no_load.operator.task_id, loaded.operator.task_id)
    choose >> [start, no_load, loaded]
    return _complete(loaded, no_load)


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
    runtime: GlueMatchingConfig | DatabricksMatchingConfig,
    corpus: CorpusDagConfig | None = None,
    output_corpus_branch: Any = "",
    provenance_task_factory: ProvenanceTaskFactory | None = None,
    validation_task_factory: ValidationTaskFactory | None = None,
    matcher_env: str = "dev",
    override_config_path: Any = "",
    output_result_file: str = "data_paths.json",
    embeddings_cache_table: Any = None,
    match_history: Any = None,
    output_debug: Any = None,
    previous_metrics_path: Any = None,
    expiration_period_days: Any = 0,
    spark_cluster_desired_worker_cores: str = "400",
) -> MatchingTasks:
    """Build an independent matching workflow with optional Corpus exports/loads.

    Input selections accept native or JSON mappings with ``branch``/``version``
    or ``override_paths``. Every runtime value is passed as a templated task
    argument. Execution backend/config is selected at DAG construction.

    A provenance factory is required whenever Corpus loading can be enabled.
    It must publish matching metadata before returning its receipt. A validation
    factory is required when using an embeddings cache. Neither factory is
    serialized in XCom: both build tasks at DAG construction.

    The caller owns output namespace isolation, source gates and completion
    markers. Use ``upstream >> result.group`` and
    ``result.complete >> finalize_source``. No source marker is written here.
    """
    names = table_names(tables)
    if not isinstance(runtime, (GlueMatchingConfig, DatabricksMatchingConfig)):
        raise ValueError("Matching supports GlueMatchingConfig or DatabricksMatchingConfig only")
    can_load = scenario.lower() == "overture" or "{{" in scenario
    if (
        can_load
        and optional_value(output_corpus_branch)
        and (corpus is None or provenance_task_factory is None)
    ):
        raise ValueError("Corpus loading requires corpus and provenance_task_factory")
    if optional_value(embeddings_cache_table) and validation_task_factory is None:
        raise ValueError("embeddings_cache_table requires validation_task_factory")

    with TaskGroup(group_id=group_id, add_suffix_on_collision=True) as group:
        with TaskGroup(group_id="input"):
            inputs = _resolve_selection(input, names, scenario, theme, corpus)
        with TaskGroup(group_id="baseline"):
            baselines = _resolve_selection(baseline, names, scenario, theme, corpus)
        validation = None
        if validation_task_factory is not None:
            with TaskGroup(group_id="validation"):
                validation = validation_task_factory(
                    input_paths=inputs,
                    baseline_paths=baselines,
                    embeddings_cache_table=embeddings_cache_table,
                )
                if not isinstance(validation, (XComArg, TaskGroup)):
                    raise TypeError(
                        "validation_task_factory must return a task output or TaskGroup"
                    )
                [inputs, baselines] >> validation
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
        if validation is not None:
            validation >> parameters
        with TaskGroup(group_id="matcher"):
            raw_paths = _build_matcher(
                runtime, parameters, output_result_file, spark_cluster_desired_worker_cores
            )
        results = _results(raw_paths, names)
        # Detect unexpected skips without waking an entirely gated-off provider.
        [inputs, baselines, parameters] >> results
        root = _metadata_root(results)
        provenance_ready = root
        if provenance_task_factory is not None:
            with TaskGroup(group_id="provenance"):
                receipt = provenance_task_factory(
                    input_paths=inputs,
                    baseline_paths=baselines,
                    result_paths=results,
                    metadata_root=root,
                    parameters=parameters,
                )
                if not isinstance(receipt, XComArg):
                    raise TypeError(
                        "provenance_task_factory must return the publishing task's output"
                    )
                [inputs, baselines, results, root, parameters] >> receipt
                provenance_ready = _provenance_ready(receipt, root, inputs, baselines)
        with TaskGroup(group_id="load_results"):
            if corpus is not None:
                parsed_baseline = _parse_selection(baseline, names, scenario)
                provenance_ready >> parsed_baseline
                complete = _load_results(
                    results,
                    parsed_baseline,
                    scenario,
                    output_corpus_branch,
                    provider,
                    theme,
                    expiration_period_days,
                    corpus,
                )
            else:
                complete = _match_only(results)
                provenance_ready >> complete
    return MatchingTasks(group, results, complete)


@task(multiple_outputs=False)
def _match_only(paths):
    return {"loaded": False, "result_paths": paths, "corpus_run_ids": []}


def _build_matcher(runtime, parameters, output_result_file, worker_cores):
    if isinstance(runtime, GlueMatchingConfig):
        from overture_airflow_provider._matching_glue import build_matching

        return build_matching(runtime, parameters, output_result_file, worker_cores)
    from overture_airflow_provider._matching_databricks import DatabricksMatchingOperator

    return DatabricksMatchingOperator(
        task_id="run",
        parameters=parameters,
        databricks_conn_id=runtime.databricks_conn_id,
        job_name=runtime.job_name,
        output_task_key_contains=runtime.output_task_key_contains,
        timeout_seconds=runtime.timeout_seconds,
        poll_interval_seconds=runtime.poll_interval_seconds,
        pool=runtime.pool,
        retries=0,
    ).output
