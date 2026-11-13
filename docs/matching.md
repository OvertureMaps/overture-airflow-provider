# Reusable matching workflows

`matching_task_group` wires one matching workflow out of task factories the
caller supplies. It resolves the input and baseline paths, runs an optional
pre-match check, runs the matcher, checks that the results cover the requested
tables, then runs an optional provenance task and an optional sink. It does not
know which matcher runs, where the inputs come from or where the results go.
Instances can have arbitrary names, nested parent groups, or repeated group
names (Airflow adds collision suffixes), and they share no task IDs or XComs.

```python
from overture_airflow_provider import child_dag_run_task_group, matching_task_group


def load_tables(*, result_paths):
    loads = child_dag_run_task_group(
        "load",
        trigger_dag_id="example_load_table",
        requests=load_requests(result_paths),
    )
    return loads.run_ids


matching = matching_task_group(
    group_id="match_linz",
    tables=("division", "division_area"),
    input={
        "division": "s3://example-bucket/linz/data/type=division",
        "division_area": "s3://example-bucket/linz/data/type=division_area",
    },
    baseline=export_baseline,
    pre_match_task_factory=check_inputs,
    matcher_task_factory=run_matcher,
    provenance_task_factory=publish_matching_metadata,
    sink_task_factory=load_tables,
)

normalize >> matching.group
finalize = finalize_source(matching.complete)
```

`normalize`, `finalize_source`, `export_baseline`, `check_inputs`, `run_matcher`,
`publish_matching_metadata` and `load_requests` are supplied by the consuming
DAG. See [`examples/example_matching_dag.py`](../examples/example_matching_dag.py)
for a credential-free-to-parse, multi-provider scaffold whose matcher is a
`spark_agnostic_task_group` job. Source discovery, unchanged-data gates,
normalization, provider ordering/concurrency, source success markers, and
assemble remain consumer-owned. Place the gate before the **whole group**, and
finalize only after `complete`. Use a failure-sensitive end barrier (for example
`NONE_FAILED`) so all-unchanged is successful but failed provider processing
blocks assembly.

`load_tables` uses `child_dag_run_task_group`, described in
[Child DAG runs](child-dag-runs.md). The wiring is described in
[`SPEC.md`](../SPEC.md#matching-workflow).

## Arguments and result

| Argument | Role |
|---|---|
| `tables` | Required. Table names, as a sequence or a comma-separated string. The input, baseline and result maps must each contain exactly these tables, with non-empty paths. |
| `input`, `baseline` | Required. A `{table: path}` map: a mapping, its JSON string, a task output, or a zero-argument factory that builds the tasks producing one (an export, for example) and returns their output. |
| `matcher_task_factory` | Required. Called with the task outputs `input_paths` and `baseline_paths`. It builds the matcher job and returns the task output holding the results: a `{table: path}` mapping or its JSON string. |
| `pre_match_task_factory` | Optional. Called with `input_paths` and `baseline_paths`. It builds the tasks that must succeed before the matcher starts. |
| `provenance_task_factory` | Optional. Called with `input_paths`, `baseline_paths` and `result_paths`. It returns the output of the task that publishes the matching metadata. |
| `sink_task_factory` | Optional. Called with `result_paths`. It builds the tasks that store the results and returns the output of the last one. |

Each factory is called once, with keyword arguments, when the DAG is built. The
call happens inside the factory's own subgroup (`input`, `baseline`,
`pre_match`, `matcher`, `provenance` or `sink`), so it must build its tasks in
that call. The steps run in the order of the table, and each one waits for every
task of the steps before it, whether or not its own tasks use their outputs: the
matcher waits for the pre-match check, and the sink waits for the provenance
task.

`matching_task_group` returns `MatchingTasks`:

| Field | Meaning |
|---|---|
| `group` | The whole workflow. Gate it with `upstream >> matching.group`. |
| `matcher` | The `matcher` subgroup, holding whatever the matcher factory built. |
| `result_paths` | The checked `{table: path}` results. |
| `complete` | The output of the last configured step: the sink, else the provenance task, else `result_paths`. Finalize after this. |

`result_paths` alone is **not** evidence that a provenance task or sink
finished.

A matcher factory built on the Spark task group looks like this:

```python
def run_matcher(*, input_paths, baseline_paths):
    parameters = matcher_parameters(input_paths, baseline_paths)  # returns a JSON string
    job = spark_agnostic_task_group(group_id="spark", parameters=parameters, **spark_job)
    results = matcher_results()  # returns {table: path}
    job >> results
    return results
```

The matcher's arguments, its output layout and how its results are read back
belong to the factory. Give each instance and each run its own output location.
What the provenance task writes, and how strictly it treats upstream metadata it
cannot read, is the publisher's responsibility: raise from the task if a missing
entry should stop the sink.

## Failure handling

The group sets no trigger rules of its own. A step that fails stops every later
step, so `complete` does not succeed and finalization chained after it does not
run. A step that is skipped also stops the later steps, without failing the DAG
run: an intentionally unchanged provider stays skipped throughout its group, and
so does everything after a check, matcher or publisher that skips itself. In
both cases a provider is finished only when `complete` succeeded.

Input, baseline or result maps that do not cover `tables` raise
`AirflowFailException`, so they are not retried even when the DAG sets default
retries. The caller's own tasks follow the caller's retry policy. When the DAG
is built, a factory that returns something other than a task output raises
`TypeError`, and one that returns the output of a task built outside the call
raises `ValueError`, because that task would not wait for the earlier steps.
Result paths are written to the task log.
