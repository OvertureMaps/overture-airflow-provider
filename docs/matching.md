# Reusable matching workflows

`matching_task_group` wires one matching workflow out of task factories the
caller supplies. It resolves the input and baseline paths, runs an optional
pre-match check, runs the matcher, checks that the results cover the requested
tables, then runs an optional provenance step and an optional sink. It does not
know which matcher runs, where the inputs come from or where the results go.
Instances can have any name, sit inside other task groups and repeat a group
name (Airflow adds a suffix), and they share no task IDs or XComs.

```python
from overture_airflow_provider import child_dag_run_task_group, matching_task_group


def load_tables(*, result_paths):
    child_dag_run_task_group(
        "load",
        trigger_dag_id="example_load_table",
        requests=load_requests(result_paths),
    )


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

source_changed >> matching.group >> finalize_source
```

`source_changed`, `finalize_source`, `export_baseline`, `check_inputs`,
`run_matcher`, `publish_matching_metadata` and `load_requests` belong to the
consuming DAG, and so do source discovery, normalization, the order providers
run in, completion markers and assemble. `load_tables` uses
`child_dag_run_task_group`, described in [Child DAG runs](child-dag-runs.md).
The wiring is described in [`SPEC.md`](../SPEC.md#matching-workflow).

## Arguments and result

| Argument | Role |
|---|---|
| `tables` | Required. A sequence of table names. The input, baseline and result maps must each contain exactly these tables, with non-empty paths. |
| `input`, `baseline` | Required. A `{table: path}` map: a mapping, its JSON string, a task output, or a zero-argument factory that builds the tasks producing one (an export, for example) and returns their output. Pass a task output that was built elsewhere directly, not through a factory. |
| `matcher_task_factory` | Required. Called with the task outputs `input_paths` and `baseline_paths`. It builds the matcher job and returns the task output holding the results: a `{table: path}` mapping or its JSON string. |
| `pre_match_task_factory` | Optional. Called with `input_paths` and `baseline_paths`. It builds the tasks that must succeed before the matcher starts. |
| `provenance_task_factory` | Optional. Called with `input_paths`, `baseline_paths` and `result_paths`. It builds the tasks that publish the matching metadata. |
| `sink_task_factory` | Optional. Called with `result_paths`. It builds the tasks that store the results. |

`matching_task_group` returns `MatchingTasks`:

| Field | Meaning |
|---|---|
| `group` | The whole workflow, from its first task, `start`, to its last, `complete`. Use `upstream >> matching.group >> finalize`. |
| `result_paths` | The checked `{table: path}` results, for a task outside the group that needs them. On its own it does not show that the provenance step or the sink finished. |

## Factories

Each factory is called once, with keyword arguments, when the DAG is built. The
call happens inside the factory's own subgroup (`input`, `baseline`,
`pre_match`, `matcher`, `provenance` or `sink`), and the factory must build its
tasks in that call. Only the `input`, `baseline` and matcher factories return
something; what the others return is ignored.

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
What the provenance step writes, and how strictly it treats upstream metadata it
cannot read, is the publisher's decision: raise from the task if a missing entry
should stop the sink.

### Trigger rules

Tasks a factory builds default to `all_success`, whatever `trigger_rule` the DAG
or a parent group sets in `default_args`. Other defaults, such as retries, still
apply. A factory may set `none_failed_min_one_success` on a task that joins
branches of its own, and everything that join waits for must be inside the
matching group. Any other rule is rejected when the DAG is built, because the
task could run after a skipped gate or step.

Cleanup that has to run after a failure is a teardown
(`cleanup.as_teardown(setups=prepare)`). Teardowns are exempt from the rule and
do not count towards completion. A teardown with a setup is skipped along with
it; one without a setup also runs when the group is gated off.

To finish a step early, for example when there is nothing to load, branch to a
no-op arm and join the arms with `none_failed_min_one_success`. A short-circuit
does not do this: it skips the rest of the step, so the group is not completed,
and the run still succeeds. A step whose last task is mapped over an empty list
ends the same way. End such a step with a `none_failed_min_one_success` task
that waits for the mapped task and for the task that produced the list, as
`run_ids` does in the child-run group.

## Failure handling

**Gate.** `start` is the group's only first task, so a gate placed before the
group is a gate on `start`. When the gate is skipped, every task in the group is
skipped and the run does not fail; when it fails, none of them runs. Teardowns
are the exception described above.

**Order.** The steps run in this order: input and baseline, pre-match check,
matcher, result check, provenance, sink. The first tasks of a step wait for
every last task of the step before it, whether or not the factory used their
outputs, so with the default rule a step starts only when the whole step before
it succeeded.

**Completion.** `complete` succeeds only when the last tasks of every step
succeeded and no task in the group failed, teardowns aside. Skipped tasks on a
branch that was not taken do not count against it. So a failed or skipped step
is never a completion: finalization chained after the group does not run, and
the provider is processed again on the next run. A failure fails the DAG run and
a skip does not.

**Contract errors.** Problems that are visible when the DAG is built raise
there: `tables` that are empty or repeated, a literal map that does not cover
`tables`, a factory that returns something other than a task output or builds no
tasks in its call, and a trigger rule that is not allowed. A map that a task
produced and that does not cover `tables` raises `AirflowFailException` when it
is checked, so it is not retried even when the DAG sets default retries. The
caller's own tasks follow the caller's retry policy. Result paths are written to
the task log.

## Several providers in one DAG

Give each provider its own group. Providers that match against and load the same
tables must not run at the same time, so chain them through a marker that also
succeeds when the provider before it was skipped:

```python
previous_done = None
for provider in providers:
    with TaskGroup(provider) as source:
        matching = matching_task_group(group_id="match", ...)
        source_changed(provider) >> matching.group >> finalize_source(provider)
    if previous_done is not None:
        previous_done >> matching.group
    previous_done = EmptyOperator(
        task_id=f"{provider}_done", trigger_rule=TriggerRule.NONE_FAILED
    )
    source >> previous_done
```

An unchanged provider releases the next one, and a failed one holds the rest.
End the ingest with one more `NONE_FAILED` task after every provider, and
assemble after that. See
[`examples/example_matching_dag.py`](../examples/example_matching_dag.py) for
the full scaffold with four providers; it parses without credentials.

## Migrating from the tf-data-platform matching group

The matching group in tf-data-platform
(`airflow/dags/src/public/overture_airflow/matching_operator.py` and
`matching_utils.py`) holds the wiring and the Corpus- and matcher-specific tasks
in one module. Here the wiring is the provider's and the rest is factories:

| Legacy piece | Becomes |
|---|---|
| `ParseMatchingInputs` and `corpus_export_tg`, once for the input and once for the baseline | `input` and `baseline`. Override paths are passed as a map. An export is a factory that triggers the Corpus export with `child_dag_run_task_group` and returns `{table: export path}`. |
| `validate_embeddings_cache` | `pre_match_task_factory` (places). |
| `choose_matching_platform`, `glue_matching`, `DatabricksMatchingOperator` and `get_matching_result_paths` | `matcher_task_factory`: the matcher's parameters, a `spark_agnostic_task_group` job, and a task that returns the `{table: path}` results. |
| `write_match_output_metadata` | `provenance_task_factory`. |
| `LoadMatchingToCorpus` and `corpus_load_tg` | `sink_task_factory`: the create-branch and per-table load requests, each sent through `child_dag_run_task_group`. |
| `X_COM_LOOKUP` and the `matching_tg` task IDs | Nothing. Factories receive task outputs, so no task is addressed by its ID. |

**Where the factories live.** They build Airflow tasks, so they cannot go into a
package that must not import Airflow. Until the Corpus DAGs' `conf` contract and
the Python matcher's job contract settle, they replace the two legacy modules in
place, without a new module under `airflow/dags/src/`. That is an interim home.
The factories are reusable orchestration, which this provider owns, and they
move here once those contracts are stable. Environment values stay with the
DAGs either way.

**What this change is for.** OvertureMaps/tf-data-platform#6265 already nests
the legacy group once per provider in the divisions ingest DAG, by rewriting its
task-ID lookups. This group therefore does not unblock that flow. It moves the
wiring to the repository that owns reusable task groups and removes the task-ID
lookups.

**Rollout.**

1. Release the provider with these task groups.
2. Raise the provider pin in the overture-pipelines requirements to that
   release, so the deployed Airflow has it.
3. Land the tf-data-platform change that builds the factories and moves the
   divisions and places DAGs onto this group.
4. Show on real runs against `dev` that the matcher inputs, result paths,
   metadata and Corpus loads are the same as with the legacy group.
5. Delete the legacy group.

**Behaviour that changes.** The equivalence check in step 4 has to account for
these:

- Result maps must cover exactly `tables` with non-empty paths. The legacy group
  drops empty paths.
- Any failed task in the group blocks completion. The legacy load carries on
  after a failed create-branch run, through a `none_skipped` rule that this
  group rejects.
- Clearing a trigger reruns the same child run. The legacy triggers start a new
  run each time.
- The compute platform is chosen when the DAG is built, by which job the matcher
  factory builds. The legacy group branches between Glue and Databricks at run
  time.
- The result is `MatchingTasks`, not a task group. Chain with `matching.group`.
