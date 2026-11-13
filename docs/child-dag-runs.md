# Child DAG runs

`child_dag_run_task_group` triggers one run of another DAG per request, waits
for each run, and returns the run IDs once every run has succeeded. It suits a
[matching](matching.md) sink that loads one table per child run, and it works
without `matching_task_group`.

```python
from overture_airflow_provider import child_dag_run_task_group

loads = child_dag_run_task_group(
    "load",
    trigger_dag_id="example_load_table",
    requests=load_requests(result_paths),
    max_active_tis_per_dagrun=1,
)

upstream >> loads.group
finalize = finalize_source(loads.run_ids)
```

## Arguments and result

| Argument | Role |
|---|---|
| `trigger_dag_id` | The DAG to run. |
| `requests` | The `conf` mapping of each child run: a list, or a task output resolving to one, a mapped task's output included. A task may return an empty list, which triggers nothing. A literal that is empty or not a list is rejected when the DAG is built, and a task output that is not a list of mappings fails the group without retries. |
| `retries` | Retries of each trigger, `0` by default. A retry reruns the same child run. |
| `max_active_tis_per_dagrun` | How many triggers may run at once. `1` runs the requests one at a time. |
| other keywords | Passed to the trigger operator, for example `pool`, `deferrable` or `poke_interval`. |

It returns `ChildDagRuns(group, run_ids)`. `run_ids` holds one run ID per
request, and an empty list when there were no requests. A failed child run fails
its trigger, and a request left without a run ID, for example because its
trigger was skipped, fails `run_ids`. When a gate placed before `group` is
skipped, every task in the group is skipped, `run_ids` included, wherever the
requests were built. A `trigger_rule` in the DAG's `default_args` does not
change that: the group's tasks set their own.

## Run IDs and retries

Each child run ID is derived from the parent DAG run, the trigger task, the
request's position and its `conf`, and triggers set `reset_dag_run`. To retry
after a child run fails, fix the cause and clear the failed trigger: the same
child run is cleared and rerun instead of failing on the existing ID or starting
a second run. Clearing a trigger that already succeeded reruns its child run
too. The child runs are independent, so one failing does not undo the others.

## Requests, child output and pools

Requests from a task output reach the child DAG as the task built them: Airflow
does not render them again, so a string such as `"123"` stays a string even in a
DAG that renders templates as native objects. A literal request list is rendered
like any other templated argument.

The helper returns run IDs only, because a trigger that waits for its child run
cannot return that run's output. To read it, pull the child's XCom by the
returned run ID in a task of your own.

Triggers block a worker while they wait unless `deferrable=True`. A deferred
trigger releases its pool slot unless the pool counts deferred tasks, so a pool
that serialises child runs must count them.

The design is described in [`SPEC.md`](../SPEC.md#child-dag-runs).
