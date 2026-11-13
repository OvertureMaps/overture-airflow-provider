# overture-airflow-provider

[![PyPI version](https://img.shields.io/pypi/v/airflow-provider-overture.svg)](https://pypi.org/project/airflow-provider-overture/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-provider-overture.svg)](https://pypi.org/project/airflow-provider-overture/)
[![License: MIT](https://img.shields.io/pypi/l/airflow-provider-overture.svg)](LICENSE)

An Apache Airflow provider for running PySpark or Scala/Spark jobs on AWS Glue, Databricks, or Wherobots Cloud from a single DAG-level API.

Write your DAG once and target any supported engine by switching one argument. Cluster shape, Iceberg catalog wiring, JAR and wheel distribution, and per-platform cluster init are all handled by the provider.

The provider is intentionally unopinionated: every environment-specific value (S3 buckets, IAM roles, catalog endpoints, package registries) is passed in via typed config dataclasses. No org-specific defaults are baked in.

> Beta. Tested against real Airflow 3.3 via Docker e2e.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Reusable matching workflows](#reusable-matching-workflows)
- [Operator links](#operator-links)
- [Failure messages](#failure-messages)
  - [Retries](#retries)
    - [What's retryable](#whats-retryable)
    - [What isn't](#what-isnt)
- [Wherobots runtime selection](#wherobots-runtime-selection)
- [Databricks runner deployment](#databricks-runner-deployment)
- [Local rendering](#local-rendering)
- [Bundle Inspector plugin](#bundle-inspector-plugin)
- [Reference](#reference)
  - [Supported versions](#supported-versions)
  - [Spark platform matrix](#spark-platform-matrix)
  - [Apache Sedona](#apache-sedona)
- [Development](#development)

## Install

```bash
pip install airflow-provider-overture
```

Optional extras for platforms that need additional SDKs:

```bash
pip install "airflow-provider-overture[databricks]"
pip install "airflow-provider-overture[wherobots]"
pip install "airflow-provider-overture[all]"
```

Requires Python `>=3.11` and Apache Airflow `>=3.3`.

## Quick start

```python
from datetime import datetime

from airflow import DAG

from overture_airflow_provider import (
    ArtifactStoreConfig,
    AwsGlueClusterSize,
    GlueConfig,
    IcebergConfig,
    PackageRegistryConfig,
    spark_agnostic_task_group,
)

with DAG(
    dag_id="example_spark_agnostic",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    spark_agnostic_task_group(
        group_id="my_spark_job",
        spark_impl_name="GLUE_v5",
        sedona_version="1.7.0",
        module_name="my_pkg.jobs",
        class_name="MyJob",
        python_packages="my-pkg==1.0.0",
        parameters={"s3_input": "s3://example-bucket/in/", "s3_output": "s3://example-bucket/out/"},
        spark_cluster_size=AwsGlueClusterSize.G_2X.name,
        artifact_store=ArtifactStoreConfig(
            s3_bucket="example-bucket",
            s3_root="spark-agnostic-operator",
        ),
        package_registry=PackageRegistryConfig(
            domain="my-domain",
            domain_owner="123456789012",
            repository="my-pypi",
            region="us-east-1",
        ),
        glue_config=GlueConfig(iam_role_name="AWSGlueServiceRole"),
        iceberg_config=IcebergConfig(spark_config="{}"),
    )
```

Switching to Databricks or Wherobots requires only a different `spark_impl_name` and the matching config dataclass. The surrounding DAG code does not change.

See [`examples/example_dag.py`](examples/example_dag.py) for a runnable DAG targeting all three platforms.

See [`SPEC.md`](SPEC.md) for the full architecture.

## Reusable matching workflows

`matching_task_group` resolves input/baseline paths, optionally exports Corpus
tables, runs the matcher, validates its results, waits for provenance to be
published, and optionally loads every result table into Corpus. Instances can
have arbitrary names, nested parent groups, or repeated group names (Airflow
adds collision suffixes). They do not depend on a global `matching_tg` or
another instance's XComs.

```python
from overture_airflow_provider import (
    CorpusDagConfig,
    matching_task_group,
    spark_matcher_task_factory,
)

matching = matching_task_group(
    group_id="match_linz",
    scenario="overture",
    theme="divisions",
    provider="linz",
    input={
        "override_paths": {
            "division": "s3://example-bucket/linz/data/type=division",
            "division_area": "s3://example-bucket/linz/data/type=division_area",
        }
    },
    baseline={"branch": "main", "version": "{{ params.baseline_version }}"},
    tables=("division", "division_area"),
    output_path="s3://example-bucket/matching/linz/{{ run_id }}",
    run_id="{{ run_id }}",
    matcher_env="dev",
    matcher_task_factory=spark_matcher_task_factory(
        spark_impl_name="GLUE_v5",
        sedona_version="1.7.0",
        spark_jar_paths="s3://example-bucket/jars/matching-shaded.jar",
        spark_cluster_desired_worker_cores="400",
        package_registry=package_registry,
        artifact_store=artifact_store,
        glue_config=glue_config,
    ),
    corpus=CorpusDagConfig(
        export_dag_id="example_corpus_export",
        load_dag_id="example_corpus_load",
        export_output_task_id="output",
    ),
    output_corpus_branch="main",
    provenance_task_factory=publish_matching_metadata,
)

normalize >> matching.group
finalize = finalize_source(matching.complete)
```

`normalize`, `finalize_source`, `publish_matching_metadata` and the Spark config
objects are supplied by the consuming DAG. See [`examples/example_matching_dag.py`](examples/example_matching_dag.py)
for a credential-free-to-parse, multi-provider scaffold with explicit adapter
placeholders. Source discovery, unchanged-data gates, normalization, provider
ordering/concurrency, source success markers, and assemble remain consumer-owned.
Place the gate before the **whole group**, and finalize only after `complete`.
Use a failure-sensitive end barrier (for example `NONE_FAILED`) so all-unchanged
is successful but failed provider processing blocks assembly.

### Matcher, inputs and Corpus

| Argument | Role |
|---|---|
| `matcher_task_factory` | Builds the matcher job at DAG construction. It is called with the keyword XCom argument `parameters` (the matcher's argument mapping) and must return the task output holding the result paths: a `{table: path}` mapping or its JSON string. |
| `spark_matcher_task_factory(...)` | Ready-made factory for the published Scala matcher, `org.overturemaps.matching.Main`. Every keyword is forwarded unchanged to the Spark task group (JARs, Sedona version, sizing, retries, pools, Spark settings, platform config), so nothing is defaulted here. To keep a deployment's own defaults and extra links, pass its wrapper around `spark_agnostic_task_group` as the first argument. `extra_files` becomes the job's `--extra-files` (for example libpostal for places). |
| `CorpusDagConfig` | Names deployed export/load DAGs and the export result task/key; it does not deploy Corpus or import its DAG modules. Triggers wait for completion. `spark_impl_name` independently selects child-DAG execution; it is not inferred from the matcher. |

`spark_matcher_task_factory` reads the matcher's result manifest from S3 and is
written for Glue, the only platform the matcher's Spark entry point runs on
today. The generic Spark API's Databricks and Wherobots support does not
establish matcher compatibility. To run the matcher another
way, for example an existing Databricks job through `DatabricksRunNowOperator`,
supply your own `matcher_task_factory`. `matching.matcher` is the matcher's task
group; with `spark_matcher_task_factory` the Spark job is the group
`f"{matching.matcher.group_id}.spark"`.

`matcher_env` is required and is passed to the matcher as `environment`.

Input and baseline accept native or JSON objects: either `{"branch": "...",
"version": "..."}` (version optional) or `{"override_paths": {"table": "path"}}`.
Explicit `tables` are required; maps must contain exactly those tables, with
nonempty paths. Branch selections require Corpus configuration and the
`overture` scenario. A version is always sent to the Corpus DAGs as a string,
including when it was given as a number or the DAG renders templates as native
objects. With `combine_output=False` the export writes active and deleted
records separately and the matcher receives only the active path. The export
result contract is `{"data_export": [{"TableName": "...", "exportPath": "..."}]}`
for each mapped child. Reads use that child's exact DAG/run/task/key, never the
latest run.

The exports of one input or baseline selection run one at a time. If the export
DAG cannot run two exports at once, also give `export_pool` a one-slot pool so
selections and instances queue behind each other. Triggers block a worker while
they wait (`deferrable=False`), which holds the slot. With `deferrable=True` the
pool must count deferred tasks, or the slot is released as soon as a trigger
defers.

Matcher arguments may be templated; the matcher job is built at DAG
construction. Supply a **unique output root per invocation**, including
provider/instance and run identity. A unique `run_id` alone is insufficient:
`spark_matcher_task_factory` reads its manifest from
`<output_path>/<output_result_file>` (default `data_paths.json`), not from the
matcher run subdirectory. Table results must share the legacy
`<metadata_root>/final/type=<table>` layout.

An empty `output_corpus_branch` or a non-`overture` scenario is match-only.
`complete` returns `{"loaded": bool, "result_paths": {...},
"corpus_run_ids": [...]}`; match-only success has `loaded=False` and no child
load IDs. `result_paths` alone is **not** evidence that loads completed.
By default the target branch must already exist. With `create_branch=True`
(which requires `create_branch_dag_id`) the create-branch DAG is triggered for
every non-default target branch, so that DAG must succeed when the branch
already exists. A failed or skipped creation blocks the loads.

### Provenance and pre-match hooks

When loading can be enabled, `provenance_task_factory` is required. This is
checked when the DAG is built and again at run time, once a templated scenario
has rendered, so no load or branch creation is requested without it. It is called
at DAG construction with keyword XCom arguments `input_paths`, `baseline_paths`,
`result_paths`, `metadata_root`, and `parameters`, and must return the
**publishing task's XComArg**. That task writes the matching metadata at
`metadata_root` and returns the root:

```python
@task
def publish_matching_metadata(input_paths, baseline_paths, result_paths, metadata_root, parameters):
    write_matching_metadata(metadata_root, input_paths, baseline_paths, result_paths, parameters)
    return metadata_root
```

Loading starts only after this task succeeds and returns the expected root. A
failed, skipped or mismatched publication blocks it. What goes into the metadata
(wildcard expansion, upstream lineage, license attribution, redaction) is the
publisher's responsibility, and so is how strictly it treats upstream metadata
it cannot read: raise from the task if a missing lineage entry should stop the
load.

`pre_match_task_factory` adds an optional check that must succeed before the
matcher runs. The matcher always waits for it and for input validation, whether
or not its factory uses the `parameters` it is given. It receives `input_paths`
and `baseline_paths` and returns a task
XComArg or TaskGroup. Places uses it to validate the embeddings cache it passes
as `embeddings_cache_table`; the validation job and its package versions stay in
the consumer. `match_history` and `output_debug` retain unset/true/false
semantics; `previous_metrics_path`, `expiration_period_days`, `extra_files`,
JARs and tuning remain explicit. No places-specific deployment defaults are
inferred.

### Failure handling and migration

Missing or invalid matcher results fail before loading. Unexpected pre-match,
matcher or provenance skips also fail closed. Completion requires every mapped
load to succeed; failed or unexpectedly skipped export, branch-creation or load
tasks cannot release source finalization. An intentionally unchanged provider
remains skipped
throughout its group. Contract violations raise `AirflowFailException`, so they
are not retried even when the DAG sets default retries; infrastructure errors
still follow the DAG's retry policy. Result paths, export paths and child run
IDs are written to the task logs.

Loads are separate transactions, not an atomic multi-table update. Child run
IDs are deterministic per parent DAG/run/task and configuration, and triggers
have zero retries. To retry after a child run fails, fix the cause and clear the
failed trigger task (`export`, `create_branch` or `load`): the same child run is
cleared and rerun, and the tasks downstream pick up its results. Clearing a
trigger that already succeeded reruns its child run too, so clearing a load
replays that load, and clearing the whole DAG run reruns every child run under
its original ID.

Release this additive provider API first, update the deployment's pinned
provider dependency, then migrate and validate consumers. Keep existing loose
helpers and places DAGs until equivalent behavior is demonstrated. This API
does not itself change divisions ingest/orchestrator DAGs or make old ingest
success markers proof of successful matching/loading.

## Operator links

### Job console link

The `execute_spark_job` task automatically gets a "Spark Job" link in the Airflow UI that opens the platform's job-run console (Glue, Databricks, or Wherobots). No configuration is needed; the link is attached automatically when the provider is installed.

### Report Issue link

`ReportIssueConfig` adds a "Report Issue" button to the execute task that opens a pre-filled GitHub issue form when a job fails. The link is push-based: the config is written to XCom at task start, so the button renders even if the run fails mid-flight.

```python
from overture_airflow_provider import ReportIssueConfig, spark_agnostic_task_group

spark_agnostic_task_group(
    "my_glue_job",
    spark_impl_name="GLUE_v5",
    report_issue_config=ReportIssueConfig(
        enabled=True,
        target="my-org/my-repo",  # GitHub owner/repo
        labels=["spark-failure"],  # optional labels pre-applied to the issue
    ),
    # ... rest of config unchanged
)
```

The button is off by default (`enabled=False`). Only `"github"` ships built in; additional trackers are pluggable via `_report_issue.IssueTracker` and `register_tracker` without touching the link or operator code.

## Failure messages

When a Spark job fails, the provider emits a classified error instead of a raw platform exception. Every failure includes a category, a reason, an optional root-cause tail, an actionable hint, and a direct console link where available:

```
Spark job FAILED on GLUE (downstream job error, not a provider/submit fault).
  run:     jr_abc1234567890      state: FAILED
  reason:  Job run failed with exit code 1
  cause:   java.lang.OutOfMemoryError: Java heap space
  hint:    OOM: increase worker size/cores or reduce partition size.
  console: https://us-east-1.console.aws.amazon.com/glue/home#/etl/jobs/run/details/jr_abc1234567890
```

Classifications: `downstream-job`, `submit/config`, `trigger/polling`, `platform/infra`, `platform/capacity`.

The hint layer scans the reason and root-cause text for known patterns (IAM denials, auth errors, OOM, throttling, missing resources, Glue capacity rejections) and appends an actionable message automatically.

### Retries

`execute_spark_job` defaults to `retries=0`: a job that fails costs real compute to re-run, so this provider doesn't retry by default.

Every failure classification above already carries a `run_launched` signal, whether the job reached the platform before it failed. Raising `retries` is a small, safe adjustment on top of that existing signal: the exception type just follows it.

#### What's retryable

`submit/config` failures raise the retryable `AirflowException`. The run never reached the platform (a worker recycling mid-submit, a bad cluster policy), so nothing ran and nothing costs money to redo.

`platform/capacity` failures are retryable too, for the same reason. On Glue, a run submitted with `JobRunQueuingEnabled` waits in Glue's run queue for about 15 minutes; if the account's DPU ceiling never frees up, Glue fails the run with `ErrorMessage: "Exceeded maximum concurrent compute"` and `ExecutionTime: 0`. The run has an id, but no job code ran and nothing was written, so the provider raises `RetryableJobFailure` (an `AirflowException` subclass) instead of `AirflowFailException`:

```
Spark job FAILED on GLUE (platform capacity failure: the run never started, safe to retry).
  run:     jr_abc1234567890      state: FAILED
  reason:  Exceeded maximum concurrent compute
  hint:    Glue capacity: the account's DPU ceiling was hit. A run with ExecutionTime 0 never started and is safe to retry; raise retries on execute_spark_job or stagger concurrent submits.
  console: https://us-west-2.console.aws.amazon.com/glue/home#/etl/jobs/run/details/jr_abc1234567890
```

With `retries=1`, one queue timeout becomes an effective 30-minute wait for capacity, which is usually enough for a nightly fan-out peak to drain. The detection is Glue-only and requires both the capacity error message and `ExecutionTime` of 0 (or missing): a run that carries the same message but actually executed stays `downstream-job` and is never retried. The retry try's pre-submit stale-run scan only looks at active runs, so the FAILED capacity run is left alone and a fresh run is submitted.

#### What isn't

`downstream-job` and `trigger/polling` failures raise `AirflowFailException`, which Airflow never retries regardless of the task's `retries=`. The run launched, then failed, or a Triggerer crash happened mid-poll after launch, so re-running it blindly burns another full job at cost instead of fixing anything.

Set `retries=1` (or higher) on `execute_spark_job` to recover automatically from transient submission-time infra faults and Glue capacity rejections without risking a silent, full-cost retry of a job that actually ran and failed.

## Wherobots runtime selection

On Wherobots, `spark_cluster_desired_worker_cores` picks a general-purpose runtime (`small` through `2x-large`). To run a task group on a specific runtime instead, such as a memory-optimized one for a job whose executors run out of memory, set `spark_cluster_size_name`:

```python
spark_agnostic_task_group(
    group_id="my_spark_job",
    spark_impl_name="WHEROBOTS_v1_5_0",
    spark_cluster_desired_worker_cores="1000",  # still sizes Glue / Databricks runs
    spark_cluster_size_name="large-himem",  # Wherobots only
    # ... rest of config unchanged
)
```

`spark_cluster_size_name` takes a `ClusterSize` name (`"XS"` to `"XXL"`) or any runtime name the installed `wherobots` SDK defines, such as `"large-himem"`, `"2x-large-himem"`, or `"x-large-hicpu"`. Matching is case-insensitive, and the kwarg is templated. When set, it wins over `spark_cluster_desired_worker_cores` on Wherobots. Glue and Databricks ignore it, so the same DAG keeps running unchanged on those platforms. An unknown name fails `execute_spark_job` before anything is submitted, and the error lists the valid names. Your Wherobots organization needs access to the chosen runtime.

## Databricks runner deployment

A Databricks run needs two workspace assets: the runner notebook (`job_runner_databricks`) and the cluster init script (`agnostic_operator_cluster_init_databricks.sh`). Both ship with the provider.

The `setup_cluster` task uploads both on the worker at execute time, the way Glue and Wherobots runners get staged to S3. Nothing runs at DAG parse time. Each asset lands at a content-hash-keyed path under `DatabricksConfig.workspace_scripts_path_template` (after `{s3_assets_root}` substitution):

- `{scripts_path}/runners/{sha256[:12]}-job_runner_databricks`
- `{scripts_path}/runners/{sha256[:12]}-agnostic_operator_cluster_init_databricks.sh`

The run's `notebook_path` and `init_scripts` point at those paths. A `workspace get-status` check skips any path that already exists. A concurrent upload of the same hash counts as success. A new provider version writes new paths instead of overwriting files that in-flight clusters, or another environment sharing the workspace, still use.

The upload uses the same Airflow connection as the submit path (`DatabricksConfig.cluster_conf["databricks_conn_id"]`), through `DatabricksSdkHook`. All of its auth modes work: PAT, OAuth M2M, Azure service principal, and federated OIDC.

> [!IMPORTANT]
> The connection's identity needs write access to `{scripts_path}/runners`, since the task creates that folder and imports into it. The default `/Shared/{s3_assets_root}` path needs no extra grant: Databricks gives every workspace user `CAN MANAGE` on `/Shared`. A custom path outside `/Shared` needs an explicit `CAN_EDIT` or `CAN_MANAGE` grant for that identity.

### Using pre-deployed assets

Set `DatabricksConfig(stage_workspace_assets=False)` to keep deploying the assets yourself, for example when Airflow workers hold no workspace write credentials. The run then references the fixed paths `{scripts_path}/job_runner_databricks` and `{scripts_path}/{cluster_init_script_name}`, with no workspace call at setup time. Deploy them via CI/CD or the bundled helpers:

```python
from overture_airflow_provider.runner_assets import (
    upload_databricks_init_script_to_workspace,
    upload_databricks_runner_to_workspace,
)

host, token = "https://my-workspace.cloud.databricks.com", "dapi..."
upload_databricks_runner_to_workspace(host, token, "/Shared/<s3_assets_root>/job_runner_databricks")
upload_databricks_init_script_to_workspace(
    host, token, "/Shared/<s3_assets_root>/agnostic_operator_cluster_init_databricks.sh"
)
```

A non-default `DatabricksConfig.cluster_init_script_name` means you supply your own init script. The provider never stages it, even with staging on, and references it at `{scripts_path}/{cluster_init_script_name}`. The runner notebook is still staged.

A missing asset surfaces as a Databricks cluster-launch error at run time, naming the missing path.

> While a Databricks job is deferred, the Triggerer may log `aiohttp` "Unclosed client session / connector" ERROR lines. These originate in the upstream `apache-airflow-providers-databricks` `DatabricksExecutionTrigger` (its async client is not explicitly closed on the event loop), not in this provider. The task defers, polls, and resumes correctly regardless. This provider deliberately reuses the installed trigger and does not fork it to silence the message.

## Local rendering

The `overture_airflow_provider.render` module produces the platform submission payload without importing or running any Airflow operators. Use it to drive real cloud resources from the CLI, or to snapshot-test payload shape in CI.

```bash
# Render the payload to stdout as JSON.
uv run python -m overture_airflow_provider.render \
    --spark-impl GLUE_v5 --module-name my_module --class-name MyJob

# Render to a directory and emit an executable cli.sh.
uv run python -m overture_airflow_provider.render \
    --spark-impl GLUE_v5 --module-name my_module --class-name MyJob \
    --out ./rendered/

bash ./rendered/cli.sh   # invokes aws glue create-job / start-job-run
```

You can also drive it from Python:

```python
from overture_airflow_provider import render_spark_job

result = render_spark_job(
    spark_impl_name="DATABRICKS_v15",
    module_name="my_module",
    class_name="MyJob",
    parameters={"date": "2024-01-01"},
)
print(result.submit_payload)  # equivalent to `databricks jobs submit --json`
print(result.operator_kwargs)  # what the Airflow operator would receive
result.write_to("./out/")  # dump JSON payloads + cli.sh
```

Pass `pre_resolved_package_info=` or `pre_resolved_jar_info=` with real S3 URIs from a previous `download_python_packages_*` or `download_jars_*` run to skip the `s3://.../REPLACE-ME.whl` placeholders.

## Bundle Inspector plugin

Installing this package registers `bundle_inspector`, an Airflow plugin (not related to `spark_agnostic_task_group`) for browsing Overture bundles in S3 by pipeline stage, theme, schema version, and run ID. It is served as an Airflow 3 FastAPI app. No plugins-folder drop-in needed, no separate install step: it ships in this package and shows up under **Browse -> Bundle Inspector** once the provider is installed.

Configure it via the `[bundle_inspector]` section in `airflow.cfg`:

| Option | Required | Description |
|--------|----------|-------------|
| `s3_bucket` | yes | S3 bucket containing bundle data |
| `environment` | no (default `dev`) | Environment name (`dev`, `staging`, `prod`) |
| `athena_output_bucket` | no | S3 bucket for Athena query results; falls back to `overture-bundle-inspector-athena-output-<environment>` |
| `user` | no | Namespace prefix under the bucket, read only when `environment` is `dev` |

Each option is also settable via its `AIRFLOW__BUNDLE_INSPECTOR__<OPTION>` environment variable (e.g. `AIRFLOW__BUNDLE_INSPECTOR__S3_BUCKET`), Airflow's standard override convention for provider config, so no metadata DB round-trip or Variable setup is needed to get the plugin running.

GeoParquet preview (`component-data`, `parquet-stats`) needs `pyarrow` and `shapely`: `pip install "airflow-provider-overture[bundle-inspector]"`. Without them those two endpoints return a 500 with the import error; the rest of the UI (tree browsing, PMTiles, CSV, Athena queries) works without either.

The UI loads its JS libraries from `cdn.jsdelivr.net` and its map basemap from [OpenFreeMap](https://openfreemap.org) (`tiles.openfreemap.org`, Positron style, no API key), so users' browsers need access to both hosts. If the basemap can't be reached, maps fall back to a blank background and still draw their data.

## Reference

### Supported versions

#### Provider requirements

|                | Minimum | Also tested |
| -------------- | ------- | ----------- |
| Python         | 3.11    | 3.12, 3.13  |
| Apache Airflow | 3.3     |             |

#### Spark platform matrix

Pass one of these names as `spark_impl_name`:

| `spark_impl_name`  | Platform                    | Spark | Scala | Python runtime |
| ------------------ | --------------------------- | ----- | ----- | -------------- |
| `GLUE_v4`          | AWS Glue 4.0                | 3.3.0 | 2.12  | 3.10           |
| `GLUE_v5`          | AWS Glue 5.0                | 3.5.2 | 2.12  | 3.11           |
| `DATABRICKS_v14`   | Databricks Runtime 14.3 LTS | 3.5.0 | 2.12  | 3.10.12        |
| `DATABRICKS_v15`   | Databricks Runtime 15.4 LTS | 3.5.0 | 2.12  | 3.11.0         |
| `WHEROBOTS_v1_5_0` | Wherobots Cloud 1.5.0       | 3.5.0 | 2.12  | 3.11           |

> `SYNAPSE_v3_3_1` and `SYNAPSE_v3_4_1` are defined but not yet active (Azure Synapse support reserved).

#### Apache Sedona

Sedona JARs are resolved from Maven Central at runtime. Tested pairings and the minimum Spark version required:

| Sedona | geotools-wrapper | Min Spark               |
| ------ | ---------------- | ----------------------- |
| 1.5.3  | 28.2             | 3.3                     |
| 1.6.1  | 28.2             | 3.3                     |
| 1.7.0  | 28.5             | 3.3                     |
| 1.7.1  | 28.5             | 3.3                     |
| 1.7.2  | 28.5             | 3.3                     |
| 1.8.1  | 33.1             | 3.4 (Spark 3.3 dropped) |
| 1.9.0  | 33.5             | 3.4 (Spark 3.3 dropped) |

## Development

```bash
uv sync --all-extras --group dev
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

Requires Airflow 3.x. [`_airflow_compat.py`](src/overture_airflow_provider/_airflow_compat.py) is the single import point for the `airflow.sdk` symbols the provider uses.

> Windows: Apache Airflow does not officially support Windows (a warning is emitted at import time). Tests, lint, and the render module all work, but production deployments should run on Linux or macOS.

See [`CONTRIBUTING.md`](CONTRIBUTING.md).