# overture-airflow-provider

[![PyPI version](https://img.shields.io/pypi/v/airflow-provider-overture.svg)](https://pypi.org/project/airflow-provider-overture/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-provider-overture.svg)](https://pypi.org/project/airflow-provider-overture/)
[![License: MIT](https://img.shields.io/pypi/l/airflow-provider-overture.svg)](LICENSE)

An Apache Airflow provider exposing a **Spark-agnostic task group** that runs
PySpark or Scala/Spark jobs on AWS Glue, Databricks, or Wherobots Cloud — from
a single, unified DAG-level API.

Write your DAG once, target any of the supported engines by switching a single
`spark_impl` argument. Cluster shape, Iceberg catalog wiring, JAR / wheel
distribution, and per-platform cluster init are all handled for you.

This project is OSS and intentionally unopinionated: every environment-specific
value (S3 buckets, IAM roles, catalog endpoints, package registries) is passed
in via typed config dataclasses. No defaults are baked in for any one
organization.

> **0.3.1** — Beta. Tested against real Airflow 2.11 + 3.0 via Docker e2e.

## Install

```bash
pip install airflow-provider-overture
```

Optional extras for platforms that need extra SDKs:

```bash
pip install "airflow-provider-overture[databricks]"
pip install "airflow-provider-overture[wherobots]"
pip install "airflow-provider-overture[all]"
```

Requires Python `>=3.11` and Apache Airflow `>=2.11`.

## Supported versions

### Provider requirements

|                | Minimum | Also tested |
| -------------- | ------- | ----------- |
| Python         | 3.11    | 3.12, 3.13  |
| Apache Airflow | 2.11    | 3.x         |

### Spark platform matrix

Pass one of these names as `spark_impl_name`:

| `spark_impl_name`  | Platform                    | Spark | Scala | Python runtime |
| ------------------ | --------------------------- | ----- | ----- | -------------- |
| `GLUE_v4`          | AWS Glue 4.0                | 3.3.0 | 2.12  | 3.10           |
| `GLUE_v5`          | AWS Glue 5.0                | 3.5.2 | 2.12  | 3.11           |
| `DATABRICKS_v14`   | Databricks Runtime 14.3 LTS | 3.5.0 | 2.12  | 3.10.12        |
| `DATABRICKS_v15`   | Databricks Runtime 15.4 LTS | 3.5.0 | 2.12  | 3.11.0         |
| `WHEROBOTS_v1_5_0` | Wherobots Cloud 1.5.0       | 3.5.0 | 2.12  | 3.11           |

> `SYNAPSE_v3_3_1` / `SYNAPSE_v3_4_1` are defined but not yet active (Azure Synapse support reserved).

### Apache Sedona (optional)

Sedona JARs are resolved from Maven Central at runtime. Tested pairings and the
minimum Spark version required:

| Sedona | geotools-wrapper | Min Spark               |
| ------ | ---------------- | ----------------------- |
| 1.5.3  | 28.2             | 3.3                     |
| 1.6.1  | 28.2             | 3.3                     |
| 1.7.0  | 28.5             | 3.3                     |
| 1.7.1  | 28.5             | 3.3                     |
| 1.7.2  | 28.5             | 3.3                     |
| 1.8.1  | 33.1             | 3.4 (Spark 3.3 dropped) |
| 1.9.0  | 33.5             | 3.4 (Spark 3.3 dropped) |

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

Switching to Databricks or Wherobots is just a different `spark_impl_name` plus
the corresponding `DatabricksConfig` / `WherobotsConfig` dataclass — the
surrounding DAG code does not change.

See [`examples/example_dag.py`](examples/example_dag.py) for a runnable DAG
that targets all three platforms.

See [`SPEC.md`](SPEC.md) for the full architecture.

## Operator links

### Job console link (`SparkJobLink`)

The `execute_spark_job` task automatically gains a **"Spark Job"** link in the
Airflow UI that opens the platform's job-run console (Glue, Databricks, or
Wherobots). No configuration needed — it's attached to the task automatically
when the provider is installed.

### Report Issue link (opt-in)

Add a **"Report Issue"** button to the execute task that opens a pre-filled
GitHub issue form whenever a job fails. The link is push-based — the config
is written to XCom at task start — so the button renders even when the run
fails mid-flight.

```python
from overture_airflow_provider import ReportIssueConfig, spark_agnostic_task_group

spark_agnostic_task_group(
    "my_glue_job",
    spark_impl_name="GLUE_v5",
    report_issue_config=ReportIssueConfig(
        enabled=True,
        target="my-org/my-repo",   # GitHub owner/repo
        labels=["spark-failure"],  # optional labels pre-applied to the issue
    ),
    # ... rest of config unchanged
)
```

The button is off by default (`enabled=False`). Only `"github"` ships built
in; additional trackers are pluggable via `_report_issue.IssueTracker` +
`register_tracker` without touching the link or operator code.

## Failure messages

When a Spark job fails, the provider emits a **classified** error instead of a
raw platform exception. Every failure includes a category, summary, retry hint,
and (where available) the platform exit code — so the Airflow task log is
self-contained without requiring a separate visit to the platform console:

```
[spark/job] Glue job run jr_abc123 did not succeed (state: FAILED)
  category : job/exit-code
  exit_code: 1
  summary  : Job exited with a non-zero exit code. Check the job log for the
             root cause; this is usually an application error, not a platform error.
  retry    : fix the application code; a bare retry is unlikely to help
  job_url  : https://us-east-1.console.aws.amazon.com/glue/home#/job/run/jr_abc123
```

Categories: `job/exit-code`, `job/oom`, `job/timeout`, `job/cancelled`,
`submit/config`, `trigger/polling`, and platform-specific variants.

## Databricks runner deployment

Unlike Glue and Wherobots — whose bundled runner scripts are auto-uploaded to
S3 during task-group setup — the **Databricks runner is a Workspace Notebook
that must be deployed once, out-of-band**, before your first run. The provider
references it at submit time but does not push it for you (notebook deployment
needs Workspace API credentials many teams keep in CI/CD, not on Airflow
workers).

Deploy it via your CI/CD pipeline or the bundled helper:

```python
from overture_airflow_provider.runner_assets import (
    upload_databricks_runner_to_workspace,
)

upload_databricks_runner_to_workspace(
    databricks_host="https://my-workspace.cloud.databricks.com",
    databricks_token="dapi...",  # PAT or CI/CD secret
    # Must match DatabricksConfig.workspace_scripts_path_template (after
    # {s3_assets_root} substitution) + "/job_runner_databricks".
    workspace_path="/Shared/<s3_assets_root>/job_runner_databricks",
)
```

Both the runner notebook and the cluster init script must be present in the
workspace before the run. If either is missing the Databricks run fails at
cluster launch with Databricks' own authoritative error pointing at the
missing asset.

### Cluster init script

A Databricks run requires **two** workspace assets in the
`workspace_scripts_path_template` folder:

1. the runner notebook (`job_runner_databricks`, above), and
2. the **cluster init script** named by
   `DatabricksConfig.cluster_init_script_name` (default
   `agnostic_operator_cluster_init_databricks.sh`), wired into the cluster's
   `init_scripts`.

The init script is **not** bundled with the provider — its contents are
platform/CI-owned — so deploy it to the same workspace folder via your CI/CD
pipeline. A missing init script surfaces authoritatively as a Databricks
cluster-launch error at run time.

> **Note (upstream log noise):** while a Databricks job is deferred, the
> Triggerer may log `aiohttp` "Unclosed client session / connector" *ERROR*
> lines. These originate in the upstream
> `apache-airflow-providers-databricks` `DatabricksExecutionTrigger` (its async
> client is not explicitly closed on the event loop), not in this provider. The
> task defers, polls, and resumes correctly regardless. This provider
> deliberately reuses the installed provider's trigger, so it does not fork the
> trigger to silence the message.

## Local rendering (testing without Airflow)

The `overture_airflow_provider.render` module produces the exact platform
submission payload that the task group would emit, **without** importing or
executing any Airflow operators. Use it to drive real cloud resources from
the CLI, or to snapshot-test payload shape in CI.

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

You can also drive it programmatically:

```python
from overture_airflow_provider import render_spark_job

result = render_spark_job(
    spark_impl_name="DATABRICKS_v15",
    module_name="my_module",
    class_name="MyJob",
    parameters={"date": "2024-01-01"},
)
print(result.submit_payload)        # equivalent to `databricks jobs submit --json`
print(result.operator_kwargs)       # what the Airflow operator would receive
result.write_to("./out/")           # dump JSON payloads + cli.sh
```

Pass `pre_resolved_package_info=` / `pre_resolved_jar_info=` with real S3
URIs from a previous `download_python_packages_*` / `download_jars_*` run if
you want to skip the `s3://.../REPLACE-ME.whl` placeholders.

## Development

```bash
uv sync --all-extras --group dev
uv run pytest -v
uv run ruff check .
uv run ruff format --check .
```

> **Airflow version:** supports Airflow **2.11.x** and **3.x** via a tiny
> compat shim ([`_airflow_compat.py`](src/overture_airflow_provider/_airflow_compat.py))
> that re-exports `DAG`, `task`, `task_group`, and `BaseHook` from
> whichever location exists on the installed Airflow. When dropping 2.x
> support, simplify the shim to just the `airflow.sdk` imports (or inline
> them).
>
> **Windows note:** Apache Airflow does not officially support Windows
> (warning emitted at import time). Tests, lint, and the render module all
> work, but production deployments should run on Linux or macOS.

See [`CONTRIBUTING.md`](CONTRIBUTING.md).

