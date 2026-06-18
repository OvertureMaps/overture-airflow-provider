# overture-airflow-provider

[![PyPI version](https://img.shields.io/pypi/v/airflow-provider-overture.svg)](https://pypi.org/project/airflow-provider-overture/)
[![Python versions](https://img.shields.io/pypi/pyversions/airflow-provider-overture.svg)](https://pypi.org/project/airflow-provider-overture/)
[![License: MIT](https://img.shields.io/pypi/l/airflow-provider-overture.svg)](LICENSE)

An Apache Airflow provider for running PySpark or Scala/Spark jobs on AWS Glue, Databricks, or Wherobots Cloud from a single DAG-level API.

Write your DAG once and target any supported engine by switching one argument. Cluster shape, Iceberg catalog wiring, JAR and wheel distribution, and per-platform cluster init are all handled by the provider.

The provider is intentionally unopinionated: every environment-specific value (S3 buckets, IAM roles, catalog endpoints, package registries) is passed in via typed config dataclasses. No org-specific defaults are baked in.

> Beta. Tested against real Airflow 2.11 and 3.0 via Docker e2e.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Operator links](#operator-links)
- [Failure messages](#failure-messages)
- [Databricks runner deployment](#databricks-runner-deployment)
- [Local rendering](#local-rendering)
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

Requires Python `>=3.11` and Apache Airflow `>=2.11`.

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
        target="my-org/my-repo",   # GitHub owner/repo
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

Classifications: `downstream-job`, `submit/config`, `trigger/polling`, `platform/infra`.

The hint layer scans the reason and root-cause text for known patterns (IAM denials, auth errors, OOM, throttling, missing resources) and appends an actionable message automatically.

## Databricks runner deployment

Glue and Wherobots runner scripts are uploaded to S3 automatically during task-group setup. The Databricks runner is a Workspace Notebook that must be deployed once before your first run. The provider references it at submit time but does not push it, because notebook deployment requires Workspace API credentials that many teams keep in CI/CD rather than on Airflow workers.

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

Both the runner notebook and the cluster init script must be present before the run. If either is missing, the Databricks run fails at cluster launch with Databricks' own error pointing at the missing asset.

### Cluster init script

A Databricks run requires two workspace assets in the `workspace_scripts_path_template` folder:

1. the runner notebook (`job_runner_databricks`, above), and
2. the cluster init script named by `DatabricksConfig.cluster_init_script_name` (default `agnostic_operator_cluster_init_databricks.sh`), wired into the cluster's `init_scripts`.

The init script is not bundled with the provider; deploy it to the same workspace folder via CI/CD. A missing init script surfaces as a Databricks cluster-launch error at run time.

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
print(result.submit_payload)        # equivalent to `databricks jobs submit --json`
print(result.operator_kwargs)       # what the Airflow operator would receive
result.write_to("./out/")           # dump JSON payloads + cli.sh
```

Pass `pre_resolved_package_info=` or `pre_resolved_jar_info=` with real S3 URIs from a previous `download_python_packages_*` or `download_jars_*` run to skip the `s3://.../REPLACE-ME.whl` placeholders.

## Reference

### Supported versions

#### Provider requirements

|                | Minimum | Also tested |
| -------------- | ------- | ----------- |
| Python         | 3.11    | 3.12, 3.13  |
| Apache Airflow | 2.11    | 3.x         |

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

Supports Airflow 2.11.x and 3.x via a compat shim ([`_airflow_compat.py`](src/overture_airflow_provider/_airflow_compat.py)) that re-exports `DAG`, `task`, `task_group`, and `BaseHook` from whichever location exists on the installed Airflow. When dropping 2.x support, simplify the shim to the `airflow.sdk` imports.

> Windows: Apache Airflow does not officially support Windows (a warning is emitted at import time). Tests, lint, and the render module all work, but production deployments should run on Linux or macOS.

See [`CONTRIBUTING.md`](CONTRIBUTING.md).