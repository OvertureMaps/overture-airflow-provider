![overture-airflow-provider: cloud-agnostic Spark for Apache Airflow](https://raw.githubusercontent.com/OvertureMaps/overture-airflow-provider/main/docs/assets/hero.svg)

[![Test](https://github.com/OvertureMaps/overture-airflow-provider/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/OvertureMaps/overture-airflow-provider/actions/workflows/test.yml)
[![Lint](https://github.com/OvertureMaps/overture-airflow-provider/actions/workflows/lint.yml/badge.svg?branch=main)](https://github.com/OvertureMaps/overture-airflow-provider/actions/workflows/lint.yml)
[![PyPI](https://img.shields.io/pypi/v/airflow-provider-overture?color=4051CC&labelColor=001A39)](https://pypi.org/project/airflow-provider-overture/)
[![Python](https://img.shields.io/pypi/pyversions/airflow-provider-overture?color=4051CC&labelColor=001A39)](https://pypi.org/project/airflow-provider-overture/)
[![Airflow 3.3+](https://img.shields.io/badge/Airflow-3.3%2B-0EC1BD?labelColor=001A39)](https://airflow.apache.org/)
[![uv](https://img.shields.io/badge/managed%20with-uv-0EC1BD?labelColor=001A39)](https://docs.astral.sh/uv/)
[![Ruff](https://img.shields.io/badge/lint-ruff-0EC1BD?labelColor=001A39)](https://docs.astral.sh/ruff/)
[![License: MIT](https://img.shields.io/badge/license-MIT-05A5AF?labelColor=001A39)](LICENSE)

An Apache Airflow provider for running PySpark or Scala/Spark jobs on AWS Glue, Databricks, or Wherobots Cloud from a single DAG-level API.

Write a DAG once and target any supported engine by switching one argument. Cluster shape, Iceberg catalog wiring, JAR and wheel distribution, and per-platform cluster init are handled by the provider.

## Features

- Single entry point: `spark_agnostic_task_group` runs the same job on any supported engine; only `spark_impl_name` and the platform config change.
- Typed configuration: no org-specific defaults; every environment value is passed in.
- Classified failures with safe retries and console links.
- Local rendering of submission payloads, plus a Bundle Inspector Airflow UI plugin.

## Contents

- [Install](#install)
- [Quick start](#quick-start)
- [Operator links](#operator-links)
- [Failure messages](#failure-messages)
- [Wherobots runtime selection](#wherobots-runtime-selection)
- [Databricks runner deployment](#databricks-runner-deployment)
- [Local rendering](#local-rendering)
- [Bundle Inspector plugin](#bundle-inspector-plugin)
- [Reference](#reference)
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

## Operator links

The `execute_spark_job` task gets a "Spark Job" console link automatically, plus an opt-in, pluggable "Report Issue" link. See [`docs/operator-links.md`](docs/operator-links.md).

## Failure messages

Failures are classified (`downstream-job`, `submit/config`, `trigger/polling`, `platform/infra`, `platform/capacity`) with a reason, root-cause tail, hint, and console link. Submit and capacity faults are retryable via `retries=`; jobs that ran and failed never are. See [`docs/failure-messages.md`](docs/failure-messages.md).

## Wherobots runtime selection

Set `spark_cluster_size_name` (for example `"large-himem"`) to pick a specific Wherobots runtime; Glue and Databricks ignore it. See [`docs/wherobots-runtime-selection.md`](docs/wherobots-runtime-selection.md).

## Databricks runner deployment

`setup_cluster` stages the runner notebook and init script into the workspace by default; `stage_workspace_assets=False` keeps deployment manual. See [`docs/databricks-runner-deployment.md`](docs/databricks-runner-deployment.md).

## Local rendering

`python -m overture_airflow_provider.render` produces the platform submission payload without running Airflow. See [`docs/local-rendering.md`](docs/local-rendering.md).

## Bundle Inspector plugin

Installing the package also registers an Airflow UI plugin for browsing Overture bundles in S3. See [`docs/bundle-inspector.md`](docs/bundle-inspector.md).

## Reference

Supported versions, the Spark platform matrix (`GLUE_v4`/`GLUE_v5`, `DATABRICKS_v14`/`DATABRICKS_v15`, `WHEROBOTS_v1_5_0`) and tested Apache Sedona pairings are in [`docs/reference.md`](docs/reference.md).

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