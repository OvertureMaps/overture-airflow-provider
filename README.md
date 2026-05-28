# overture-airflow-provider

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

> Status: `0.1.0` — initial MVP. Unit + mock test coverage only; live-platform
> E2E tests are tracked as a follow-up.

## Install

```bash
pip install overture-airflow-provider
```

Optional extras for platforms that need extra SDKs:

```bash
pip install "overture-airflow-provider[databricks]"
pip install "overture-airflow-provider[wherobots]"
pip install "overture-airflow-provider[all]"
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
            s3_assets_bucket="example-bucket",
            s3_assets_root="spark-agnostic-operator",
        ),
        package_registry=PackageRegistryConfig(
            codeartifact_domain="my-domain",
            codeartifact_domain_owner="123456789012",
            codeartifact_repository="my-pypi",
            codeartifact_region="us-east-1",
        ),
        glue=GlueConfig(iam_role_name="AWSGlueServiceRole"),
        iceberg=IcebergConfig(
            warehouse="my_catalog",
            uri="https://glue.us-west-2.amazonaws.com/iceberg",
            account_id="123456789012",
            region="us-west-2",
        ),
    )
```

Switching to Databricks or Wherobots is just a different `spark_impl_name` plus
the corresponding `DatabricksConfig` / `WherobotsConfig` dataclass — the
surrounding DAG code does not change.

See [`examples/example_dag.py`](examples/example_dag.py) for a runnable DAG
that targets all three platforms.

See [`SPEC.md`](SPEC.md) for the full architecture.

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

## License

MIT — see [`LICENSE`](LICENSE).
