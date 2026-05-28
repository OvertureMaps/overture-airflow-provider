# `overture-airflow-provider` — Architecture Spec

This document describes the internal architecture of the provider.

## Goal

Expose a **single Airflow task group** that runs a Spark job (PySpark or Scala)
on one of:

- AWS Glue (v4 or v5)
- Databricks (13.3 / 14.3 / 15.4 LTS)
- Wherobots Cloud (1.5.0)

…with the same caller-facing API. The DAG code does not change when the engine
changes; only `spark_impl_name` and the platform-specific config dataclass do.

## Non-goals

- A general Spark abstraction. We only abstract the **submission** of a job, not
  Spark itself.
- Opinionated defaults for any specific organization. Every bucket, role,
  catalog, package registry, and pool name is provided by the caller.

## Public surface

Two entry points, both Airflow `@task_group`-decorated factories:

- `spark_agnostic_task_group(...)` — a single concrete task group invocation.
- `spark_agnostic_mapped_task_group(...)` — dynamic task mapping over a list
  of per-invocation configs.

Six typed config dataclasses:

| Dataclass | Required for |
|---|---|
| `ArtifactStoreConfig` | All platforms (S3 bucket for wheel/JAR cache) |
| `PackageRegistryConfig` | All platforms (CodeArtifact pip + maven) |
| `IcebergConfig` | Optional; Iceberg catalog wiring |
| `GlueConfig` | Glue jobs (IAM role) |
| `DatabricksConfig` | Databricks jobs (conn id, DBFS layout) |
| `WherobotsConfig` | Wherobots jobs (role ARN, external id) |

And three enums:

- `SparkImpl` — concrete engine version (e.g. `GLUE_v5`, `DATABRICKS_v15`).
- `SparkFamily` — `GLUE` / `DATABRICKS` / `WHEROBOTS` / `SYNAPSE` (reserved).
- `AwsGlueClusterSize` / `DatabricksClusterSize` / `WherobotsClusterSize` —
  named cluster sizes per platform.

## Internal layout

```
spark_agnostic_taskgroup     ← public @task_group
        │
        ▼
SparkPlatformHandler ABC     ← dispatch (factory: get_platform_handler)
        │
   ┌────┼────┐
   ▼    ▼    ▼
 Glue  DBX  Wherobots        ← per-platform handlers
   │    │    │
   ▼    ▼    ▼
 _glue _databricks _wherobots  ← per-platform submit logic (operator boundary)
        │
        ▼
SparkAgnosticHelper          ← shared S3 wheel/JAR cache
        │
        ▼
python_package_utils         ← CodeArtifact pip/maven HTTP clients
```

## Task graph

The task group has five tasks per invocation:

```
setup_spark_job ──┬─► download_python_packages ─┐
                  ├─► download_jars            ─┼─► execute_spark_job
                  └─► setup_cluster            ─┘
```

- `setup_spark_job` resolves `spark_impl_name` to versions, parses parameters,
  constructs the CodeArtifact client, and produces the `setup_info` dict
  written to XCom.
- `download_python_packages` / `download_jars` cache wheels and JARs in S3
  (cache key includes wheel filename → version pinning). Native wheels and
  non-CodeArtifact JARs are uploaded to a job-specific prefix; pure-Python
  wheels and CodeArtifact JARs share a content-addressed cache.
- `setup_cluster` builds the platform-specific cluster spec and merges
  spark-conf in the order **platform defaults < iceberg config < extra**.
- `execute_spark_job` instantiates the platform operator
  (`GlueJobOperator`, `DatabricksSubmitRunOperator`,
  `WherobotsRunOperator`) and runs it.

## XCom contract

`setup_info.SERIALIZABLE_KEYS` is the explicit allowlist of keys that get
written to XCom. Adding a new `setup_spark_job` field requires updating both
`_setup.py` (build) and `setup_info.py` (allowlist).

Non-serializable values (the `py_pi_client` HTTP session, enum objects) are
passed in-process only and rehydrated via `setup_info.rehydrate` on the other
side.

## Iceberg

`IcebergConfig` exposes two structural variants:

- `spark_config` — REST/SigV4 keys for Glue and Databricks
  (`spark.sql.catalog.iceberg_catalog.catalog-impl = RESTCatalog`).
- `wherobots_spark_config` — GlueCatalog cross-account keys for Wherobots
  (`...catalog-impl = GlueCatalog`, plus account-id), since the Wherobots
  runtime does not support the REST catalog client.

The task group picks the variant at runtime based on `spark_family_name`.

## Wherobots specifics

- Wherobots strips a fixed list of unsupported spark-conf keys at execute
  time (`extraJavaOptions`, `sedona.join.numpartition`, `kryoserializer.buffer`,
  `maxResultSize`, `partitionOverwriteMode`).
- When Iceberg is enabled (i.e. `spark.sql.defaultCatalog` is in the merged
  conf), the Wherobots handler also injects the Wherobots credential factory
  config so the Wherobots-managed pod can assume the customer's Glue role.

## Versioning

Semantic versioning. Breaking changes to the public surface (config
dataclasses, `spark_agnostic_task_group` keyword arguments, XCom contract)
bump the major version.

## Out of scope

- EMR / Synapse / standalone Spark support.
- A non-Airflow runtime.
- Type-checking gates beyond Ruff.
- Publishing to PyPI (tracked separately).
