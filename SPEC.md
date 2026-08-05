# `overture-airflow-provider` — Architecture Spec

This document describes the internal architecture of the provider.

## Goal

Expose a **single Airflow task group** that runs a Spark job (PySpark or Scala)
on one of:

- AWS Glue (v4 or v5)
- Databricks (14.3 / 15.4 LTS)
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

Seven typed config dataclasses:

| Dataclass | Required for |
|---|---|
| `ArtifactStoreConfig` | All platforms (S3 bucket for wheel/JAR cache) |
| `PackageRegistryConfig` | All platforms (CodeArtifact pip + maven) |
| `IcebergConfig` | Optional; Iceberg catalog wiring |
| `GlueConfig` | Glue jobs (IAM role) |
| `DatabricksConfig` | Databricks jobs (conn id, DBFS layout) |
| `WherobotsConfig` | Wherobots jobs (role ARN, external id) |
| `ReportIssueConfig` | Optional; wires a "Report Issue" extra-link onto `execute_spark_job` |

And three enumerations (implemented as plain classes with class-level constants, not Python `enum.Enum`; do not call `.name` on instances):

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
        ├──► _failures          ← classify + format job failures (stdlib only)
        │
        ▼
SparkAgnosticHelper          ← shared S3 wheel/JAR cache
        │
        ▼
python_package_utils         ← CodeArtifact pip/maven HTTP clients

links.py                     ← SparkJobLink + ReportIssueLink (extra-links)
_report_issue.py             ← pluggable issue tracker (IssueTracker ABC, GitHub built in)
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
- `execute_spark_job` is a deferrable `SparkAgnosticExecuteOperator`. It
  instantiates the platform operator (`GlueJobOperator`,
  `DatabricksSubmitRunOperator`, `WherobotsRunOperator`), submits the job
  non-blocking, then defers on the upstream provider's trigger
  (`GlueJobCompleteTrigger`, `DatabricksExecutionTrigger`) so the Triggerer —
  not a Celery worker — polls until completion. It resumes in
  `execute_complete`. Wherobots has no upstream trigger and runs synchronously.

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

Each variant also has an S3 Tables counterpart (`s3tables_spark_config`,
`wherobots_s3tables_spark_config`) for an optional, coexisting S3 Tables
catalog under a distinct alias.

`iceberg.resolve_iceberg_spark_config(iceberg_config, spark_family)` is the
single place that picks the right variant pair for the resolved platform
family and merges primary + S3 Tables into one dict (S3 Tables wins on key
collision). Both the live DAG path (`spark_agnostic_taskgroup.py`) and the
Airflow-free render path (`render.py`) call this one function — it used to be
duplicated between them, which is how the Wherobots S3 Tables credential bug
below went unnoticed for a release.

## Spark config merge layers

Three platforms, three merge points, one shared primitive
(`spark_platform_handlers._merge_spark_conf(defaults, middle, extra)` — later
args win):

- **Glue**: `_GLUE_DATABRICKS_DEFAULTS` → Iceberg → `extra_spark_conf`, done
  once in `GluePlatformHandler.setup_cluster`.
- **Databricks**: two merges. `DatabricksPlatformHandler.setup_cluster` does
  `_GLUE_DATABRICKS_DEFAULTS` → Iceberg → `extra_spark_conf` (same as Glue),
  then `setup_databricks_cluster` (`_databricks.py`) does a second merge —
  Databricks-specific Spark defaults (Sedona serde, Iceberg extensions,
  commit protocol) → `DatabricksConfig.cluster_conf`'s `databricks_spark_conf`
  → the already-merged conf from the first pass — via the same
  `_merge_spark_conf` helper, so both Databricks layers and the Glue/Wherobots
  layer share one precedence rule instead of a bespoke dict splat.
- **Wherobots**: `_WHEROBOTS_DEFAULTS` → Iceberg → `extra_spark_conf` in
  `WherobotsPlatformHandler.setup_cluster`, followed by a Wherobots-only
  post-merge mutation pass in `_wherobots.build_wherobots_operator_kwargs`
  (see below) that runs *after* the shared merge and is invisible from the
  taskgroup/render call sites.

## Wherobots specifics

- `_wherobots._strip_unsupported_wherobots_keys` drops a fixed list of
  unsupported spark-conf keys at execute time (`extraJavaOptions`,
  `sedona.join.numpartition`, `kryoserializer.buffer`, `maxResultSize`,
  `partitionOverwriteMode`).
- `_wherobots._inject_wherobots_catalog_credentials` — when Iceberg is
  enabled, injects the Wherobots credential factory config for **every**
  registered catalog (via `spark_platform_handlers.registered_catalog_names`,
  which scans `spark.sql.catalog.<name>` keys plus `spark.sql.defaultCatalog`)
  so the Wherobots-managed pod can assume the customer's role for each one —
  not just the catalog named by `spark.sql.defaultCatalog`. This is what lets
  the S3 Tables catalog (`wherobots_s3tables_spark_config`) authenticate
  correctly whether it coexists with a primary catalog or is the only catalog
  configured.

## Failure enrichment

When `execute_spark_job` raises, the handler's `describe_failure()` method
produces a `FailureInfo` dataclass (platform, run id, state, reason, root-cause
tail, console URL). `classify_failure()` assigns one of four categories based
on signals the orchestration layer already holds:

| Classification | When |
|---|---|
| `downstream-job` | run launched (run-id XCom present); failure is in the job |
| `submit/config` | run never launched; likely a provider or config fault |
| `trigger/polling` | deferral machinery failed; see Triggerer logs |
| `platform/infra` | platform reported an internal error (e.g. Databricks `INTERNAL_ERROR`) |

`apply_heuristics()` scans the combined reason + root-cause text for known
patterns (IAM denials, auth errors, OOM, throttling, missing resources) and
appends an actionable hint. `format_failure()` renders all fields into a
uniform multi-line message.

`_operator.py` picks the raised exception type from the same `run_launched`
signal that drives classification above: `downstream-job` and `trigger/polling`
(the run reached the platform, or a Triggerer crash happened mid-poll after
launch) raise `AirflowFailException`, which Airflow never retries regardless of
the task's `retries=`. `submit/config` (the run never reached the platform)
raises the retryable `AirflowException`. This lets a caller set
`retries=1` on `execute_spark_job` to recover from transient submission-time
infra faults (e.g. a worker recycling mid-submit) without risking a silent
retry of a job that actually ran and failed.

`_failures.py` has no Airflow or platform SDK imports; it works purely from
stdlib so it is testable and importable without any runtime dependencies.

## Report Issue link

`ReportIssueConfig(enabled=True, target="owner/repo")` wires a "Report Issue"
button onto `execute_spark_job`. The config is written to XCom at task start
(`_push_report_issue_config`) so the link renders even when the task later
fails. The tracker is pluggable: subclass `IssueTracker`, implement
`build_url()`, and call `register_tracker(provider_name, cls)`. GitHub ships
built in. No changes to the link, operator, or config wiring are needed to add
a new backend.

## Versioning

Semantic versioning. Breaking changes to the public surface (config
dataclasses, `spark_agnostic_task_group` keyword arguments, XCom contract)
bump the major version.

## Out of scope

- EMR / Synapse / standalone Spark support.
- A non-Airflow runtime.
- Type-checking gates beyond Ruff.
- Publishing to PyPI (tracked separately).
