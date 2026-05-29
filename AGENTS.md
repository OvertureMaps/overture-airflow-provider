# AGENTS.md — Grounding context for AI coding agents

This file is the canonical reference for AI agents (Copilot, Codex, etc.)
working in this repository.

---

## What this repo is

`overture-airflow-provider` is an Apache Airflow provider that exposes a
**Spark-agnostic TaskGroup** (`spark_agnostic_task_group`) for running PySpark
or Scala/Spark jobs on **AWS Glue**, **Databricks**, or **Wherobots Cloud**
from a single, unified DAG entry point.

The caller switches engines by changing one argument (`spark_impl_name`) and
swapping a platform config dataclass. The DAG body does not change.

Architecture details live in [`SPEC.md`](SPEC.md).

See [`SPEC.md`](SPEC.md) for architecture diagrams, task graph, XCom contract
details, Iceberg variants, Wherobots specifics, versioning policy, and
out-of-scope items.

---

## Quick-reference: API correctness

These are the spots where agents (and humans) most often get things wrong.

### `spark_agnostic_task_group` config kwargs

Use `*_config=` suffixed names — not short aliases:

```python
spark_agnostic_task_group(
    ...,
    iceberg_config=IcebergConfig(...),
    package_registry=PackageRegistryConfig(...),
    artifact_store=ArtifactStoreConfig(...),
    glue_config=GlueConfig(...),
    databricks_config=DatabricksConfig(...),
    wherobots_config=WherobotsConfig(...),
)
```

### PackageRegistryConfig field names

Fields are `domain`, `domain_owner`, `repository`, `region`,
`maven_repository`, `maven_repository_path`. **Not** prefixed with
`codeartifact_`.

### IcebergConfig

Holds two JSON-string fields: `spark_config` (Glue/Databricks) and
`wherobots_spark_config` (Wherobots GlueCatalog). Not a warehouse/URI struct.

### DatabricksConfig

`cluster_conf` is a raw dict; put `databricks_conn_id` inside it.
`dbfs_root_template` / `workspace_scripts_path_template` accept
`{s3_assets_root}` substitution.

GPU (or any custom node type) is generic, not a hardcoded SKU. Two paths:

- **Auto-discovery (preferred):** set `gpu=True`. The provider queries the
  connected workspace via the `databricks-sdk` (`[databricks]` extra), picks
  GPU node types + a GPU ML runtime for that workspace's cloud, and sizes from
  them. Discovery only fills gaps — explicit overrides below win per-field, and
  setting all three skips the API call.
- **Explicit override:** set `worker_instance_types` (a `{node_type_id: cores}`
  catalog), `driver_node_type`, and/or `spark_version` (pin a GPU runtime like
  `"15.4.x-gpu-ml-scala2.12"`).

Worker count derives from desired cores; pin `spark_cluster_desired_workers`
for GPU-count-sensitive jobs.

### WherobotsConfig

AWS region field is `aws_region` (not `region`).

### Cluster sizing classes

`AwsGlueClusterSize` / `DatabricksClusterSize` / `WherobotsClusterSize` are
plain classes, **not enums** — do not call `.XS.name` on them. Pass a
`ClusterSize` name string (`"XS"`, `"S"`, `"M"`, …) to `from_cluster_size`.
Prefer `spark_cluster_desired_worker_cores` (integer string) in DAG code; it
is more portable across platforms.

### Airflow compat shim

Always import `DAG`, `task`, `task_group`, `BaseHook`, `BaseOperatorLink` from
`overture_airflow_provider._airflow_compat`, never directly from `airflow.*`.

### XCom: adding a new setup field

Update **both** `_setup.py` (produce the field) **and**
`setup_info.SERIALIZABLE_KEYS` (allowlist it). Forgetting the second step
silently drops the field on XCom round-trips.

---

## Development commands

```bash
uv sync --all-extras --group dev   # install all deps
uv run pytest -v                   # run tests
uv run ruff check .                # lint (includes AIR* rules)
uv run ruff format .               # format
```

Tests use MagicMock stubs for optional platform SDKs (`databricks`, `sh`,
`wherobots`) so the suite runs without them installed. See `tests/conftest.py`.

---

## Decisions & constraints

- **Unopinionated**: no baked-in S3 buckets, role names, catalog names, pool
  names, or other org-specific defaults. All config comes from the caller.
- **No platform-specific branching in the orchestration layer**: all
  platform differences live in `SparkPlatformHandler` subclasses.
- **Lazy imports throughout**: `import overture_airflow_provider` does not pull
  in Airflow internals or platform SDKs unless the caller uses them.
- **Runners are Overture-free**: the bundled runner scripts
  (`runners/job_runner_*.py`) depend only on the Spark runtime and the Python
  stdlib; they do not import any Overture package.
- **Windows**: Airflow does not officially support Windows (a warning is
  emitted). Tests, lint, and the render module work fine on Windows; production
  runs on Linux/macOS.

---

## PR / commit conventions

Title format: `[TYPE] Short description` (see `CONTRIBUTING.md`).

Include the `Co-authored-by: Copilot` trailer in AI-generated commits:

```
Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
```
