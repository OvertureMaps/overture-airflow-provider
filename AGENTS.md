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
    report_issue_config=ReportIssueConfig(...),
)
```

### PackageRegistryConfig field names

Fields are `domain`, `domain_owner`, `repository`, `region`,
`maven_repository`, `maven_repository_path`. **Not** prefixed with
`codeartifact_`.

### IcebergConfig

Holds four JSON-string fields:

- `spark_config` (Glue/Databricks primary catalog, typically REST)
- `wherobots_spark_config` (Wherobots primary catalog, typically GlueCatalog)
- `s3tables_spark_config` (Glue/Databricks S3 Tables catalog — coexists with primary)
- `wherobots_s3tables_spark_config` (Wherobots S3 Tables catalog variant)

S3 Tables keys must be namespaced under a distinct catalog alias (e.g.
`spark.sql.catalog.s3tables_catalog.*`) to avoid conflicts with the primary
catalog. Both primary and S3 Tables configs are merged together at runtime.

### ReportIssueConfig

Opt-in "Report Issue" operator extra-link (off by default; provider assumes
nothing about issue trackers). `enabled=True` requires a non-empty `target`.
Tracker is pluggable via `_report_issue.IssueTracker` + `register_tracker`;
`provider="github"` ships built in (`target` is an `"owner/repo"` slug). Add a
new backend (e.g. Jira) by subclassing `IssueTracker` and registering it — no
changes to the link, operator, or config wiring. `extra` carries
provider-specific knobs.

### DatabricksConfig

`cluster_conf` is a raw dict; put `databricks_conn_id` inside it.
`dbfs_root_template` / `workspace_scripts_path_template` accept
`{s3_assets_root}` substitution.

GPU (or any custom node type) is generic, not a hardcoded SKU. Two paths:

- **Auto-discovery (preferred):** set `gpu=True`. The provider queries the
  connected workspace via the `databricks-sdk` (`[databricks]` extra), picks
  GPU node types + a GPU ML runtime for that workspace's cloud, and sizes from
  them. The driver defaults to the cheapest discovered CPU node (the driver
  doesn't need a GPU; compute runs on the workers). Discovery only fills gaps —
  explicit overrides below win per-field, and setting all three skips the API
  call.
- **Explicit override:** set `worker_instance_types` (a `{node_type_id: cores}`
  catalog), `driver_node_type`, and/or `spark_version` (pin a GPU runtime like
  `"15.4.x-gpu-ml-scala2.12"`).

Worker count derives from desired cores; pin `spark_cluster_desired_workers`
for GPU-count-sensitive jobs.

Workspace discovery (`gpu=True`) authenticates via `DatabricksSdkHook`
(`hooks.py`), which maps the Airflow connection onto the databricks-sdk's
*unified auth*: PAT (`password`), OAuth M2M service principal
(`extra.service_principal_oauth` + `login`/`password`), Azure service principal
(`extra.azure_tenant_id` + `login`/`password`), and in-cluster federated OIDC
(`login="federated_k8s"` or `extra.federated_k8s`). Auth is injected via masked
env vars inside a short-lived context, so it is not PAT-only. Pinning all three
override fields avoids the workspace call (and its auth) entirely.

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

### Real-Airflow e2e (Docker)

The unit suite mocks Airflow/SDKs; `tests/e2e/` proves the provider installs into
a **real** Airflow and its example DAGs parse cleanly (credential-free).

```bash
cd tests/e2e
docker compose run --rm --build e2e                 # default: Airflow 2.11
AIRFLOW_VERSION=3.0.3 docker compose run --rm --build e2e
./run.sh all   # bonus: run the whole tests/ suite under real Airflow
```

`tests/e2e` is excluded from the default `pytest` run unless `RUN_E2E=1` (set by
the container). CI: `.github/workflows/e2e.yml` (matrix: Airflow 2.11 + 3.0).
Details in `tests/e2e/README.md`.

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
