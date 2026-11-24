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

It also exposes `matching_task_group`, which wires a matching workflow out of
caller-supplied task factories so several isolated instances can run in one
DAG, and `child_dag_run_task_group`, which triggers child DAG runs with stable
run IDs. Their usage docs live in [`docs/matching.md`](docs/matching.md) and
[`docs/child-dag-runs.md`](docs/child-dag-runs.md); the README only summarises
them.

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
`maven_repository`, `maven_repository_path`, `maven_domain`,
`maven_domain_owner`. **Not** prefixed with `codeartifact_`. The un-scoped
`domain`/`domain_owner`/`repository` always describe the **PyPI** repo; use the
`maven_*` fields for Maven. The two `maven_domain*` fields default to their pip
counterparts, set them only when the Maven repository lives in a different
domain or account.

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

`stage_workspace_assets=True` (default) makes `setup_cluster` upload the
bundled runner notebook and init script to
`{scripts_path}/runners/{sha12}-{name}` via `DatabricksSdkHook` on the same
`databricks_conn_id`, then reference those paths. Path resolution
(`_resolve_databricks_workspace_assets`) is pure so `render` shows the same
paths without uploading; only `DatabricksPlatformHandler.setup_cluster` calls
`stage_databricks_workspace_assets`. `False` restores the fixed pre-deployed
paths. A non-default `cluster_init_script_name` is never staged.

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

`version` (default `"latest"`) is the Wherobots run-API runtime channel:
`"latest"` targets the stable runtime (the API's own default). Only set
`"preview"` deliberately — the preview stack runs Spark 4 / Scala 2.13 and
cannot load Scala 2.12 JARs. `None` omits the field from submissions.

### Cluster sizing classes

`AwsGlueClusterSize` / `DatabricksClusterSize` / `WherobotsClusterSize` are
plain classes, **not enums** — do not call `.XS.name` on them. Pass a
`ClusterSize` name string (`"XS"`, `"S"`, `"M"`, …) to `from_cluster_size`.
`spark_cluster_size_name` also accepts any native `wherobots.db.Runtime` value,
e.g. `"large-himem"`, resolved by `WherobotsClusterSize.from_runtime_name`
(`from_cluster_size` stays `XS`..`XXL` only). Unknown names raise `ValueError`;
Glue/Databricks ignore that kwarg.
Prefer `spark_cluster_desired_worker_cores` (integer string) in DAG code; it
is more portable across platforms.

### matching_task_group and child_dag_run_task_group

`matching_task_group` is wiring only. Matcher arguments, data sources, sinks,
path layouts and domain vocabulary belong in the caller's factories, never in
`matching_taskgroup.py`. It returns `MatchingTasks(group, result_paths)`, and
callers write `upstream >> result.group >> finalize`. `child_dag_run_task_group`
returns `ChildDagRuns(group, run_ids)`. The design is in
[`SPEC.md`](SPEC.md#matching-workflow); these are the invariants to keep:

- **One root, one leaf.** `start` is the matching group's only root and
  `complete` its only leaf (teardowns aside); `requests` is the child-run
  group's only root. That is what makes a gate on the group reach every task
  and lets `group >> finalize` wait for completion. Do not add a second root,
  and do not hand out `start` or `complete` as handles.
- **Explicit `all_success`.** Keep `all_success` in the matching group's
  `default_args` and explicitly on the provider's own tasks. It is Airflow's
  default, but only an explicit value stops a `trigger_rule` in a DAG's
  `default_args` from reaching them. The joins `run_ids` and `work_succeeded`
  use `NONE_FAILED_MIN_ONE_SUCCESS` on purpose, to allow an empty request list
  and branches that were not taken.
- **`_require_gated`.** It rejects, at DAG construction, a factory task that a
  skipped gate would not skip. Do not loosen it to accept other trigger rules;
  cleanup belongs in a teardown.
- **Explicit `>>` between steps.** A factory may ignore the outputs it is given
  or leave its own tasks unchained, and the next step must still wait for all
  of them. `_require_built` rejects a factory task built outside its call for
  the same reason.
- **The failure barrier.** `complete` waits for the leaves of every step and
  for `work_succeeded`, which follows every non-teardown task. Do not replace
  the barrier with dependencies on leaves alone: `_failure_barrier` explains how
  a task can fail without any leaf failing, and
  `test_a_failed_task_blocks_completion_even_when_the_last_tasks_succeed` shows
  it. A setup task is followed through an `all_success` relay because Airflow
  allows nothing else after a setup.
- **The staged `requests` task.** Map the trigger over it, not over the
  caller's requests, and with `.expand(conf=...)`, not `expand_kwargs`. Airflow
  does not render values resolved from a task output under `.expand`, so
  `"123"` stays a string under native rendering. A mapped task's output reaches
  `_requests` as a lazily read sequence, which it turns into a list; do not
  test for `list`.
- **Stable child run IDs.** The run ID is set in `ChildDagRunOperator.execute`
  and paired with `reset_dag_run=True`; keep both, or clearing a trigger fails
  on the existing run ID. Keep `conf` in the digest: a reset run keeps its old
  `conf`, so a changed request needs a new ID.
- **Permanent contract errors.** Wrap in-task contract checks with `_permanent`
  so a violation raises `AirflowFailException` and is not retried.

### Airflow compat shim

Always import `DAG`, `task`, `task_group`, `BaseHook`, `BaseOperatorLink`,
`EmptyOperator`, `TriggerDagRunOperator` from
`overture_airflow_provider._airflow_compat`, never directly from `airflow.*`.
The two operators load on first access because importing them pulls in
`airflow.models`.

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
docker compose run --rm --build e2e                 # default: Airflow 3.3.1
AIRFLOW_VERSION=3.3.1 docker compose run --rm --build e2e
./run.sh all   # bonus: run the whole tests/ suite under real Airflow
```

`tests/e2e` is excluded from the default `pytest` run unless `RUN_E2E=1` (set by
the container). CI: `.github/workflows/test.yml` (matrix: Airflow 3.3).
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

Title format: Conventional Commits, `type: short description` (see `CONTRIBUTING.md`).

Include the `Co-authored-by: Copilot` trailer in AI-generated commits:

```
Co-authored-by: Copilot <223556219+Copilot@users.noreply.github.com>
```

Releases are automated (python-semantic-release, see `CONTRIBUTING.md`): do not bump
`version` in `pyproject.toml` (a placeholder; the release stamps it from the tag) or edit
`CHANGELOG.md` (frozen at 0.17.1) in PRs. The merge
commit title must follow Conventional Commits (`feat:`, `fix:`, `feat!:`) to produce a release.
