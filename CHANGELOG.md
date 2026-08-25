# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.10.1] - 2026-08-25

### Fixed

- **Databricks `cluster_log_conf` hardcoded a `dbfs` destination, rejected on
  AWS UC-first workspaces.** `setup_databricks_cluster` always wrote
  `{"dbfs": {"destination": ...}}` built from `DatabricksConfig.dbfs_root_template`,
  regardless of `DatabricksConfig.cloud`. Databricks' Clusters API only
  accepts `dbfs` or `s3` destinations, and UC-first AWS workspaces (one
  workspace per managed account, no legacy DBFS root) reject the `dbfs` one
  outright (`INVALID_PARAMETER_VALUE: Invalid cluster log storage info`).
  Confirmed live against `tf-data-platform`'s AWS Databricks smoke test
  (fixes #86). Cluster log delivery now branches on `cloud` the same way #79
  branched `*_attributes`: AWS gets an `s3` destination built from the same
  `s3_assets_bucket`/`s3_assets_root` the Glue/Wherobots builders already use,
  Azure/GCP keep the `dbfs` destination.

### Deprecated

- **`DatabricksConfig.dbfs_root_template`** (and the underlying DBFS cluster
  log path). Databricks is deprecating the legacy DBFS root in favor of Unity
  Catalog volumes/external locations; this field and its `dbfs` branch will be
  removed in a future major version (OvertureMaps/overture-airflow-provider#87).

## [0.10.0] - 2026-08-24

### Fixed

- **A zombie-killed Spark task's retry resubmitted the job while the original
  run kept writing to the same output, duplicating/corrupting it.**
  `SparkAgnosticExecuteOperator` never cancelled a still-running remote job
  before a retry submitted a new one, and a zombie kill bypasses `on_kill()`
  entirely (the scheduler fails the task instance externally without
  signaling the process), so there was no hook to catch it. Any caller with
  `retries >= 1` was exposed. Each platform handler (Glue, Databricks,
  Wherobots) now records the launched run id to the task instance's own
  XCom as soon as it's known, and the operator wraps `on_failure_callback`
  (chaining through any caller-supplied one) to read that recorded run and
  cancel it at failure-detection time, before a retry can resubmit.
  Fixes #83.

- **`setup`/`setup_cluster`'s Task Documentation rendered as a Markdown code
  block, with literal double-backtick markup instead of inline code.** `@task`
  auto-populates `doc_md` from the function's own docstring without
  dedenting it first (apache/airflow#66477 is still open), so a docstring's
  indented continuation lines land as an indented code block once rendered.
  Both tasks now pass `doc_md` explicitly as a flush-left string constant
  instead.

## [0.9.0] - 2026-08-24

### Fixed

- **Scala jobs failed on Wherobots with
  `NoClassDefFoundError: scala/Serializable`.** Every Wherobots submission
  hardcoded `version="preview"`, which resolves to the WherobotsDB 2.x stack
  (Spark 4 / Scala 2.13) — but `SparkImpl.WHEROBOTS_v1_5_0` declares
  Spark 3.5 / Scala 2.12, so any Scala 2.12 JAR died at driver class-load
  time (Python jobs happened to survive, masking the bug). Submissions now
  target the stable runtime by default via the new `WherobotsConfig.version`
  field, which defaults to `"latest"` (the run API's own default). Set it to
  `"preview"` to opt into the preview channel deliberately, or `None` to omit
  the field from the submission; the config field is the single source of
  truth for the runtime channel (the internal `version=` pass-through
  argument was removed). The `wherobots` extra now requires
  `airflow-providers-wherobots>=1.4.3`, the first release whose operator
  accepts the `version` kwarg (already an implicit requirement of the old
  hardcoded value). (#81)

## [0.8.0] - 2026-08-19

### Fixed

- **`DatabricksConfig` always submitted `azure_attributes`, rejecting clusters
  on AWS workspaces.** `setup_databricks_cluster` and
  `DatabricksClusterSize.as_json` hardcoded that key with no `aws_attributes`
  branch and no cloud detection, so the Databricks Clusters API rejected the
  spec outright against an AWS workspace
  (`airflow.exceptions.AirflowException: Spark job FAILED on DATABRICKS
  (submit/config failure, likely a provider or configuration fault)`,
  `databricks_base.py:615 ERROR - ... raised HTTPError`). Worker/driver
  instance-type discovery had the same gap: it only returned Azure VM SKUs
  (`Standard_E4a_v4`), so an AWS workspace got sized with SKUs it can't run.
  Caught live in OvertureMaps/tf-data-platform#4779's CI smoke test against a
  real AWS Databricks workspace (fixes #78).

### Added

- **`DatabricksConfig.cloud`**, so a caller running against an AWS or GCP
  workspace can pin the target cloud explicitly (`"aws"`, `"azure"`, or
  `"gcp"`). Defaults to `"azure"` (this provider's original, Azure-only
  behavior), so existing callers are unaffected and never trigger a workspace
  lookup. Set it to `""` to auto-detect instead, via the `databricks-sdk`'s
  own environment detection (derived from the connection host, no API round
  trip). Picks the cluster attributes key the Clusters API requires
  (`aws_attributes` / `azure_attributes` / `gcp_attributes`) and, unless
  `worker_instance_types`/`driver_node_type` are pinned, the default node-type
  catalog for that cloud. `DatabricksClusterSize` now carries an
  `aws_databricks_instance_types` catalog (`m5d.*` family) alongside the
  existing Azure one, and `from_desired_cores`/`from_cluster_size` both take a
  `cloud` kwarg.

## [0.7.3] - 2026-08-18

### Fixed

- **`bundle_inspector`'s SQL syntax highlighting silently stopped working under
  SRI-enforcing browsers.** The `index.html` template pinned an
  `integrity` hash against `prismjs@1.29.0/prism.min.js`, but that path
  isn't actually published by the `prismjs` npm package: the real tarball
  ships only unminified `prism.js` at its root. `cdn.jsdelivr.net` was
  synthesizing `prism.min.js` on the fly and caching the result, so its
  bytes (and hash) could drift whenever jsdelivr's own minifier changed,
  independent of the pinned package version. When that happened, the
  browser rejected the stale hash with `Failed to find a valid digest in
  the 'integrity' attribute`, and `prism-sql.min.js` then threw
  `Uncaught ReferenceError: Prism is not defined`. Every other CDN asset in
  that template (`maplibre-gl.js`, `themes/prism.min.css`,
  `components/prism-sql.min.js`, `marked.min.js`) is a real file in its
  package, so their hashes are safe to pin as-is; only the synthesized
  `prism.min.js` wasn't. Dropped `integrity`/`crossorigin` from that one tag
  instead of pinning against jsdelivr's derivative output: this is a
  read-only internal dashboard, not a spot that warrants chasing a moving
  hash for a minifier we don't control.

## [0.7.2] - 2026-08-17

### Added

- **`GlueConfig.output_log_group`, so a consumer whose jobs write continuous
  logging output to a non-default group can still get the CloudWatch
  fallback below.** The CloudWatch output log group backing that fallback
  was hardcoded to AWS's own `/aws-glue/jobs/output`. Defaults to that same
  value; override it via `GlueConfig` when your jobs are configured
  otherwise.

### Fixed

- **Deferred Glue job failures surfaced as a generic "trigger/polling failure"
  where the real error belonged.** The AWS Glue trigger raises on a terminal
  FAILED/STOPPED/TIMEOUT state, and a raising trigger reaches
  `resume_execution` as a `__fail__`, so a finished-but-failed run was
  classified as a Triggerer crash: the task log showed
  `Spark job FAILED on GLUE (trigger/polling failure ...) run: <unknown>`
  while the actual Glue `ErrorMessage` stayed only in CloudWatch.
  `resume_execution` now recovers the run id from the trigger error and
  resolves it through the same `complete_job` path `execute_complete` uses, so
  the task log names the real platform error with the run id. Genuine
  Triggerer crashes (no resolvable run) keep the trigger-failure
  classification.

- **Glue failure messages carried only a one-line `ErrorMessage`, even though
  the job's own diagnostics (e.g. a validation job's full multi-line error
  report) were sitting in CloudWatch the whole time.** Glue's `JobRun.LogTail`
  field, meant to carry a stderr tail, is empty for GlueVersion 5.0 Spark
  jobs, so `describe_failure`'s root-cause line was always blank for Glue.
  `complete_glue_job` now falls back to the run's own CloudWatch output log
  stream (deterministically named after the run id) when `LogTail` is empty,
  so the full report reaches the task log's `cause:` line. A fetch failure
  (missing stream, permissions, throttling) is swallowed, so the message
  just omits the cause line and failure reporting stays intact.

- **The CloudWatch fallback above could itself return a truncated tail that
  cuts off right where the job's real failure report begins.** `GetLogEvents`
  can hand back a stale, partial view of a stream for a while after a run
  finishes. `_fetch_glue_output_log_tail` now polls a few times, watching for
  the line Glue's driver prints right before exiting, as proof the stream has
  nothing left to deliver. A freshly-completed stream can also hand every
  event back on the first call out of timestamp order, so the fetch now
  sorts events before joining them.

- **Three Copilot review findings on the above.** A run id that lives only in
  `error` is now checked alongside `traceback`. A malformed CloudWatch event
  missing `timestamp`/`message` is now handled gracefully, keeping the
  "returns `None` on any failure" contract intact. The log-tail fetch's
  diagnostics now go through a module logger (`debug`/`warning`), filterable
  by log level.

- **A resolved terminal Glue failure re-raised as the retryable
  `AirflowException`, letting a caller-configured retry resubmit a job that
  already ran to a real failure.** `resume_execution`'s recovered-run-id path
  reaches `complete_job` only once a run id is confirmed, i.e. the job
  launched, but `complete_job` only ever raises plain `AirflowException`. That
  plain exception is now converted to the non-retryable `AirflowFailException`,
  matching the same launched-and-failed reasoning `execute()` already applies.

## [0.7.1] - 2026-08-10

### Fixed

- **`bundle_inspector`: Athena query polling hangs on error responses instead
  of surfacing them.** `fetchAthenaQuery`'s poll loop only exited on
  `status.state` of `SUCCEEDED`, `FAILED`, or `CANCELLED`. When
  `athena-status` returned a non-2xx error (e.g. `TABLE_NOT_FOUND` for a
  query against an unregistered table), the body has no `state`, so the loop
  polled every second for the full 5-minute timeout showing `undefined...`
  instead of the real error.
  ([#70](https://github.com/OvertureMaps/overture-airflow-provider/issues/70))

## [0.7.0] - 2026-08-06

### Added

- **`bundle_inspector` plugin, ported from `tf-data-platform`.** Adds a
  Browse -> Bundle Inspector UI for walking pipeline stages, themes, schema
  versions, and run IDs in S3, plus GeoParquet/PMTiles preview and ad hoc
  Athena queries against the same bundle. Loads on both Airflow 2 (Flask
  blueprints) and Airflow 3 (FastAPI apps), selected automatically at import
  time. Configure it via the `[bundle_inspector]` section in `airflow.cfg`
  (`s3_bucket`, `environment`, `athena_output_bucket`, `user`), each also
  settable through its `AIRFLOW__BUNDLE_INSPECTOR__<OPTION>` environment
  variable. GeoParquet preview needs the new `bundle-inspector` extra
  (`pyarrow`, `shapely`). The original's `/register-table` endpoint isn't
  ported: it depended on `tf-data-platform`-internal DAG naming with no
  equivalent here.
  ([#68](https://github.com/OvertureMaps/overture-airflow-provider/issues/68))

## [0.6.0] - 2026-08-05

### Changed

- **`execute_spark_job` failures now raise `AirflowFailException` when the job
  actually launched, instead of a plain `AirflowException` in every case.**
  `describe_failure` already classifies failures by whether the run reached
  the platform (`run_launched`); this now drives the exception type too:
  `submit/config` failures (never launched) stay retryable, while
  `downstream-job` and `trigger/polling` failures (launched, then failed, or a
  Triggerer crash after launch) raise the non-retryable
  `AirflowFailException`. Callers can set `retries=1` (or higher) on
  `execute_spark_job` and get automatic recovery from transient
  submission-time infra faults, without risking a silent retry of a job that
  actually ran and failed.
  ([#65](https://github.com/OvertureMaps/overture-airflow-provider/issues/65))

## [0.3.1] - 2026-06-12

### Fixed

- **Glue runner now forwards `extra_spark_conf` to `SparkSedonaJob`-style jobs.**
  Previously, jobs whose `run()` does not accept a `spark` argument (e.g.
  `SparkSedonaJob` subclasses that initialise Spark internally) silently
  discarded all DAG-level `extra_spark_conf` entries. The runner now detects
  `init_spark_for_platform` on the instance and calls it with the extra conf
  before `run()`, so builder-time settings (Iceberg catalog registrations, etc.)
  are applied correctly.
  ([#51](https://github.com/OvertureMaps/overture-airflow-provider/issues/51))

## [0.3.0] - 2026-06-10

### Changed

- **Spark job execution is now deferrable via a custom operator.** The
  `execute_spark_job` task is now a real `BaseOperator`
  (`SparkAgnosticExecuteOperator`) instead of a `@task`-decorated
  `PythonOperator`. It submits the Glue/Databricks job non-blocking and defers
  on the upstream provider's own trigger (`GlueJobCompleteTrigger`,
  `DatabricksExecutionTrigger`), resuming via its own `execute_complete` when the
  Triggerer reports completion. Instead of blocking a Celery worker for the full
  job duration (up to 8 hours), the worker slot is released within seconds of
  submission; the Triggerer polls asynchronously at negligible memory cost (~MB
  for hundreds of tasks vs. ~200–500 MB per blocked worker). This eliminates the
  OOM SIGKILL pressure on MWAA worker fleets running concurrent long-running
  Spark jobs.

  The earlier `deferrable=True` flag on the inner operators did **not** work:
  because the provider called `operator.execute()` inside a `PythonOperator`, the
  resulting `TaskDeferred` deferred the `PythonOperator`, whose missing
  `execute_complete` crashed on resume. The custom operator owns
  `execute_complete`, so Airflow resumes it correctly.

  No DAG changes required — deferral is a platform-internal concern and is not
  exposed as a parameter on `spark_agnostic_task_group`. Wherobots has no
  upstream trigger and continues to run synchronously. Requires an Airflow
  Triggerer (standard in MWAA 2.4+).
  ([#45](https://github.com/OvertureMaps/overture-airflow-provider/pull/45),
  fixes [#46](https://github.com/OvertureMaps/overture-airflow-provider/issues/46))

### Fixed

- **Glue deferral resume crashed with `KeyError: 'run_id'`.** On resume,
  `GluePlatformHandler.complete_job` read the run id from the trigger event under
  `run_id`, but the upstream `GlueJobCompleteTrigger` follows the
  `AwsBaseWaiterTrigger` contract and emits it under `value`. The handler now
  reads `value` (with a `run_id` fallback), so a successful Glue job resumes and
  finalizes correctly. Caught by a live smoke test.
  ([#45](https://github.com/OvertureMaps/overture-airflow-provider/pull/45))
- **Databricks workspace paths now use the bare workspace path, not the
  `/Workspace` FUSE prefix.** `DatabricksConfig.workspace_scripts_path_template`
  defaulted to `/Workspace/Shared/{s3_assets_root}`, but the Workspace REST
  (`2.0/workspace/get-status`) and Jobs (`notebook_path`, `init_scripts`) APIs
  address objects by bare path (`/Shared/...`). The `/Workspace` prefix caused a
  mismatch even when the assets were deployed at `/Shared/...`. The default is
  now `/Shared/{s3_assets_root}`, and a leading `/Workspace` is stripped from the
  resolved path so the notebook task and the init-script reference stay
  consistent and API-addressable. Caught by a live smoke test.
  ([#45](https://github.com/OvertureMaps/overture-airflow-provider/pull/45))

### Known issues

- **Upstream `aiohttp` log noise during Databricks deferral.** While a
  Databricks job is deferred, the Triggerer may log `aiohttp` "Unclosed client
  session / connector" *ERROR* lines. These originate in the upstream
  `apache-airflow-providers-databricks` `DatabricksExecutionTrigger` (its async
  client is not closed on the event loop), not in this provider. The task
  defers, polls, and resumes correctly regardless. This provider deliberately
  reuses the installed provider's trigger (to always match the installed
  version), so it does not fork the trigger to silence the message.
## [0.2.0] - 2026-06-10

### Changed

- **Renamed the published PyPI distribution from `overture-airflow-provider` to
  `airflow-provider-overture`** to match the common Airflow third-party provider
  naming convention. The import module (`overture_airflow_provider`) and the
  GitHub repository are unchanged; only `pip install` and PyPI metadata differ.
  ([#26](https://github.com/OvertureMaps/overture-airflow-provider/issues/26))
- Bumped the package development status classifier from Alpha to Beta.
  ([#28](https://github.com/OvertureMaps/overture-airflow-provider/issues/28))

### Added

- **Documented the one-time Databricks runner deploy step in the README.** The
  runner notebook and cluster init script must be staged to the workspace
  out-of-band (CI/CD or `upload_databricks_runner_to_workspace`) before the
  first run; a missing asset surfaces as Databricks' own authoritative
  cluster-launch / run error.
  ([#13](https://github.com/OvertureMaps/overture-airflow-provider/issues/13))

## [0.1.5] - 2026-06-03

### Fixed

- **IcebergConfig under native rendering (regression from
  [#27](https://github.com/OvertureMaps/overture-airflow-provider/pull/27)):**
  The four `IcebergConfig` JSON fields are forwarded as `setup_cluster_task`
  `op_kwargs` so Airflow renders their Jinja at execution time. On DAGs with
  `render_template_as_native_obj=True`, Airflow's native renderer `literal_eval`s
  a rendered JSON-object string back into a `dict` before the task runs, so the
  config arrived already parsed and the str-only parser raised
  `TypeError: the JSON object must be str, bytes or bytearray, not dict`. The
  config parsing now accepts an already-parsed `dict`.
  ([#30](https://github.com/OvertureMaps/overture-airflow-provider/pull/30),
  fixes [#29](https://github.com/OvertureMaps/overture-airflow-provider/issues/29))

### Changed

- Consolidated the three duplicate JSON-config parsers (`_parse_json_or_dict`,
  and two copies of `_load_json_config` in `spark_agnostic_taskgroup` and
  `render`) into a single `config.coerce_config_dict`. It is both validating
  (field-named errors, must-be-a-JSON-object check) and tolerant of an
  already-parsed `dict`. All four `IcebergConfig` variants (task group and
  `render`) and `extra_spark_conf` now route through it; `extra_spark_conf`
  gains the same object validation. A non-object payload — including a
  native-rendered empty list — now raises a field-named error instead of being
  silently dropped.

## [0.1.1] - 2026-06-02

### Fixed

- **Glue Scala jobs (regression):** The `scriptLocation` stub uploaded to S3 for
  Scala jobs was a real Scala program (`object JobRunnerGlue`, importing
  `com.amazonaws.services.glue.*` and `scala.jdk.CollectionConverters._`). AWS
  Glue compiles that file before every run — even when the real entry point is
  a precompiled JAR selected via `--class` / `--extra-jars` — and the file
  failed to compile on Glue 5.0 (Scala 2.12.18), killing jobs ~20 s after
  submission. Replaced with a comment-only no-op stub (zero compile surface),
  restoring the proven pre-migration behaviour. The reflective
  `run(spark, params)` dispatch was dead code in all current code paths and has
  been removed. ([#18](https://github.com/OvertureMaps/overture-airflow-provider/pull/18))

### Removed

- `DATABRICKS_v13` (`SparkImpl`): Databricks Runtime 13.3 LTS (Spark 3.4.1 / Python 3.10.6) dropped.

## [0.1.0] - Initial release

- `spark_agnostic_task_group` and `spark_agnostic_mapped_task_group` public
  entry points.
- Support for AWS Glue v4 / v5, Databricks 14.3 / 15.4 LTS, and Wherobots
  Cloud 1.5.0.
- Typed config dataclasses: `PackageRegistryConfig`, `ArtifactStoreConfig`,
  `IcebergConfig`, `GlueConfig`, `DatabricksConfig`, `WherobotsConfig`.
- Iceberg catalog wiring (REST/SigV4 for Glue+Databricks, GlueCatalog
  cross-account for Wherobots).
- S3-cached wheel and JAR distribution; native-deps detection.
- Unit and mocked-SDK test coverage.
- **Airflow-free render mode** (`overture_airflow_provider.render`): build
  platform submit payloads (Glue `create-job` / `start-job-run`, Databricks
  `jobs submit --json`, Wherobots REST body) and shell commands without
  importing or executing any Airflow operator. CLI:
  `python -m overture_airflow_provider.render --spark-impl ... --out ./out/`.
