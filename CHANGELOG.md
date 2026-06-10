# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
- **Databricks preflight now also verifies the cluster init script.** The
  preflight previously only checked the runner notebook, so a missing init
  script (named by `DatabricksConfig.cluster_init_script_name`, wired into the
  cluster's `init_scripts`) sailed past it and surfaced later as an opaque
  cluster-launch failure. Both required workspace assets are now checked up
  front and fail fast with one actionable error.
  ([#45](https://github.com/OvertureMaps/overture-airflow-provider/pull/45))
- **Databricks workspace paths now use the bare workspace path, not the
  `/Workspace` FUSE prefix.** `DatabricksConfig.workspace_scripts_path_template`
  defaulted to `/Workspace/Shared/{s3_assets_root}`, but the Workspace REST
  (`2.0/workspace/get-status`) and Jobs (`notebook_path`, `init_scripts`) APIs
  address objects by bare path (`/Shared/...`). The `/Workspace` prefix caused a
  false-negative preflight (`RESOURCE_DOES_NOT_EXIST`) even when the assets were
  deployed at `/Shared/...`. The default is now `/Shared/{s3_assets_root}`, and
  a leading `/Workspace` is stripped from the resolved path so preflight, the
  notebook task and the init-script reference stay consistent and
  API-addressable. Caught by a live smoke test.
  ([#45](https://github.com/OvertureMaps/overture-airflow-provider/pull/45))

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

- **Fail-fast preflight for the Databricks runner notebook.** Notebook jobs now
  verify the bundled runner is deployed to the workspace before submitting and
  raise an actionable error (pointing at
  `upload_databricks_runner_to_workspace`) instead of failing opaquely mid-run.
  Documented the one-time Databricks runner deploy step in the README.
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
