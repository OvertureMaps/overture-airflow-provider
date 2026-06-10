# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
