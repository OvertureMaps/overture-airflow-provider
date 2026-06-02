# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
- **Glue Iceberg catalog not registered:** Iceberg catalog Spark conf (e.g.
  `spark.sql.catalog.iceberg_catalog.*`, `spark.sql.extensions`) was not applied
  at SparkSession-creation time for Glue jobs, so multi-part names like
  `iceberg_catalog.my_table` routed to the session catalog and failed with
  `REQUIRES_SINGLE_PART_NAMESPACE` (later `_LEGACY_ERROR_TEMP_1055`). The merged
  Spark conf is now injected into the job's `--conf` `DefaultArguments` for both
  Scala **and** PySpark jobs, so Glue registers the catalog before any user code
  runs. The PySpark runner's runtime `spark.conf.set()` is now best-effort
  (static configs that cannot change on a live session are skipped instead of
  failing the job). ([#18](https://github.com/OvertureMaps/overture-airflow-provider/pull/18))

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
