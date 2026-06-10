"""Spark platform handlers.

One handler class per Spark platform, all conforming to ``SparkPlatformHandler``.
Each handler knows how to:

- download Python packages and JARs to S3 (Glue, Wherobots) or no-op (Databricks)
- compute the merged Spark config for the platform
- submit and wait for a Spark job

The task group calls handlers polymorphically via ``get_platform_handler``; no
platform-specific branching lives in the orchestration layer.
"""

from abc import ABC, abstractmethod

from overture_airflow_provider.spark import SparkFamily

# Spark settings shared by Glue and Databricks. Wherobots strips most of these
# at the platform level, so it carries its own minimal defaults.
_GLUE_DATABRICKS_DEFAULTS = {
    "spark.driver.extraJavaOptions": "-Djts.overlay=ng",
    "spark.executor.extraJavaOptions": "-Djts.overlay=ng",
    "sedona.join.numpartition": 4000,
    "spark.kryoserializer.buffer": "128m",
    "spark.driver.maxResultSize": "10g",
    "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
    "spark.sql.sources.commitProtocolClass": (
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    ),
    "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
    "fs.s3a.directory.marker.retention": "delete",
}

_WHEROBOTS_DEFAULTS = {
    "mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
    "spark.sql.sources.commitProtocolClass": (
        "org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol"
    ),
    "spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs": "false",
    "fs.s3a.directory.marker.retention": "delete",
}


def _merge_spark_conf(
    platform_defaults: dict,
    iceberg_spark_config: dict | None,
    extra_spark_conf: dict | None,
) -> dict:
    """Merge Spark configs in precedence order: platform defaults, Iceberg, user.

    Later entries override earlier ones. Returns a new dict; inputs are not mutated.
    """
    merged = dict(platform_defaults)
    if iceberg_spark_config:
        merged.update(iceberg_spark_config)
    if extra_spark_conf:
        merged.update(extra_spark_conf)
    return merged


class SparkPlatformHandler(ABC):
    """Abstract base class for Spark platform handlers."""

    def __init__(self, setup_info: dict):
        self.setup_info = setup_info
        self.spark_family = setup_info["spark_family"]

    @abstractmethod
    def download_python_packages(self, python_packages: str) -> dict | None:
        """Download and cache Python packages, or return None if not applicable."""

    @abstractmethod
    def download_jars(self) -> dict | None:
        """Download and cache JAR files, or return None if not applicable."""

    @abstractmethod
    def setup_cluster(
        self,
        python_packages: str,
        spark_jar_paths: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_desired_worker_cores: str,
        spark_cluster_desired_workers: str,
        iceberg_spark_config: dict | None = None,
    ) -> dict | None:
        """Compute merged Spark config (and, for Databricks, the cluster spec).

        Returns a dict that always contains ``"merged_spark_conf"``; Databricks
        additionally returns the ``new_cluster``/``libraries``/``databricks_conf``
        payload consumed by ``execute_job``.
        """

    @abstractmethod
    def submit_job(
        self,
        package_info: dict | None,
        jar_info: dict | None,
        cluster_info: dict | None,
        module_name: str,
        class_name: str,
        parameters: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_size_name: str,
        spark_cluster_desired_worker_cores: int | None,
        spark_cluster_desired_workers: int | None,
        iam_role_name: str,
        wherobots_role_arn: str,
        task_id: str,
        context: dict,
    ) -> dict:
        """Submit the Spark job without blocking on completion.

        Returns a dict with:

        - ``"trigger"``: a provider trigger to defer on, or ``None`` for
          platforms that run synchronously (Wherobots).
        - ``"run_id"``: the platform run identifier (when deferrable).
        - ``"result"``: the final result dict, only for synchronous platforms
          where there is nothing to defer on.

        Deferrable platforms (Glue, Databricks) push the early ``spark_agnostic``
        XCom here so ``SparkJobLink`` works while the task is deferred.
        """

    @abstractmethod
    def complete_job(
        self,
        event: dict | None,
        context: dict,
        cluster_info: dict | None = None,
    ) -> dict:
        """Resolve a deferred run into the final result dict.

        Called from the operator's ``execute_complete`` after the trigger
        reports a terminal state. Raises on job failure.
        """


class GluePlatformHandler(SparkPlatformHandler):
    """Handler for AWS Glue."""

    def download_python_packages(self, python_packages: str) -> dict:
        from overture_airflow_provider._glue import download_python_packages_glue

        return download_python_packages_glue(self.setup_info, python_packages)

    def download_jars(self) -> dict:
        from overture_airflow_provider._glue import download_jars_glue

        return download_jars_glue(self.setup_info, self.setup_info["spark_jar_paths"])

    def setup_cluster(
        self,
        python_packages: str,
        spark_jar_paths: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_desired_worker_cores: str,
        spark_cluster_desired_workers: str,
        iceberg_spark_config: dict | None = None,
    ) -> dict:
        merged = _merge_spark_conf(
            _GLUE_DATABRICKS_DEFAULTS, iceberg_spark_config, extra_spark_conf
        )
        return {"merged_spark_conf": merged}

    def submit_job(
        self,
        package_info: dict | None,
        jar_info: dict | None,
        cluster_info: dict | None,
        module_name: str,
        class_name: str,
        parameters: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_size_name: str,
        spark_cluster_desired_worker_cores: int | None,
        spark_cluster_desired_workers: int | None,
        iam_role_name: str,
        wherobots_role_arn: str,
        task_id: str,
        context: dict,
    ) -> dict:
        from overture_airflow_provider._glue import submit_glue_job

        submitted = submit_glue_job(
            setup_info=self.setup_info,
            package_info=package_info,
            jar_info=jar_info,
            module_name=module_name,
            class_name=class_name,
            extra_spark_conf=extra_spark_conf,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            iam_role_name=iam_role_name,
            task_id=task_id,
            context=context,
            execution_class=self.setup_info.get("glue_execution_class", "STANDARD"),
        )
        return {"trigger": submitted["trigger"], "run_id": submitted["run_id"]}

    def complete_job(
        self,
        event: dict | None,
        context: dict,
        cluster_info: dict | None = None,
    ) -> dict:
        from overture_airflow_provider._glue import complete_glue_job

        run_id = event["run_id"] if event else None
        return complete_glue_job(self.setup_info, run_id, context)


class DatabricksPlatformHandler(SparkPlatformHandler):
    """Handler for Databricks."""

    def download_python_packages(self, python_packages: str) -> None:
        # Databricks installs packages on the cluster via the libraries spec
        # produced in setup_cluster, so there's nothing to do here.
        return None

    def download_jars(self) -> None:
        return None

    def setup_cluster(
        self,
        python_packages: str,
        spark_jar_paths: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_desired_worker_cores: str,
        spark_cluster_desired_workers: str,
        iceberg_spark_config: dict | None = None,
    ) -> dict:
        from overture_airflow_provider._databricks import setup_databricks_cluster

        merged = _merge_spark_conf(
            _GLUE_DATABRICKS_DEFAULTS, iceberg_spark_config, extra_spark_conf
        )
        cluster_config = setup_databricks_cluster(
            setup_info=self.setup_info,
            python_packages=python_packages,
            spark_jar_paths=spark_jar_paths,
            extra_spark_conf=merged,
            extra_spark_env_vars=extra_spark_env_vars,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
        )
        cluster_config["merged_spark_conf"] = merged
        return cluster_config

    def submit_job(
        self,
        package_info: dict | None,
        jar_info: dict | None,
        cluster_info: dict | None,
        module_name: str,
        class_name: str,
        parameters: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_size_name: str,
        spark_cluster_desired_worker_cores: int | None,
        spark_cluster_desired_workers: int | None,
        iam_role_name: str,
        wherobots_role_arn: str,
        task_id: str,
        context: dict,
    ) -> dict:
        from overture_airflow_provider._databricks import submit_databricks_job

        submitted = submit_databricks_job(
            setup_info=self.setup_info,
            cluster_info=cluster_info,
            module_name=module_name,
            class_name=class_name,
            parameters=parameters,
            task_id=task_id,
            context=context,
        )
        return {"trigger": submitted["trigger"], "run_id": submitted["run_id"]}

    def complete_job(
        self,
        event: dict | None,
        context: dict,
        cluster_info: dict | None = None,
    ) -> dict:
        from overture_airflow_provider._databricks import complete_databricks_job

        return complete_databricks_job(self.setup_info, cluster_info, event, context)


class WherobotsPlatformHandler(SparkPlatformHandler):
    """Handler for Wherobots."""

    def download_python_packages(self, python_packages: str) -> dict:
        from overture_airflow_provider._wherobots import (
            download_python_packages_wherobots,
        )

        return download_python_packages_wherobots(self.setup_info, python_packages)

    def download_jars(self) -> dict:
        from overture_airflow_provider._wherobots import download_jars_wherobots

        return download_jars_wherobots(self.setup_info, self.setup_info["spark_jar_paths"])

    def setup_cluster(
        self,
        python_packages: str,
        spark_jar_paths: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_desired_worker_cores: str,
        spark_cluster_desired_workers: str,
        iceberg_spark_config: dict | None = None,
    ) -> dict:
        merged = _merge_spark_conf(_WHEROBOTS_DEFAULTS, iceberg_spark_config, extra_spark_conf)
        return {"merged_spark_conf": merged}

    def submit_job(
        self,
        package_info: dict | None,
        jar_info: dict | None,
        cluster_info: dict | None,
        module_name: str,
        class_name: str,
        parameters: str,
        extra_spark_conf: dict,
        extra_spark_env_vars: str,
        spark_cluster_size_name: str,
        spark_cluster_desired_worker_cores: int | None,
        spark_cluster_desired_workers: int | None,
        iam_role_name: str,
        wherobots_role_arn: str,
        task_id: str,
        context: dict,
    ) -> dict:
        from overture_airflow_provider._wherobots import execute_wherobots_job

        # Wherobots has no upstream Airflow trigger, so it runs synchronously and
        # returns the final result with no deferral.
        result = execute_wherobots_job(
            setup_info=self.setup_info,
            package_info=package_info,
            jar_info=jar_info,
            module_name=module_name,
            class_name=class_name,
            extra_spark_conf=extra_spark_conf,
            spark_cluster_size=spark_cluster_size_name,
            spark_cluster_desired_worker_cores=spark_cluster_desired_worker_cores,
            spark_cluster_desired_workers=spark_cluster_desired_workers,
            wherobots_role_arn=wherobots_role_arn,
            task_id=task_id,
            context=context,
            version="preview",
        )
        return {"trigger": None, "result": result}

    def complete_job(
        self,
        event: dict | None,
        context: dict,
        cluster_info: dict | None = None,
    ) -> dict:
        raise RuntimeError("Wherobots jobs run synchronously and never defer.")


_HANDLERS = {
    SparkFamily.GLUE: GluePlatformHandler,
    SparkFamily.DATABRICKS: DatabricksPlatformHandler,
    SparkFamily.WHEROBOTS: WherobotsPlatformHandler,
}


def get_platform_handler(spark_family: SparkFamily, setup_info: dict) -> SparkPlatformHandler:
    """Return the handler instance for ``spark_family``.

    Raises ``ValueError`` for any family not in ``_HANDLERS`` (e.g. ``SYNAPSE``).
    """
    handler_class = _HANDLERS.get(spark_family)
    if handler_class is None:
        raise ValueError(f"Unsupported Spark platform: {spark_family}")
    return handler_class(setup_info)
