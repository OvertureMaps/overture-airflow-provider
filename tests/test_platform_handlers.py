"""Tests for SparkPlatformHandler subclasses."""

import json
from collections.abc import Mapping
from unittest.mock import MagicMock, patch

import pytest

from overture_airflow_provider._airflow_compat import AirflowException
from overture_airflow_provider.spark import SparkFamily, SparkImpl
from overture_airflow_provider.spark_platform_handlers import (
    DatabricksPlatformHandler,
    GluePlatformHandler,
    WherobotsPlatformHandler,
    get_platform_handler,
)

_ICEBERG_WHEROBOTS_KEY = "spark.sql.catalog.iceberg_catalog.catalog-impl"
_ICEBERG_WHEROBOTS_IMPL = "org.apache.iceberg.aws.glue.GlueCatalog"


class _AirflowContext(Mapping):
    """A Mapping that is NOT a dict subclass.

    Simulates the Airflow 3.x task Context, which is a Mapping but fails
    ``isinstance(ctx, dict)`` (regression guard for issue #33).
    """

    def __init__(self, data):
        self._data = dict(data)

    def __getitem__(self, key):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


_DEFAULT_PROVIDER_KEYS = {
    "force_pip_packages": [],
    "databricks_extra_libraries": [],
    "databricks_dbfs_root_template": "dbfs:/FileStore/deploy/{s3_assets_root}",
    "databricks_workspace_scripts_path_template": "/Workspace/Shared/{s3_assets_root}",
    "databricks_cluster_init_script_name": "agnostic_operator_cluster_init_databricks.sh",
    "databricks_custom_tags": {},
    "databricks_spark_conf": {},
    "databricks_spark_env_vars": {},
    "codeartifact_domain_owner": "",
    "codeartifact_domain": "",
    "codeartifact_repository": "",
    "codeartifact_region": "us-east-1",
    "codeartifact_maven_repository": "",
    "codeartifact_maven_repository_path": "",
}


def _glue_setup_info(spark_impl_name="GLUE_v5", sedona_version="1.7.0"):
    impl = SparkImpl.from_str(spark_impl_name)
    return {
        **_DEFAULT_PROVIDER_KEYS,
        "job_name": "test.Job",
        "spark_impl": impl,
        "spark_impl_name": spark_impl_name,
        "spark_family": SparkFamily.GLUE,
        "spark_version": impl.get_spark_version(),
        "scala_version": impl.get_scala_version(),
        "python_version": impl.get_python_version(),
        "sedona_version": sedona_version,
        "spark_version_for_sedona": "3.5",
        "geotools_wrapper_version": "28.5",
        "run_identifier": "test.Job/20240101",
        "py_pi_client": MagicMock(),
        "parameters": '{"s3_input": "s3://bucket/in", "s3_output": "s3://bucket/out"}',
        "spark_jar_paths": [],
        "s3_assets_bucket": "test-bucket",
        "s3_assets_root": "spark-agnostic-operator",
        "job_runner_wheel_prefix": None,
        "wherobots_external_id": "",
        "wherobots_role_arn": "",
        "aws_region": "us-east-1",
        "databricks_conf": None,
        "glue_execution_class": "STANDARD",
        "iam_role_name": "AWSGlueServiceRole",
    }


def _databricks_setup_info():
    impl = SparkImpl.DATABRICKS_v15
    return {
        **_DEFAULT_PROVIDER_KEYS,
        "job_name": "test.Job",
        "spark_impl": impl,
        "spark_impl_name": "DATABRICKS_v15",
        "spark_family": SparkFamily.DATABRICKS,
        "spark_version": impl.get_spark_version(),
        "scala_version": impl.get_scala_version(),
        "python_version": impl.get_python_version(),
        "sedona_version": "1.7.0",
        "spark_version_for_sedona": "3.5",
        "geotools_wrapper_version": "28.5",
        "run_identifier": "test.Job/20240101",
        "py_pi_client": MagicMock(),
        "parameters": '{"key": "value"}',
        "spark_jar_paths": [],
        "s3_assets_bucket": "test-bucket",
        "s3_assets_root": "spark-agnostic-operator",
        "job_runner_wheel_prefix": None,
        "wherobots_external_id": "",
        "aws_region": "us-east-1",
        "databricks_conf": {"databricks_conn_id": "databricks_default"},
        "glue_execution_class": "STANDARD",
        "iam_role_name": "AWSGlueServiceRole",
    }


def _wherobots_setup_info():
    impl = SparkImpl.WHEROBOTS_v1_5_0
    return {
        **_DEFAULT_PROVIDER_KEYS,
        "job_name": "test.Job",
        "spark_impl": impl,
        "spark_impl_name": "WHEROBOTS_v1_5_0",
        "spark_family": SparkFamily.WHEROBOTS,
        "spark_version": impl.get_spark_version(),
        "scala_version": impl.get_scala_version(),
        "python_version": impl.get_python_version(),
        "sedona_version": "1.7.0",
        "spark_version_for_sedona": "3.5",
        "geotools_wrapper_version": "28.5",
        "run_identifier": "test.Job/20240101",
        "py_pi_client": MagicMock(),
        "parameters": '{"key": "value"}',
        "spark_jar_paths": [],
        "s3_assets_bucket": "test-bucket",
        "s3_assets_root": "spark-agnostic-operator",
        "job_runner_wheel_prefix": None,
        "wherobots_external_id": "test-external-id",
        "wherobots_role_arn": "arn:aws:iam::123456789012:role/test-role",
        "aws_region": "us-east-1",
        "databricks_conf": None,
        "glue_execution_class": "STANDARD",
        "iam_role_name": "AWSGlueServiceRole",
    }


def _mock_iceberg_rest():
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "iceberg_catalog",
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
        "spark.sql.catalog.iceberg_catalog.uri": "https://glue.us-west-2.amazonaws.com/iceberg",
        "spark.sql.catalog.iceberg_catalog.warehouse": "123456789012",
        "spark.sql.catalog.iceberg_catalog.rest.sigv4-enabled": "true",
        "spark.sql.catalog.iceberg_catalog.rest.signing-name": "glue",
        "spark.sql.catalog.iceberg_catalog.http-client.apache.max-connections": 3000,
    }


def _mock_iceberg_wherobots(warehouse_path):
    return {
        "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        "spark.sql.defaultCatalog": "iceberg_catalog",
        "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
        "spark.sql.catalog.iceberg_catalog.warehouse": warehouse_path,
        "spark.sql.catalog.iceberg_catalog.glue.account-id": "123456789012",
        "spark.sql.catalog.iceberg_catalog.http-client.apache.max-connections": 3000,
    }


class TestGetPlatformHandler:
    def test_glue_returns_glue_handler(self):
        handler = get_platform_handler(SparkFamily.GLUE, _glue_setup_info())
        assert isinstance(handler, GluePlatformHandler)

    def test_databricks_returns_databricks_handler(self):
        handler = get_platform_handler(SparkFamily.DATABRICKS, _databricks_setup_info())
        assert isinstance(handler, DatabricksPlatformHandler)

    def test_wherobots_returns_wherobots_handler(self):
        handler = get_platform_handler(SparkFamily.WHEROBOTS, _wherobots_setup_info())
        assert isinstance(handler, WherobotsPlatformHandler)

    def test_unknown_family_raises(self):
        with pytest.raises(ValueError, match="Unsupported"):
            get_platform_handler(SparkFamily.SYNAPSE, _glue_setup_info())


class TestGlueSetupCluster:
    def _run(self, extra_spark_conf=None, iceberg_spark_config=None):
        handler = GluePlatformHandler(_glue_setup_info())
        return handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf=extra_spark_conf or {},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=iceberg_spark_config or _mock_iceberg_rest(),
        )

    def test_returns_merged_spark_conf(self):
        result = self._run()
        assert "merged_spark_conf" in result

    def test_default_config_keys_present(self):
        result = self._run()
        conf = result["merged_spark_conf"]
        assert "spark.driver.extraJavaOptions" in conf
        assert "spark.executor.extraJavaOptions" in conf
        assert "spark.driver.maxResultSize" in conf

    def test_iceberg_keys_merged(self):
        result = self._run()
        conf = result["merged_spark_conf"]
        assert "spark.sql.extensions" in conf
        assert "spark.sql.defaultCatalog" in conf

    def test_user_override_takes_precedence(self):
        result = self._run(extra_spark_conf={"spark.driver.maxResultSize": "20g"})
        assert result["merged_spark_conf"]["spark.driver.maxResultSize"] == "20g"

    def test_user_can_add_new_key(self):
        result = self._run(extra_spark_conf={"spark.custom.setting": "myval"})
        assert result["merged_spark_conf"]["spark.custom.setting"] == "myval"

    def test_empty_user_conf_does_not_remove_defaults(self):
        result = self._run(extra_spark_conf={})
        assert "spark.driver.maxResultSize" in result["merged_spark_conf"]


class TestGlueExecuteJob:
    def _make_context(self, mapping=False):
        data = {
            "ti": MagicMock(),
            "dag": MagicMock(dag_id="test_dag"),
        }
        data["ti"].task_id = "execute_spark_job"
        return _AirflowContext(data) if mapping else data

    def _run_glue(
        self,
        module_name="my_module",
        class_name="MyClass",
        extra_spark_conf=None,
        desired_worker_cores="40",
        desired_workers="",
        iam_role_name="AWSGlueServiceRole",
        simulate_submit=False,
        mapping_context=False,
    ):
        from overture_airflow_provider._glue import submit_glue_job

        setup_info = _glue_setup_info()
        package_info = {
            "py_files": "s3://bucket/pkg.whl",
            "script_location": "s3://bucket/job_runner_glue.py",
            "scala_script_location": "s3://bucket/job_runner_glue.scala",
            "s3_bucket": "test-bucket",
            "s3_prefix": "overture-airflow-operator/test.Job/20240101",
            "native_packages": [],
        }
        jar_info = {
            "jars_s3": "s3://bucket/sedona.jar,s3://bucket/geotools.jar",
            "sedona_packages": "org.apache.sedona:sedona-spark-shaded-3.5_2.12:1.7.0",
            "sedona_module": "apache-sedona==1.7.0",
        }

        captured = {}

        from overture_airflow_provider._airflow_compat import TaskDeferred

        # deferrable=True -> GlueJobOperator.execute() submits then raises
        # TaskDeferred carrying its own trigger.
        mock_trigger = MagicMock(name="GlueJobCompleteTrigger", run_id="jr_early123")

        mock_operator = MagicMock()
        mock_operator._job_run_id = "jr_early123"
        mock_operator.aws_conn_id = "aws_default"
        mock_operator.execute.side_effect = TaskDeferred(
            trigger=mock_trigger, method_name="execute_complete"
        )

        mock_glue_client = MagicMock()

        with (
            patch(
                "overture_airflow_provider._glue.GlueJobOperator",
                return_value=mock_operator,
            ) as MockOperator,
            patch(
                "overture_airflow_provider._glue.boto3.client",
                return_value=mock_glue_client,
            ),
        ):
            context = self._make_context(mapping=mapping_context)
            result = submit_glue_job(
                setup_info=setup_info,
                package_info=package_info,
                jar_info=jar_info,
                module_name=module_name,
                class_name=class_name,
                extra_spark_conf=extra_spark_conf or {},
                spark_cluster_desired_worker_cores=desired_worker_cores,
                spark_cluster_desired_workers=desired_workers,
                iam_role_name=iam_role_name,
                task_id="execute_spark_job",
                context=context,
            )
            captured["call_kwargs"] = MockOperator.call_args[1]
            captured["context"] = context

        return result, captured

    def test_pyspark_job_uses_py_script_location(self):
        _, captured = self._run_glue(module_name="my_module", class_name="MyClass")
        assert captured["call_kwargs"]["script_location"] == "s3://bucket/job_runner_glue.py"

    def test_scala_job_uses_scala_script_location(self):
        _, captured = self._run_glue(module_name="", class_name="com.example.Main")
        assert captured["call_kwargs"]["script_location"] == "s3://bucket/job_runner_glue.scala"

    def test_pyspark_script_args_contain_module_name(self):
        _, captured = self._run_glue(module_name="my_module", class_name="MyClass")
        operator_kwargs = captured["call_kwargs"]
        script_args = operator_kwargs["script_args"]
        assert "--module_name" in script_args
        assert script_args["--module_name"] == "my_module"

    def test_scala_job_args_contain_class(self):
        _, captured = self._run_glue(module_name="", class_name="com.example.Main")
        create_kwargs = captured["call_kwargs"]["create_job_kwargs"]
        assert create_kwargs["DefaultArguments"]["--class"] == "com.example.Main"

    def test_scala_job_language_set(self):
        _, captured = self._run_glue(module_name="", class_name="com.example.Main")
        create_kwargs = captured["call_kwargs"]["create_job_kwargs"]
        assert create_kwargs["DefaultArguments"]["--job-language"] == "scala"

    def test_sedona_module_in_additional_python_modules(self):
        _, captured = self._run_glue()
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        assert "--additional-python-modules" in default_args
        assert "apache-sedona==1.7.0" in default_args["--additional-python-modules"]

    def test_extra_jars_in_default_args(self):
        _, captured = self._run_glue()
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        assert "--extra-jars" in default_args
        assert "sedona.jar" in default_args["--extra-jars"]

    def test_glue_version_set_from_impl(self):
        _, captured = self._run_glue()
        create_kwargs = captured["call_kwargs"]["create_job_kwargs"]
        assert create_kwargs["GlueVersion"] == "5.0"

    def test_operator_kwargs_include_deferrable_true(self):
        _, captured = self._run_glue()
        assert captured["call_kwargs"]["deferrable"] is True

    def test_result_contains_trigger_and_run_id(self):
        result, _ = self._run_glue()
        assert result["run_id"] == "jr_early123"
        assert result["trigger"] is not None

    def test_pushes_spark_agnostic_xcom_after_submission(self):
        _, captured = self._run_glue()
        calls = captured["context"]["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls
        payload = json.loads(spark_agnostic_calls[0].kwargs["value"])
        assert payload["job_url"].endswith("/run/jr_early123")
        assert payload["status"] == "RUNNING"

    def test_early_xcom_push_fires_with_non_dict_context(self):
        # Airflow 3.x passes a Mapping Context (not a dict). Regression for #33.
        _, captured = self._run_glue(mapping_context=True)
        assert not isinstance(captured["context"], dict)
        calls = captured["context"]["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls

    # ------------------------------------------------------------------
    # Scala --conf injection (Iceberg catalog registration)
    # ------------------------------------------------------------------

    def test_scala_iceberg_conf_injected_into_default_args(self):
        # Iceberg catalog conf must land in DefaultArguments["--conf"] so Glue
        # applies it at session-creation time before any user code runs.
        iceberg_conf = {
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg_catalog.catalog-impl": "org.apache.iceberg.rest.RESTCatalog",
            "spark.sql.catalog.iceberg_catalog.uri": "https://glue.us-west-2.amazonaws.com/iceberg",
        }
        _, captured = self._run_glue(
            module_name="", class_name="com.example.Main", extra_spark_conf=iceberg_conf
        )
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        assert "--conf" in default_args
        conf_str = default_args["--conf"]
        assert "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" in conf_str
        assert (
            "spark.sql.catalog.iceberg_catalog.catalog-impl=org.apache.iceberg.rest.RESTCatalog"
            in conf_str
        )
        assert (
            "spark.sql.catalog.iceberg_catalog.uri=https://glue.us-west-2.amazonaws.com/iceberg"
            in conf_str
        )

    def test_scala_conf_format_uses_glue_delimiter(self):
        # Multi-conf string must use " --conf " as delimiter so Glue parses it correctly.
        iceberg_conf = {
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.iceberg_catalog.type": "rest",
        }
        _, captured = self._run_glue(
            module_name="", class_name="com.example.Main", extra_spark_conf=iceberg_conf
        )
        conf_str = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]["--conf"]
        # Two entries → exactly one " --conf " delimiter between them.
        # The jar_info["sedona_packages"] key (spark.jars.packages) is excluded, so the
        # only entries are the two iceberg_conf keys + any non-excluded platform defaults.
        entries = conf_str.split(" --conf ")
        assert len(entries) >= 2, "Expected at least two --conf entries for a multi-key conf"
        for entry in entries:
            assert "=" in entry, f"conf entry missing '=': {entry!r}"

    def test_scala_conf_excludes_spark_jars_packages(self):
        # spark.jars.packages must not appear in --conf: Glue can't resolve Maven coords
        # at runtime; JARs are pre-staged via --extra-jars.
        _, captured = self._run_glue(module_name="", class_name="com.example.Main")
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        if "--conf" in default_args:
            assert "spark.jars.packages" not in default_args["--conf"]

    def test_scala_conf_excludes_java_options(self):
        # driver/executor extraJavaOptions are handled by --driver-java-options /
        # --executor-java-options; duplicating them in --conf would override those args
        # and lose the sedona charset setting.
        conf_with_java_opts = {
            "spark.driver.extraJavaOptions": "-Dfoo=bar",
            "spark.executor.extraJavaOptions": "-Dfoo=bar",
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
        }
        _, captured = self._run_glue(
            module_name="", class_name="com.example.Main", extra_spark_conf=conf_with_java_opts
        )
        conf_str = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]["--conf"]
        assert "spark.driver.extraJavaOptions" not in conf_str
        assert "spark.executor.extraJavaOptions" not in conf_str
        # The iceberg key must still be present.
        assert "spark.sql.catalog.iceberg_catalog" in conf_str

    def test_scala_conf_no_conf_key_when_only_excluded_keys(self):
        # When extra_spark_conf is empty (only spark.jars.packages from jar_info is in
        # spark_conf_dict), there is nothing to inject and --conf must not be added at all.
        _, captured = self._run_glue(
            module_name="", class_name="com.example.Main", extra_spark_conf={}
        )
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        # spark.jars.packages is excluded; if platform defaults also end up excluded,
        # --conf should be absent. Assert it's either absent OR, if platform defaults
        # contributed conf, they are present but jars.packages is not.
        if "--conf" in default_args:
            assert "spark.jars.packages" not in default_args["--conf"]

    def test_pyspark_iceberg_conf_injected_into_default_args(self):
        # PySpark jobs must also register catalogs at SparkSession bootstrap via Glue's
        # native --conf. Glue builds the session before user code runs, so legacy
        # SparkSedonaJob.run() implementations without a `spark` kwarg still get catalogs.
        iceberg_conf = {
            "spark.sql.catalog.iceberg_catalog": "org.apache.iceberg.spark.SparkCatalog",
            "spark.sql.catalog.s3tables_catalog": "org.apache.iceberg.spark.SparkCatalog",
        }
        _, captured = self._run_glue(
            module_name="my_module", class_name="MyClass", extra_spark_conf=iceberg_conf
        )
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        assert "--conf" in default_args
        conf_str = default_args["--conf"]
        assert "spark.sql.catalog.iceberg_catalog=org.apache.iceberg.spark.SparkCatalog" in conf_str
        assert (
            "spark.sql.catalog.s3tables_catalog=org.apache.iceberg.spark.SparkCatalog" in conf_str
        )
        # Maven coords can't be resolved at runtime; JARs are pre-staged via --extra-jars.
        assert "spark.jars.packages" not in conf_str

    def test_pyspark_no_conf_key_when_only_excluded_keys(self):
        # With no extra conf, only the excluded spark.jars.packages remains → no --conf.
        _, captured = self._run_glue(module_name="my_module", class_name="MyClass")
        default_args = captured["call_kwargs"]["create_job_kwargs"]["DefaultArguments"]
        if "--conf" in default_args:
            assert "spark.jars.packages" not in default_args["--conf"]


class TestCompleteGlueJob:
    def _run(self, job_state):
        from overture_airflow_provider._glue import complete_glue_job

        context = {"ti": MagicMock(task_id="execute_spark_job")}

        mock_glue = MagicMock()
        mock_glue.get_job_run.return_value = {"JobRun": {"JobRunState": job_state}}

        with patch(
            "overture_airflow_provider._glue.boto3.client",
            return_value=mock_glue,
        ):
            return complete_glue_job(_glue_setup_info(), "jr_abc123", context)

    def test_succeeded_returns_result(self):
        result = self._run("SUCCEEDED")
        assert "job_url" in result
        assert "status" in result

    def test_job_url_contains_run_id(self):
        result = self._run("SUCCEEDED")
        assert result["job_url"].endswith("/run/jr_abc123")

    def test_failed_raises(self):
        with pytest.raises(AirflowException, match="did not succeed"):
            self._run("FAILED")

    def test_stopped_raises(self):
        with pytest.raises(AirflowException, match="did not succeed"):
            self._run("STOPPED")

    def test_timeout_raises(self):
        with pytest.raises(AirflowException, match="did not succeed"):
            self._run("TIMEOUT")

    def test_error_raises(self):
        with pytest.raises(AirflowException, match="did not succeed"):
            self._run("ERROR")


class TestGlueHandlerCompleteJobEventContract:
    """GlueJobCompleteTrigger emits the run id under "value" (AwsBaseWaiterTrigger
    contract), not "run_id" like the Databricks trigger. Regression for the live
    KeyError: 'run_id' bug.
    """

    def _complete(self, event):
        handler = GluePlatformHandler(_glue_setup_info())
        captured = {}

        def _fake_complete(setup_info, run_id, context):
            captured["run_id"] = run_id
            return {"job_url": "https://example/run/x", "status": "SUCCEEDED"}

        with patch(
            "overture_airflow_provider._glue.complete_glue_job",
            side_effect=_fake_complete,
        ):
            handler.complete_job(event, {"ti": MagicMock()})
        return captured["run_id"]

    def test_reads_value_key_from_glue_trigger_event(self):
        # Shape emitted live by GlueJobCompleteTrigger.
        event = {"status": "success", "message": "Job done", "value": "jr_34fddb5c"}
        assert self._complete(event) == "jr_34fddb5c"

    def test_falls_back_to_run_id_key(self):
        assert self._complete({"run_id": "jr_legacy"}) == "jr_legacy"

    def test_none_event_yields_none_run_id(self):
        assert self._complete(None) is None


class TestDatabricksSetupCluster:
    def _run(
        self,
        extra_spark_conf=None,
        iceberg_spark_config=None,
        python_packages="overture-spark==1.0",
    ):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        return handler.setup_cluster(
            python_packages=python_packages,
            spark_jar_paths="",
            extra_spark_conf=extra_spark_conf or {},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=iceberg_spark_config or _mock_iceberg_rest(),
        )

    def test_returns_merged_spark_conf(self):
        result = self._run()
        assert "merged_spark_conf" in result

    def test_default_config_keys_present(self):
        conf = self._run()["merged_spark_conf"]
        assert "spark.driver.maxResultSize" in conf
        assert "spark.driver.extraJavaOptions" in conf

    def test_user_override_takes_precedence(self):
        conf = self._run(extra_spark_conf={"spark.driver.maxResultSize": "20g"})[
            "merged_spark_conf"
        ]
        assert conf["spark.driver.maxResultSize"] == "20g"

    def test_iceberg_keys_in_merged_conf(self):
        conf = self._run()["merged_spark_conf"]
        assert "spark.sql.extensions" in conf

    def test_libraries_contain_python_packages(self):
        result = self._run(python_packages="overture-spark==1.0 numba")
        libs = result["libraries"]
        pypi_pkgs = [lib["pypi"]["package"] for lib in libs if "pypi" in lib]
        assert "overture-spark==1.0" in pypi_pkgs
        assert "numba" in pypi_pkgs

    def test_libraries_always_contain_sedona(self):
        result = self._run()
        libs = result["libraries"]
        pypi_pkgs = [lib["pypi"]["package"] for lib in libs if "pypi" in lib]
        assert any("apache-sedona" in p for p in pypi_pkgs)

    def test_new_cluster_contains_spark_version(self):
        result = self._run()
        assert "new_cluster" in result
        assert (
            result["new_cluster"]["spark_version"] == SparkImpl.DATABRICKS_v15.get_native_version()
        )

    def test_databricks_spark_conf_merged_into_new_cluster(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        handler.setup_info["databricks_spark_conf"] = {"spark.custom.platform": "dbx-only"}
        result = handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf={},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=_mock_iceberg_rest(),
        )
        assert result["new_cluster"]["spark_conf"]["spark.custom.platform"] == "dbx-only"

    def test_extra_spark_conf_overrides_databricks_spark_conf(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        handler.setup_info["databricks_spark_conf"] = {"spark.custom.platform": "dbx-only"}
        result = handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf={"spark.custom.platform": "from-extra"},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=_mock_iceberg_rest(),
        )
        assert result["new_cluster"]["spark_conf"]["spark.custom.platform"] == "from-extra"

    def test_databricks_spark_env_vars_merged_into_new_cluster(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        handler.setup_info["databricks_spark_env_vars"] = {"PLATFORM_TOKEN": "dbx-only"}
        result = handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf={},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=_mock_iceberg_rest(),
        )
        assert result["new_cluster"]["spark_env_vars"]["PLATFORM_TOKEN"] == "dbx-only"

    def test_extra_spark_env_vars_override_databricks_spark_env_vars(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        handler.setup_info["databricks_spark_env_vars"] = {"PLATFORM_TOKEN": "dbx-only"}
        result = handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf={},
            extra_spark_env_vars='{"PLATFORM_TOKEN": "from-extra"}',
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=_mock_iceberg_rest(),
        )
        assert result["new_cluster"]["spark_env_vars"]["PLATFORM_TOKEN"] == "from-extra"

    def test_gpu_overrides_applied_to_new_cluster(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        handler.setup_info["databricks_worker_instance_types"] = {"Standard_NC8as_T4_v3": 8}
        handler.setup_info["databricks_driver_node_type"] = "Standard_NC8as_T4_v3"
        handler.setup_info["databricks_spark_version"] = "15.4.x-gpu-ml-scala2.12"
        result = handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf={},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="32",
            spark_cluster_desired_workers="",
            iceberg_spark_config=_mock_iceberg_rest(),
        )
        cluster = result["new_cluster"]
        assert cluster["node_type_id"] == "Standard_NC8as_T4_v3"
        assert cluster["driver_node_type_id"] == "Standard_NC8as_T4_v3"
        assert cluster["spark_version"] == "15.4.x-gpu-ml-scala2.12"

    def test_gpu_discovery_fills_new_cluster(self, monkeypatch):
        import overture_airflow_provider._databricks as dbx

        monkeypatch.setattr(
            dbx,
            "discover_gpu_cluster_options",
            lambda conn_id, *, need_nodes=True, need_runtime=True: {
                "worker_instance_types": {"Standard_NC8as_T4_v3": 8},
                "driver_node_type": "Standard_NC8as_T4_v3",
                "spark_version": "15.4.x-gpu-ml-scala2.12",
            },
        )
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        handler.setup_info["py_pi_client"].get_url.return_value = "https://fake-pypi/simple/"
        handler.setup_info["databricks_gpu"] = True
        result = handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf={},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="32",
            spark_cluster_desired_workers="",
            iceberg_spark_config=_mock_iceberg_rest(),
        )
        cluster = result["new_cluster"]
        assert cluster["node_type_id"] == "Standard_NC8as_T4_v3"
        assert cluster["driver_node_type_id"] == "Standard_NC8as_T4_v3"
        assert cluster["spark_version"] == "15.4.x-gpu-ml-scala2.12"

    def test_download_python_packages_returns_none(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        assert handler.download_python_packages("anything") is None

    def test_download_jars_returns_none(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        assert handler.download_jars() is None


class TestDatabricksSubmitJob:
    _CLUSTER_INFO = {
        "new_cluster": {"spark_version": "15.4.x-scala2.12"},
        "libraries": [],
        "databricks_conf": {"databricks_conn_id": "databricks_default"},
        "databricks_deployed_scripts_path": "/Workspace/Shared/spark-agnostic-operator",
    }

    def _run(self, context):
        from overture_airflow_provider._airflow_compat import TaskDeferred
        from overture_airflow_provider._databricks import submit_databricks_job

        mock_trigger = MagicMock(
            name="DatabricksExecutionTrigger",
            run_page_url="https://dbc.example/runs/12345",
        )

        mock_operator = MagicMock()
        mock_operator.run_id = "12345"
        mock_operator.execute.side_effect = TaskDeferred(
            trigger=mock_trigger, method_name="execute_complete"
        )

        mock_hook = MagicMock()
        mock_hook.get_run_page_url.return_value = "https://dbc.example/runs/12345"

        with (
            patch(
                "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
                return_value=mock_operator,
            ),
            patch(
                "airflow.providers.databricks.hooks.databricks.DatabricksHook",
                return_value=mock_hook,
            ),
            patch("overture_airflow_provider._databricks.preflight_databricks_runner"),
        ):
            result = submit_databricks_job(
                setup_info=_databricks_setup_info(),
                cluster_info=self._CLUSTER_INFO,
                module_name="my_module",
                class_name="MyClass",
                parameters='{"key":"value"}',
                task_id="execute_spark_job",
                context=context,
            )
        return result

    def test_returns_trigger_and_run_id(self):
        result = self._run({"ti": MagicMock()})
        assert result["run_id"] == "12345"
        assert result["trigger"] is not None
        assert result["run_page_url"] == "https://dbc.example/runs/12345"

    def test_pushes_spark_agnostic_xcom_after_submission(self):
        context = {"ti": MagicMock()}
        self._run(context)
        calls = context["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls
        payload = json.loads(spark_agnostic_calls[0].kwargs["value"])
        assert payload["job_url"] == "https://dbc.example/runs/12345"
        assert payload["status"] == "RUNNING"

    def test_early_xcom_push_fires_with_non_dict_context(self):
        # Airflow 3.x passes a Mapping Context (not a dict). Regression for #33.
        context = _AirflowContext({"ti": MagicMock()})
        self._run(context)
        assert not isinstance(context, dict)
        calls = context["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls

    def test_operator_kwargs_include_deferrable_true(self):
        from overture_airflow_provider._databricks import build_databricks_operator_kwargs

        result = build_databricks_operator_kwargs(
            setup_info=_databricks_setup_info(),
            cluster_info=self._CLUSTER_INFO,
            module_name="my_module",
            class_name="MyClass",
            task_id="execute_spark_job",
        )
        assert result["operator_kwargs"]["deferrable"] is True
        # Databricks only defers when wait_for_termination is True.
        assert result["operator_kwargs"]["wait_for_termination"] is True

    def test_synchronous_completion_returns_result_without_trigger(self):
        # If the run reaches a terminal (successful) state within the submit
        # window, DatabricksSubmitRunOperator.execute() returns normally instead
        # of raising TaskDeferred. We must finalize with a result, not crash.
        from overture_airflow_provider._databricks import submit_databricks_job

        mock_operator = MagicMock()
        mock_operator.run_id = "12345"
        mock_operator.execute.return_value = None  # no TaskDeferred -> synchronous

        mock_hook = MagicMock()
        mock_hook.get_run_page_url.return_value = "https://dbc.example/runs/12345"
        mock_hook.get_run.return_value = {"state": {"result_state": "SUCCESS"}}

        with (
            patch(
                "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
                return_value=mock_operator,
            ),
            patch(
                "airflow.providers.databricks.hooks.databricks.DatabricksHook",
                return_value=mock_hook,
            ),
            patch("overture_airflow_provider._databricks.preflight_databricks_runner"),
        ):
            result = submit_databricks_job(
                setup_info=_databricks_setup_info(),
                cluster_info=self._CLUSTER_INFO,
                module_name="my_module",
                class_name="MyClass",
                parameters='{"key":"value"}',
                task_id="execute_spark_job",
                context={"ti": MagicMock()},
            )

        assert result["trigger"] is None
        assert result["result"]["job_url"] == "https://dbc.example/runs/12345"
        assert "status" in result["result"]


class TestCompleteDatabricksJob:
    _CLUSTER_INFO = {
        "databricks_conf": {"databricks_conn_id": "databricks_default"},
    }

    def _run(self, *, successful):
        from overture_airflow_provider._databricks import complete_databricks_job

        event = {
            "run_id": "12345",
            "run_page_url": "https://dbc.example/runs/12345",
            "run_state": '{"life_cycle_state": "TERMINATED"}',
            "errors": [],
        }

        mock_run_state = MagicMock()
        mock_run_state.is_successful = successful
        mock_run_state_cls = MagicMock()
        mock_run_state_cls.from_json.return_value = mock_run_state

        mock_hook = MagicMock()
        mock_hook.get_run.return_value = {"state": {"result_state": "SUCCESS"}}

        with (
            patch(
                "airflow.providers.databricks.hooks.databricks.DatabricksHook",
                return_value=mock_hook,
            ),
            patch(
                "airflow.providers.databricks.hooks.databricks.RunState",
                mock_run_state_cls,
            ),
        ):
            return complete_databricks_job(
                _databricks_setup_info(), self._CLUSTER_INFO, event, {"ti": MagicMock()}
            )

    def test_success_returns_result(self):
        result = self._run(successful=True)
        assert result["job_url"] == "https://dbc.example/runs/12345"
        assert "status" in result

    def test_failure_raises(self):
        with pytest.raises(AirflowException, match="failed"):
            self._run(successful=False)


class TestDatabricksRunnerPreflight:
    _CLUSTER_INFO = {
        "databricks_conf": {"databricks_conn_id": "databricks_default"},
        "databricks_deployed_scripts_path": "/Workspace/Shared/spark-agnostic-operator",
    }
    _NOTEBOOK_PATH = "/Workspace/Shared/spark-agnostic-operator/job_runner_databricks"

    def test_passes_when_notebook_exists(self):
        from overture_airflow_provider._databricks import preflight_databricks_runner

        mock_hook = MagicMock()
        mock_hook._do_api_call.return_value = {"object_type": "NOTEBOOK"}

        with patch(
            "airflow.providers.databricks.hooks.databricks.DatabricksHook",
            return_value=mock_hook,
        ):
            preflight_databricks_runner({}, self._CLUSTER_INFO)

        mock_hook._do_api_call.assert_called_once_with(
            ("GET", "2.0/workspace/get-status"),
            {"path": self._NOTEBOOK_PATH},
            wrap_http_errors=False,
        )

    def test_raises_actionable_error_when_notebook_missing(self):
        from requests.exceptions import HTTPError

        from overture_airflow_provider._databricks import preflight_databricks_runner

        mock_hook = MagicMock()
        mock_hook._do_api_call.side_effect = HTTPError(response=MagicMock(status_code=404))

        with patch(
            "airflow.providers.databricks.hooks.databricks.DatabricksHook",
            return_value=mock_hook,
        ):
            with pytest.raises(RuntimeError, match="runner notebook not found"):
                preflight_databricks_runner({}, self._CLUSTER_INFO)

    def test_warns_and_proceeds_on_non_404_http_error(self, capsys):
        from requests.exceptions import HTTPError

        from overture_airflow_provider._databricks import preflight_databricks_runner

        mock_hook = MagicMock()
        mock_hook._do_api_call.side_effect = HTTPError(response=MagicMock(status_code=500))

        with patch(
            "airflow.providers.databricks.hooks.databricks.DatabricksHook",
            return_value=mock_hook,
        ):
            preflight_databricks_runner({}, self._CLUSTER_INFO)

        assert "could not verify runner notebook" in capsys.readouterr().out

    def test_warns_and_proceeds_on_auth_error(self, capsys):
        from overture_airflow_provider._databricks import preflight_databricks_runner

        mock_hook = MagicMock()
        mock_hook._do_api_call.side_effect = RuntimeError("auth boom")

        with patch(
            "airflow.providers.databricks.hooks.databricks.DatabricksHook",
            return_value=mock_hook,
        ):
            preflight_databricks_runner({}, self._CLUSTER_INFO)

        assert "could not verify runner notebook" in capsys.readouterr().out

    def test_checks_both_assets_when_init_script_present(self):
        from overture_airflow_provider._databricks import preflight_databricks_runner

        mock_hook = MagicMock()
        mock_hook._do_api_call.return_value = {"object_type": "NOTEBOOK"}

        setup_info = {"databricks_cluster_init_script_name": "init.sh"}
        with patch(
            "airflow.providers.databricks.hooks.databricks.DatabricksHook",
            return_value=mock_hook,
        ):
            preflight_databricks_runner(setup_info, self._CLUSTER_INFO)

        checked_paths = [c.args[1]["path"] for c in mock_hook._do_api_call.call_args_list]
        assert self._NOTEBOOK_PATH in checked_paths
        assert "/Workspace/Shared/spark-agnostic-operator/init.sh" in checked_paths

    def test_raises_actionable_error_when_init_script_missing(self):
        from requests.exceptions import HTTPError

        from overture_airflow_provider._databricks import preflight_databricks_runner

        init_path = "/Workspace/Shared/spark-agnostic-operator/init.sh"

        def _status(method, params, **kwargs):
            if params["path"] == init_path:
                raise HTTPError(response=MagicMock(status_code=404))
            return {"object_type": "NOTEBOOK"}

        mock_hook = MagicMock()
        mock_hook._do_api_call.side_effect = _status

        setup_info = {"databricks_cluster_init_script_name": "init.sh"}
        with patch(
            "airflow.providers.databricks.hooks.databricks.DatabricksHook",
            return_value=mock_hook,
        ):
            with pytest.raises(RuntimeError, match="cluster init script not found"):
                preflight_databricks_runner(setup_info, self._CLUSTER_INFO)


class TestWherobotsSetupCluster:
    def _run(self, extra_spark_conf=None, iceberg_spark_config=None):
        handler = WherobotsPlatformHandler(_wherobots_setup_info())
        return handler.setup_cluster(
            python_packages="overture-spark==1.0",
            spark_jar_paths="",
            extra_spark_conf=extra_spark_conf or {},
            extra_spark_env_vars="{}",
            spark_cluster_desired_worker_cores="40",
            spark_cluster_desired_workers="",
            iceberg_spark_config=iceberg_spark_config,
        )

    def test_returns_merged_spark_conf(self):
        assert "merged_spark_conf" in self._run()

    def test_default_keys_present(self):
        conf = self._run()["merged_spark_conf"]
        assert "mapreduce.fileoutputcommitter.marksuccessfuljobs" in conf

    def test_no_iceberg_when_no_config(self):
        conf = self._run(iceberg_spark_config=None)["merged_spark_conf"]
        assert "spark.sql.defaultCatalog" not in conf

    def test_iceberg_injected_when_config_provided(self):
        iceberg_conf = _mock_iceberg_wherobots("s3://my-iceberg-bucket/")
        conf = self._run(iceberg_spark_config=iceberg_conf)["merged_spark_conf"]
        assert "spark.sql.defaultCatalog" in conf
        assert conf[_ICEBERG_WHEROBOTS_KEY] == _ICEBERG_WHEROBOTS_IMPL

    def test_user_override_takes_precedence(self):
        conf = self._run(
            extra_spark_conf={"mapreduce.fileoutputcommitter.marksuccessfuljobs": "true"}
        )["merged_spark_conf"]
        assert conf["mapreduce.fileoutputcommitter.marksuccessfuljobs"] == "true"

    def test_glue_java_options_not_in_wherobots_defaults(self):
        conf = self._run()["merged_spark_conf"]
        assert "spark.driver.extraJavaOptions" not in conf
        assert "spark.executor.extraJavaOptions" not in conf


class TestWherobotsExecuteJob:
    _UNSUPPORTED = [
        "spark.driver.extraJavaOptions",
        "spark.executor.extraJavaOptions",
        "sedona.join.numpartition",
        "spark.kryoserializer.buffer",
        "spark.driver.maxResultSize",
        "spark.sql.sources.partitionOverwriteMode",
    ]

    def _run(
        self,
        module_name="my_module",
        class_name="MyClass",
        extra_spark_conf=None,
        spark_cluster_size="",
        desired_cores="40",
        package_info=None,
        jar_info=None,
        parameters='{"key": "value"}',
        simulate_submit=False,
        mapping_context=False,
    ):
        from overture_airflow_provider._wherobots import execute_wherobots_job

        setup_info = _wherobots_setup_info()
        setup_info["parameters"] = parameters

        if package_info is None:
            package_info = {
                "py_files": ["s3://bucket/pkg.whl"],
                "script_location": "s3://bucket/job_runner_wherobots.py",
                "python_packages_or_jars_list": [
                    {"sourceType": "FILE", "filePath": "s3://bucket/pkg.whl"}
                ],
            }
        if jar_info is None:
            jar_info = {"jars_s3": []}

        captured_operator_kwargs = {}
        context = _AirflowContext({"ti": MagicMock()}) if mapping_context else {"ti": MagicMock()}

        mock_operator = MagicMock()
        if simulate_submit:
            mock_operator.execute.side_effect = lambda ctx: ctx["ti"].xcom_push(
                key="run_id", value="wb_run_123"
            )
        else:
            mock_operator.execute.return_value = None

        mock_sts = MagicMock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789012"}

        with (
            patch(
                "overture_airflow_provider._wherobots.WHEROBOTS_AVAILABLE",
                True,
            ),
            patch(
                "overture_airflow_provider._wherobots.WherobotsRunOperator",
                return_value=mock_operator,
            ) as MockWherobots,
            patch(
                "overture_airflow_provider._wherobots.Region",
                MagicMock(AWS_US_WEST_2="us-west-2"),
            ),
            patch(
                "overture_airflow_provider._wherobots._resolve_wherobots_region",
                return_value="us-east-1",
            ),
            patch(
                "overture_airflow_provider._wherobots.BaseHook.get_connection",
                return_value=MagicMock(host="api.cloud.wherobots.com"),
            ),
        ):
            result = execute_wherobots_job(
                setup_info=setup_info,
                package_info=package_info,
                jar_info=jar_info,
                module_name=module_name,
                class_name=class_name,
                extra_spark_conf=extra_spark_conf or {},
                spark_cluster_size=spark_cluster_size,
                spark_cluster_desired_worker_cores=desired_cores,
                spark_cluster_desired_workers="",
                wherobots_role_arn="arn:aws:iam::123456789012:role/wherobots-access",
                task_id="execute_spark_job",
                context=context,
            )
            if MockWherobots.called:
                captured_operator_kwargs.update(MockWherobots.call_args[1])
        captured_operator_kwargs["context"] = context

        return result, captured_operator_kwargs

    @pytest.mark.parametrize("unsupported_key", _UNSUPPORTED)
    def test_unsupported_spark_configs_stripped(self, unsupported_key):
        _, kwargs = self._run(
            extra_spark_conf={unsupported_key: "some_value", "spark.safe.key": "kept"}
        )
        spark_configs = kwargs["environment"]["sparkConfigs"]
        assert unsupported_key not in spark_configs

    def test_supported_configs_retained(self):
        _, kwargs = self._run(extra_spark_conf={"spark.safe.key": "kept"})
        spark_configs = kwargs["environment"]["sparkConfigs"]
        assert spark_configs.get("spark.safe.key") == "kept"

    def test_python_job_uses_run_python(self):
        _, kwargs = self._run(module_name="my_module", class_name="MyClass")
        assert "run_python" in kwargs
        assert "run_jar" not in kwargs

    def test_scala_job_uses_run_jar(self):
        jar_info = {"jars_s3": ["s3://bucket/my-pipeline-1.0.jar"]}
        package_info = {
            "py_files": [],
            "script_location": "",
            "python_packages_or_jars_list": [
                {"sourceType": "FILE", "filePath": "s3://bucket/my-pipeline-1.0.jar"}
            ],
        }
        _, kwargs = self._run(
            module_name="",
            class_name="com.example.Main",
            jar_info=jar_info,
            package_info=package_info,
        )
        assert "run_jar" in kwargs
        assert "run_python" not in kwargs

    def test_scala_job_run_jar_main_class(self):
        jar_info = {"jars_s3": ["s3://bucket/my-pipeline-1.0.jar"]}
        package_info = {
            "py_files": [],
            "script_location": "",
            "python_packages_or_jars_list": [
                {"sourceType": "FILE", "filePath": "s3://bucket/my-pipeline-1.0.jar"}
            ],
        }
        _, kwargs = self._run(
            module_name="",
            class_name="com.example.Main",
            jar_info=jar_info,
            package_info=package_info,
        )
        assert kwargs["run_jar"]["mainClass"] == "com.example.Main"

    def test_scala_job_skips_sedona_jar_for_main(self):
        jar_info = {"jars_s3": []}
        package_info = {
            "py_files": [],
            "script_location": "",
            "python_packages_or_jars_list": [
                {
                    "sourceType": "FILE",
                    "filePath": "s3://bucket/sedona-spark-shaded-3.5_2.12-1.7.0.jar",
                },
                {
                    "sourceType": "FILE",
                    "filePath": "s3://bucket/geotools-wrapper-1.7.0-28.5.jar",
                },
                {"sourceType": "FILE", "filePath": "s3://bucket/my-pipeline-1.0.jar"},
            ],
        }
        _, kwargs = self._run(
            module_name="",
            class_name="com.example.Main",
            jar_info=jar_info,
            package_info=package_info,
        )
        assert "my-pipeline-1.0.jar" in kwargs["run_jar"]["uri"]

    def test_iceberg_catalog_configs_injected_when_default_catalog_present(self):
        _, kwargs = self._run(extra_spark_conf={"spark.sql.defaultCatalog": "iceberg_catalog"})
        spark_configs = kwargs["environment"]["sparkConfigs"]
        assert any("client.factory" in k for k in spark_configs)

    def test_no_iceberg_injection_without_default_catalog(self):
        _, kwargs = self._run(extra_spark_conf={})
        spark_configs = kwargs["environment"]["sparkConfigs"]
        assert not any("client.factory" in k for k in spark_configs)

    def test_python_job_args_contain_module_and_class(self):
        _, kwargs = self._run(module_name="my_module", class_name="MyClass")
        args = kwargs["run_python"]["args"]
        assert "--module_name" in args
        assert "my_module" in args
        assert "--class_name" in args
        assert "MyClass" in args

    def test_scala_job_args_built_from_parameters(self):
        jar_info = {"jars_s3": ["s3://bucket/my-pipeline-1.0.jar"]}
        package_info = {
            "py_files": [],
            "script_location": "",
            "python_packages_or_jars_list": [
                {"sourceType": "FILE", "filePath": "s3://bucket/my-pipeline-1.0.jar"}
            ],
        }
        params = json.dumps({"s3_input": "s3://bucket/in", "s3_output": "s3://bucket/out"})
        _, kwargs = self._run(
            module_name="",
            class_name="com.example.Main",
            jar_info=jar_info,
            package_info=package_info,
            parameters=params,
        )
        args = kwargs["run_jar"]["args"]
        assert "--s3_input" in args
        assert "s3://bucket/in" in args

    def test_wherobots_unavailable_raises(self):
        from overture_airflow_provider._wherobots import execute_wherobots_job

        with patch("overture_airflow_provider._wherobots.WHEROBOTS_AVAILABLE", False):
            with pytest.raises(ImportError, match="Wherobots"):
                execute_wherobots_job(
                    setup_info=_wherobots_setup_info(),
                    package_info={},
                    jar_info={},
                    module_name="mod",
                    class_name="Cls",
                    extra_spark_conf={},
                    spark_cluster_size="",
                    spark_cluster_desired_worker_cores="40",
                    spark_cluster_desired_workers="",
                    wherobots_role_arn="arn:aws:iam::123456789012:role/test",
                    task_id="t",
                    context={},
                )

    def test_pushes_spark_agnostic_xcom_after_submission(self):
        _, kwargs = self._run(simulate_submit=True)
        calls = kwargs["context"]["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls
        payload = json.loads(spark_agnostic_calls[0].kwargs["value"])
        assert payload["job_url"].endswith("/runs/wb_run_123")
        assert "api." not in payload["job_url"]
        assert payload["status"] == "RUNNING"

    def test_early_xcom_push_fires_with_non_dict_context(self):
        # Airflow 3.x passes a Mapping Context (not a dict). Regression for #33.
        _, kwargs = self._run(simulate_submit=True, mapping_context=True)
        assert not isinstance(kwargs["context"], dict)
        calls = kwargs["context"]["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls

    def test_returns_job_url_in_result_after_execution(self):
        # Regression: execute_wherobots_job must return job_url so the final
        # spark_agnostic XCom push (in spark_agnostic_task_group) preserves it.
        result, _ = self._run(simulate_submit=True)
        assert "job_url" in result
        assert result["job_url"].endswith("/runs/wb_run_123")
        assert "api." not in result["job_url"]

    def test_returns_no_job_url_when_run_id_never_pushed(self):
        # When the operator never pushes run_id (e.g. dry-run / mock with no submit),
        # job_url should be absent rather than present with a None/empty value.
        result, _ = self._run(simulate_submit=False)
        assert "job_url" not in result


class TestSparkJobLink:
    """Tests for SparkJobLink.get_link."""

    def _make_link(self):
        from overture_airflow_provider.links import SparkJobLink

        return SparkJobLink()

    def _make_operator(self, dag_id="test_dag", task_id="execute_spark_job"):
        op = MagicMock()
        op.dag_id = dag_id
        op.task_id = task_id
        return op

    def test_returns_url_from_xcom_via_ti_key(self):
        link = self._make_link()
        operator = self._make_operator()
        ti_key = MagicMock()
        payload = json.dumps({"job_url": "https://example.com/runs/123"})

        with patch("overture_airflow_provider.links.XCom.get_value", return_value=payload):
            url = link.get_link(operator, ti_key=ti_key)

        assert url == "https://example.com/runs/123"

    def test_returns_url_from_xcom_via_dttm_fallback(self):
        link = self._make_link()
        operator = self._make_operator()
        payload = json.dumps({"job_url": "https://example.com/runs/456"})

        with patch("overture_airflow_provider.links.XCom.get_one", return_value=payload):
            url = link.get_link(operator, dttm=MagicMock())

        assert url == "https://example.com/runs/456"

    def test_returns_url_from_dict_xcom(self):
        """Airflow 3.x may deserialize the XCom value to a dict directly."""
        link = self._make_link()
        operator = self._make_operator()
        ti_key = MagicMock()

        with patch(
            "overture_airflow_provider.links.XCom.get_value",
            return_value={"job_url": "https://example.com/runs/789"},
        ):
            url = link.get_link(operator, ti_key=ti_key)

        assert url == "https://example.com/runs/789"

    def test_returns_empty_string_when_xcom_absent(self):
        link = self._make_link()
        operator = self._make_operator()

        with patch("overture_airflow_provider.links.XCom.get_value", return_value=None):
            url = link.get_link(operator, ti_key=MagicMock())

        assert url == ""

    def test_returns_empty_string_when_job_url_missing_from_payload(self):
        link = self._make_link()
        operator = self._make_operator()
        payload = json.dumps({"status": "RUNNING"})

        with patch("overture_airflow_provider.links.XCom.get_value", return_value=payload):
            url = link.get_link(operator, ti_key=MagicMock())

        assert url == ""

    def test_returns_empty_string_on_malformed_json(self):
        link = self._make_link()
        operator = self._make_operator()

        with patch("overture_airflow_provider.links.XCom.get_value", return_value="not-json{"):
            url = link.get_link(operator, ti_key=MagicMock())

        assert url == ""
