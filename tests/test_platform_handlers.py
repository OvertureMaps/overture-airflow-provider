"""Tests for SparkPlatformHandler subclasses."""

import json
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


_DEFAULT_PROVIDER_KEYS = {
    "force_pip_packages": [],
    "databricks_extra_libraries": [],
    "databricks_dbfs_root_template": "dbfs:/FileStore/deploy/{s3_assets_root}",
    "databricks_workspace_scripts_path_template": "/Workspace/Shared/{s3_assets_root}",
    "databricks_cluster_init_script_name": "agnostic_operator_cluster_init_databricks.sh",
    "databricks_custom_tags": {},
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
    def _make_context(self):
        ctx = {
            "ti": MagicMock(),
            "dag": MagicMock(dag_id="test_dag"),
        }
        ctx["ti"].task_id = "execute_spark_job"
        return ctx

    def _run_glue(
        self,
        module_name="my_module",
        class_name="MyClass",
        extra_spark_conf=None,
        desired_worker_cores="40",
        desired_workers="",
        iam_role_name="AWSGlueServiceRole",
        simulate_submit=False,
    ):
        from overture_airflow_provider._glue import execute_glue_job

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

        def fake_execute(context):
            if simulate_submit:
                context["ti"].xcom_push(key="glue_job_run_id", value="jr_early123")

        mock_operator = MagicMock()
        mock_operator.execute.side_effect = fake_execute

        mock_glue_client = MagicMock()
        mock_glue_client.get_job.side_effect = MagicMock(
            side_effect=mock_glue_client.exceptions.EntityNotFoundException
        )

        with (
            patch(
                "overture_airflow_provider._glue.GlueJobOperator",
                return_value=mock_operator,
            ) as MockOperator,
            patch(
                "overture_airflow_provider._glue.boto3.client",
                return_value=mock_glue_client,
            ),
            patch(
                "overture_airflow_provider._glue._get_glue_job_url_and_status",
                return_value={
                    "job_url": "https://console.aws.amazon.com/...",
                    "status": {"JobRunState": "SUCCEEDED"},
                },
            ),
        ):
            context = self._make_context()
            result = execute_glue_job(
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

    def test_result_contains_job_url(self):
        result, _ = self._run_glue()
        assert "job_url" in result

    def test_pushes_spark_agnostic_xcom_after_submission(self):
        _, captured = self._run_glue(simulate_submit=True)
        calls = captured["context"]["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls
        payload = json.loads(spark_agnostic_calls[0].kwargs["value"])
        assert payload["job_url"].endswith("/run/jr_early123")
        assert payload["status"] == "RUNNING"


class TestGetGlueJobUrlAndStatus:
    def _make_operator(self, job_state):
        op = MagicMock()
        op.xcom_pull.return_value = {
            "region_name": "us-west-2",
            "aws_domain": "aws.amazon.com",
            "job_name": "test-job",
            "job_run_id": "jr_abc123",
        }
        return op

    def _run(self, job_state):
        from overture_airflow_provider._glue import _get_glue_job_url_and_status

        operator = self._make_operator(job_state)
        context = {"ti": MagicMock(task_id="execute_spark_job")}

        mock_glue = MagicMock()
        mock_glue.get_job_run.return_value = {"JobRun": {"JobRunState": job_state}}

        with patch(
            "overture_airflow_provider._glue.boto3.client",
            return_value=mock_glue,
        ):
            return _get_glue_job_url_and_status(operator, context, _glue_setup_info())

    def test_succeeded_returns_result(self):
        result = self._run("SUCCEEDED")
        assert "job_url" in result
        assert "status" in result

    def test_job_url_contains_job_name(self):
        result = self._run("SUCCEEDED")
        assert "test-job" in result["job_url"]

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

    def test_download_python_packages_returns_none(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        assert handler.download_python_packages("anything") is None

    def test_download_jars_returns_none(self):
        handler = DatabricksPlatformHandler(_databricks_setup_info())
        assert handler.download_jars() is None


class TestDatabricksExecuteJob:
    def test_pushes_spark_agnostic_xcom_after_submission(self):
        from overture_airflow_provider._databricks import execute_databricks_job

        setup_info = _databricks_setup_info()
        cluster_info = {
            "new_cluster": {"spark_version": "15.4.x-scala2.12"},
            "libraries": [],
            "databricks_conf": {"databricks_conn_id": "databricks_default"},
            "databricks_deployed_scripts_path": "/Workspace/Shared/spark-agnostic-operator",
        }
        context = {"ti": MagicMock()}

        mock_operator = MagicMock()

        def fake_execute(ctx):
            ctx["ti"].xcom_push(key="run_id", value="12345")
            ctx["ti"].xcom_push(key="run_page_url", value="https://dbc.example/runs/12345")

        mock_operator.execute.side_effect = fake_execute
        mock_operator.xcom_pull.side_effect = lambda _ctx, key: {
            "run_id": "12345",
            "run_page_url": "https://dbc.example/runs/12345",
        }[key]

        mock_conn = MagicMock()
        mock_conn.host = "https://dbc.example"
        mock_conn.password = "token"
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"state": {"result_state": "SUCCESS"}}

        with (
            patch(
                "airflow.providers.databricks.operators.databricks.DatabricksSubmitRunOperator",
                return_value=mock_operator,
            ),
            patch(
                "overture_airflow_provider._airflow_compat.BaseHook.get_connection",
                return_value=mock_conn,
            ),
            patch("overture_airflow_provider._databricks.requests.get", return_value=mock_resp),
        ):
            execute_databricks_job(
                setup_info=setup_info,
                cluster_info=cluster_info,
                module_name="my_module",
                class_name="MyClass",
                parameters='{"key":"value"}',
                task_id="execute_spark_job",
                context=context,
            )

        calls = context["ti"].xcom_push.call_args_list
        spark_agnostic_calls = [c for c in calls if c.kwargs.get("key") == "spark_agnostic"]
        assert spark_agnostic_calls
        payload = json.loads(spark_agnostic_calls[0].kwargs["value"])
        assert payload["job_url"] == "https://dbc.example/runs/12345"
        assert payload["status"] == "RUNNING"


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
        context = {"ti": MagicMock()}

        mock_operator = MagicMock()
        if simulate_submit:
            mock_operator.execute.side_effect = (
                lambda ctx: ctx["ti"].xcom_push(key="run_id", value="wb_run_123")
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
        assert payload["job_url"] == "https://cloud.wherobots.com/runs/wb_run_123"
        assert payload["status"] == "RUNNING"
