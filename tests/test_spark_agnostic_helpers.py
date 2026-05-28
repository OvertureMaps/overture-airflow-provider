"""Tests for SparkAgnosticHelper."""

import os
import tempfile
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from overture_airflow_provider.spark_agnostic_helpers import SparkAgnosticHelper


def _make_s3_mock(existing_keys: set = None):
    existing_keys = set(existing_keys or [])
    uploaded = []

    mock_s3 = MagicMock()

    def head_object(Bucket, Key):
        if Key in existing_keys:
            return {"ContentLength": 100}
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")

    def upload_file(local_path, bucket, key, **kwargs):
        uploaded.append((bucket, key))
        existing_keys.add(key)

    mock_s3.head_object.side_effect = head_object
    mock_s3.upload_file.side_effect = upload_file
    mock_s3.exceptions.ClientError = ClientError

    mock_s3._uploaded = uploaded
    mock_s3._existing_keys = existing_keys
    return mock_s3


def _make_helper(s3_mock, bucket="test-glue-assets"):
    helper = SparkAgnosticHelper(
        job_name="test.Job",
        run_identifier="test.Job/20240101120000",
        s3_bucket=bucket,
    )
    helper.s3_client = s3_mock
    return helper


class TestParseWheelFilename:
    def test_native_linux_wheel(self):
        name, ver = SparkAgnosticHelper._parse_wheel_filename(
            "numba-0.59.0-cp311-cp311-manylinux_2_17_x86_64.whl"
        )
        assert name == "numba"
        assert ver == "0.59.0"

    def test_pure_python_wheel(self):
        name, ver = SparkAgnosticHelper._parse_wheel_filename("requests-2.31.0-py3-none-any.whl")
        assert name == "requests"
        assert ver == "2.31.0"

    def test_underscore_to_hyphen(self):
        name, ver = SparkAgnosticHelper._parse_wheel_filename(
            "overture_spark-1.0.0-py3-none-any.whl"
        )
        assert name == "overture-spark"
        assert ver == "1.0.0"

    def test_invalid_returns_none(self):
        assert SparkAgnosticHelper._parse_wheel_filename("invalid") == (None, None)

    def test_empty_returns_none(self):
        assert SparkAgnosticHelper._parse_wheel_filename("") == (None, None)


class TestHasNativeDependencies:
    def test_pure_python_false(self):
        assert SparkAgnosticHelper._has_native_dependencies("pkg-1.0-py3-none-any.whl") is False

    def test_py2_py3_false(self):
        assert (
            SparkAgnosticHelper._has_native_dependencies("six-1.16.0-py2.py3-none-any.whl") is False
        )

    def test_manylinux_true(self):
        assert (
            SparkAgnosticHelper._has_native_dependencies(
                "numpy-1.24.0-cp311-cp311-manylinux_2_17_x86_64.whl"
            )
            is True
        )

    def test_macos_true(self):
        assert (
            SparkAgnosticHelper._has_native_dependencies(
                "numpy-1.24.0-cp311-cp311-macosx_11_0_arm64.whl"
            )
            is True
        )


class TestForcePipInstall:
    def _helper(self, force_pip_packages=None):
        h = SparkAgnosticHelper.__new__(SparkAgnosticHelper)
        h.force_pip_packages = list(force_pip_packages or [])
        return h

    def test_configured_substring_forced(self):
        h = self._helper(force_pip_packages=["sentence-transformers"])
        assert h._force_pip_install("sentence-transformers==2.2.0") is True

    def test_regular_package_not_forced(self):
        h = self._helper(force_pip_packages=["sentence-transformers"])
        assert h._force_pip_install("numba==0.59.0") is False

    def test_empty_list_never_forces(self):
        h = self._helper(force_pip_packages=[])
        assert h._force_pip_install("sentence-transformers==2.2.0") is False


class TestDownloadAndCachePythonPackages:
    def _run(
        self,
        wheel_files,
        requested_packages,
        existing_s3_keys=None,
        job_runner_wheel_prefix=None,
    ):
        s3_mock = _make_s3_mock(existing_s3_keys)
        helper = _make_helper(s3_mock)

        mock_client = MagicMock()

        tmp_dir = tempfile.mkdtemp()
        for fname in wheel_files:
            open(os.path.join(tmp_dir, fname), "wb").close()

        def fake_download(packages, python_version):
            pass

        with (
            patch(
                "overture_airflow_provider.spark_agnostic_helpers.PyPiDownloader"
            ) as MockDownloader,
            patch(
                "overture_airflow_provider.spark_agnostic_helpers.tempfile.mkdtemp",
                return_value=tmp_dir,
            ),
        ):
            MockDownloader.return_value.download_packages.side_effect = fake_download
            result = helper.download_and_cache_python_packages(
                py_pi_client=mock_client,
                packages=list(requested_packages),
                python_version="3.11",
                job_runner_wheel_prefix=job_runner_wheel_prefix,
            )

        result_with_meta = result + (s3_mock,)
        return result_with_meta

    def test_pure_python_wheel_uploaded_on_cache_miss(self):
        py_files, _, _, native, s3 = self._run(
            wheel_files=["requests-2.31.0-py3-none-any.whl"],
            requested_packages=["requests"],
        )
        assert len(s3._uploaded) == 1
        assert (
            "python_wheels/codeartifact_cache/requests-2.31.0-py3-none-any.whl"
            in s3._uploaded[0][1]
        )

    def test_pure_python_wheel_not_uploaded_on_cache_hit(self):
        py_files, _, _, native, s3 = self._run(
            wheel_files=["requests-2.31.0-py3-none-any.whl"],
            requested_packages=["requests"],
            existing_s3_keys={"python_wheels/codeartifact_cache/requests-2.31.0-py3-none-any.whl"},
        )
        assert len(s3._uploaded) == 0
        assert "requests-2.31.0-py3-none-any.whl" in py_files

    def test_cache_hit_path_in_py_files(self):
        py_files, _, _, native, s3 = self._run(
            wheel_files=["requests-2.31.0-py3-none-any.whl"],
            requested_packages=["requests"],
            existing_s3_keys={"python_wheels/codeartifact_cache/requests-2.31.0-py3-none-any.whl"},
        )
        assert (
            "s3://test-glue-assets/python_wheels/codeartifact_cache/requests-2.31.0-py3-none-any.whl"
            in py_files
        )

    def test_native_wheel_not_uploaded_to_s3(self):
        _, _, _, native, s3 = self._run(
            wheel_files=["numba-0.59.0-cp311-cp311-manylinux_2_17_x86_64.whl"],
            requested_packages=["numba"],
        )
        assert len(s3._uploaded) == 0

    def test_explicit_native_package_tracked(self):
        _, _, _, native, s3 = self._run(
            wheel_files=["numba-0.59.0-cp311-cp311-manylinux_2_17_x86_64.whl"],
            requested_packages=["numba"],
        )
        assert any("numba" in p for p in native)

    def test_transitive_native_dep_not_tracked(self):
        _, _, _, native, _ = self._run(
            wheel_files=[
                "numba-0.59.0-cp311-cp311-manylinux_2_17_x86_64.whl",
                "numpy-1.24.0-cp311-cp311-manylinux_2_17_x86_64.whl",
            ],
            requested_packages=["numba"],
        )
        assert any("numba" in p for p in native)
        assert not any("numpy" in p for p in native)

    def test_mixed_cache_hit_and_miss(self):
        _, _, _, native, s3 = self._run(
            wheel_files=[
                "requests-2.31.0-py3-none-any.whl",
                "six-1.16.0-py2.py3-none-any.whl",
            ],
            requested_packages=["requests", "six"],
            existing_s3_keys={"python_wheels/codeartifact_cache/requests-2.31.0-py3-none-any.whl"},
        )
        assert len(s3._uploaded) == 1
        assert "six-1.16.0-py2.py3-none-any.whl" in s3._uploaded[0][1]

    def test_overture_spark_wheel_tracked_when_requested(self):
        _, overture_spark_whl, _, _, _ = self._run(
            wheel_files=["overture_spark-1.0.0-py3-none-any.whl"],
            requested_packages=["overture-spark==1.0.0"],
            job_runner_wheel_prefix="overture_spark",
        )
        assert overture_spark_whl is not None
        assert "overture_spark" in overture_spark_whl

    def test_empty_packages_returns_empty(self):
        py_files, _, _, native, s3 = self._run(
            wheel_files=[],
            requested_packages=[],
        )
        assert py_files == ""
        assert native == []
        assert len(s3._uploaded) == 0


class TestDownloadAndCacheJars:
    def _run(self, jar_urls, existing_s3_keys=None, pre_provisioned=None):
        s3_mock = _make_s3_mock(existing_s3_keys)
        helper = _make_helper(s3_mock)

        with patch("overture_airflow_provider.spark_agnostic_helpers.HttpDownloader") as MockHttp:
            tmp_dir = tempfile.mkdtemp()
            MockHttp.return_value = MagicMock()

            def _fake_dl(urls):
                paths = []
                for url in urls:
                    fname = url.rstrip("/").split("/")[-1].split("?")[0]
                    p = os.path.join(tmp_dir, fname)
                    open(p, "wb").close()
                    paths.append(p)
                return paths

            MockHttp.return_value.download_urls.side_effect = _fake_dl

            with patch(
                "overture_airflow_provider.spark_agnostic_helpers.tempfile.mkdtemp",
                return_value=tmp_dir,
            ):
                result = helper.download_and_cache_jars(
                    jar_urls=jar_urls,
                    pre_provisioned_jars=pre_provisioned or [],
                )

        return result, s3_mock

    def test_codeartifact_jar_cached_on_hit(self):
        jar_url = "https://example-123.d.codeartifact.us-west-2.amazonaws.com/maven/example-maven/my-job-1.0.jar"
        jar_fname = "my-job-1.0.jar"
        cache_key = f"scala_jars/codeartifact_cache/{jar_fname}"

        result, s3 = self._run(
            jar_urls=[jar_url],
            existing_s3_keys={cache_key},
        )

        assert len(s3._uploaded) == 0
        assert f"s3://test-glue-assets/{cache_key}" in result

    def test_codeartifact_jar_uploaded_on_miss(self):
        jar_url = "https://example-123.d.codeartifact.us-west-2.amazonaws.com/maven/example-maven/my-job-1.0.jar"

        result, s3 = self._run(jar_urls=[jar_url])

        assert len(s3._uploaded) == 1
        assert "scala_jars/codeartifact_cache/my-job-1.0.jar" in s3._uploaded[0][1]

    def test_non_codeartifact_jar_uploaded_to_job_prefix(self):
        jar_url = "https://repo1.maven.org/maven2/org/apache/sedona/sedona.jar"

        result, s3 = self._run(jar_urls=[jar_url])

        assert len(s3._uploaded) == 1
        uploaded_key = s3._uploaded[0][1]
        assert "scala_jars/codeartifact_cache" not in uploaded_key
        assert "test.Job" in uploaded_key or "jars" in uploaded_key

    def test_pre_provisioned_s3_paths_included(self):
        pre = ["s3://my-bucket/scala_jars/pre-existing.jar"]
        result, s3 = self._run(jar_urls=[], pre_provisioned=pre)
        assert "s3://my-bucket/scala_jars/pre-existing.jar" in result

    def test_empty_jar_url_skipped(self):
        result, s3 = self._run(jar_urls=["", "  "])
        assert len(s3._uploaded) == 0

    def test_result_is_comma_separated_string(self):
        pre = [
            "s3://bucket/a.jar",
            "s3://bucket/b.jar",
        ]
        result, _ = self._run(jar_urls=[], pre_provisioned=pre)
        parts = result.split(",")
        assert len(parts) == 2
