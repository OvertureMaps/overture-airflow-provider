"""overture-core is an optional extra: importing the provider must not need it."""

import subprocess
import sys
import tempfile
import zipfile
from unittest.mock import patch

import pytest

from overture_airflow_provider._overture_core import import_overture_core
from overture_airflow_provider._setup import setup_spark_job
from overture_airflow_provider.setup_info import rehydrate
from overture_airflow_provider.spark_agnostic_helpers import SparkAgnosticHelper


@pytest.fixture
def without_overture_core(monkeypatch):
    class BlockOvertureCore:
        def find_spec(self, fullname, path=None, target=None):
            if fullname == "overture_core":
                raise ModuleNotFoundError("No module named 'overture_core'", name=fullname)

    for name in list(sys.modules):
        if name == "overture_core" or name.startswith("overture_core."):
            monkeypatch.delitem(sys.modules, name)
    monkeypatch.setattr(sys, "meta_path", [BlockOvertureCore(), *sys.meta_path])


def test_provider_modules_import_without_overture_core():
    code = (
        "import sys\n"
        "sys.modules['overture_core'] = None\n"
        "import overture_airflow_provider\n"
        "import overture_airflow_provider._setup\n"
        "import overture_airflow_provider.setup_info\n"
        "import overture_airflow_provider.spark_agnostic_helpers\n"
        "from overture_airflow_provider import spark_agnostic_task_group, render_spark_job\n"
        "from overture_airflow_provider.plugins.bundle_inspector import BundleInspectorPlugin\n"
        "for impl in ('GLUE_v5', 'DATABRICKS_v15'):\n"
        "    result = render_spark_job(spark_impl_name=impl, module_name='example', class_name='Job', parameters={})\n"
        "    assert result.submit_payload\n"
        "assert sys.modules['overture_core'] is None\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_missing_overture_core_points_at_extra(without_overture_core):
    with pytest.raises(ModuleNotFoundError, match=r"airflow-provider-overture\[overture\]"):
        import_overture_core("overture_core.pypi")


def test_unrelated_missing_module_is_not_rewritten():
    with pytest.raises(ModuleNotFoundError) as exc_info:
        import_overture_core("not_a_real_module_xyz")
    assert "overture-core" not in str(exc_info.value)


@pytest.mark.parametrize("name", ["requests", "overture_core.pypi", "overture_core.cloud.aws"])
def test_missing_dependency_or_submodule_is_not_rewritten(name):
    error = ModuleNotFoundError(f"No module named '{name}'", name=name)
    with patch(
        "overture_airflow_provider._overture_core.importlib.import_module", side_effect=error
    ):
        with pytest.raises(ModuleNotFoundError) as exc_info:
            import_overture_core("overture_core.pypi")
    assert exc_info.value is error


@pytest.mark.parametrize("operation", ["setup", "rehydrate", "python", "jars", "scripts"])
def test_spark_runtime_without_extra_fails_cleanly(
    operation, without_overture_core, tmp_path, monkeypatch
):
    monkeypatch.setattr(tempfile, "tempdir", str(tmp_path))
    helper = SparkAgnosticHelper("job", "run", "bucket", force_pip_packages=["native"])
    packages = ["native==1.0", "example"]
    wheel = tmp_path / "runner.whl"
    with zipfile.ZipFile(wheel, "w") as archive:
        archive.writestr("runner.py", "print('job')")

    operations = {
        "setup": lambda: setup_spark_job("GLUE_v5", "1.7.0", "example", "Job", "job", "{}", ""),
        "rehydrate": lambda: rehydrate(
            {
                "spark_impl_name": "GLUE_v5",
                "spark_family_name": "GLUE",
                "codeartifact_domain_owner": "123456789012",
                "codeartifact_domain": "domain",
                "codeartifact_repository": "repo",
                "codeartifact_region": "us-east-1",
            }
        ),
        "python": lambda: helper.download_and_cache_python_packages(None, packages, "3.11"),
        "jars": lambda: helper.download_and_cache_jars(["https://example.com/job.jar"]),
        "scripts": lambda: helper.extract_job_runner_scripts(str(wheel), ["runner.py"]),
    }
    with pytest.raises(ModuleNotFoundError, match=r"airflow-provider-overture\[overture\]"):
        operations[operation]()
    assert packages == ["native==1.0", "example"]
    assert list(tmp_path.iterdir()) == [wheel]
