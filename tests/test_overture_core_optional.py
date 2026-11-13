"""overture-core is an optional extra: importing the provider must not need it."""

import subprocess
import sys

import pytest

from overture_airflow_provider._overture_core import import_overture_core


def test_provider_modules_import_without_overture_core():
    code = (
        "import sys\n"
        "sys.modules['overture_core'] = None\n"
        "import overture_airflow_provider\n"
        "import overture_airflow_provider._setup\n"
        "import overture_airflow_provider.setup_info\n"
        "import overture_airflow_provider.spark_agnostic_helpers\n"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_missing_overture_core_points_at_extra(monkeypatch):
    monkeypatch.setitem(sys.modules, "overture_core", None)
    with pytest.raises(ModuleNotFoundError, match=r"airflow-provider-overture\[overture\]"):
        import_overture_core("overture_core.pypi")


def test_unrelated_missing_module_is_not_rewritten():
    with pytest.raises(ModuleNotFoundError) as exc_info:
        import_overture_core("not_a_real_module_xyz")
    assert "overture-core" not in str(exc_info.value)
