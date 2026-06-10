"""Version metadata stays in sync with the installed package."""

from importlib.metadata import version

import overture_airflow_provider
from overture_airflow_provider.provider_info import get_provider_info


def test_version_matches_installed_metadata():
    assert overture_airflow_provider.__version__ == version("airflow-provider-overture")


def test_provider_info_versions_match_package_version():
    info = get_provider_info()
    assert info["versions"] == [overture_airflow_provider.__version__]
