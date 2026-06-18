"""Tests for setup_info.to_xcom / rehydrate — the XCom contract."""

import json
from unittest.mock import MagicMock, patch

from overture_airflow_provider.setup_info import SERIALIZABLE_KEYS, rehydrate, to_xcom
from overture_airflow_provider.spark import SparkFamily, SparkImpl

_FULL = {
    "spark_family": SparkFamily.GLUE,
    "spark_impl": SparkImpl.GLUE_v5,
    "py_pi_client": MagicMock(),
    "spark_impl_name": "GLUE_v5",
    "job_name": "my_job",
    "codeartifact_domain_owner": "123456789012",
    "codeartifact_domain": "my-domain",
    "codeartifact_repository": "my-repo",
    "codeartifact_region": "us-east-1",
    **{
        k: ""
        for k in SERIALIZABLE_KEYS
        if k
        not in (
            "spark_impl_name",
            "job_name",
            "codeartifact_domain_owner",
            "codeartifact_domain",
            "codeartifact_repository",
            "codeartifact_region",
        )
    },
}


def test_to_xcom_strips_non_serializable():
    result = to_xcom(_FULL)
    assert "spark_family" not in result
    assert "spark_impl" not in result
    assert "py_pi_client" not in result


def test_to_xcom_adds_spark_family_name():
    result = to_xcom(_FULL)
    assert result["spark_family_name"] == "GLUE"


def test_to_xcom_keeps_serializable_keys():
    result = to_xcom(_FULL)
    assert result["job_name"] == "my_job"
    assert result["spark_impl_name"] == "GLUE_v5"


def test_to_xcom_is_json_safe():
    result = to_xcom(_FULL)
    assert json.loads(json.dumps(result)) == result


def test_rehydrate_reconstructs_enums():
    serialized = {
        "spark_impl_name": "GLUE_v5",
        "spark_family_name": "GLUE",
        "codeartifact_domain_owner": "123",
        "codeartifact_domain": "dom",
        "codeartifact_repository": "repo",
        "codeartifact_region": "us-east-1",
    }
    with patch("overture_airflow_provider.python_package_utils.CodeArtifactPyPiClient"):
        result = rehydrate(serialized)
    assert result["spark_impl"] == SparkImpl.GLUE_v5
    assert result["spark_family"] == SparkFamily.GLUE


def test_rehydrate_constructs_pypi_client():
    serialized = {
        "spark_impl_name": "GLUE_v5",
        "spark_family_name": "GLUE",
        "codeartifact_domain_owner": "123",
        "codeartifact_domain": "dom",
        "codeartifact_repository": "repo",
        "codeartifact_region": "us-east-1",
    }
    with patch(
        "overture_airflow_provider.python_package_utils.CodeArtifactPyPiClient"
    ) as MockClient:
        result = rehydrate(serialized)
    assert result["py_pi_client"] is MockClient.return_value
    MockClient.assert_called_once_with(
        domain_owner="123", domain="dom", repository="repo", region_name="us-east-1"
    )


def test_rehydrate_preserves_extra_keys():
    serialized = {
        "spark_impl_name": "GLUE_v5",
        "spark_family_name": "GLUE",
        "codeartifact_domain_owner": "x",
        "codeartifact_domain": "x",
        "codeartifact_repository": "x",
        "codeartifact_region": "x",
        "custom_field": "preserved",
    }
    with patch("overture_airflow_provider.python_package_utils.CodeArtifactPyPiClient"):
        result = rehydrate(serialized)
    assert result["custom_field"] == "preserved"


def test_roundtrip_restores_key_values():
    with patch("overture_airflow_provider.python_package_utils.CodeArtifactPyPiClient"):
        restored = rehydrate(to_xcom(_FULL))
    assert restored["spark_family"] == SparkFamily.GLUE
    assert restored["spark_impl"] == SparkImpl.GLUE_v5
    assert restored["job_name"] == "my_job"
