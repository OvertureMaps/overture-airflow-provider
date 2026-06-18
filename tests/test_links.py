"""Tests for links.py fallback / error paths."""

from unittest.mock import MagicMock, patch

from overture_airflow_provider.links import ReportIssueLink, _read_xcom


def test_read_xcom_uses_dttm_when_no_ti_key():
    with patch("overture_airflow_provider.links.XCom") as MockXCom:
        MockXCom.get_one.return_value = "val"
        result = _read_xcom("k", MagicMock(dag_id="d", task_id="t"), None, "2024-01-01")
    MockXCom.get_one.assert_called_once_with(
        key="k", dag_id="d", task_id="t", execution_date="2024-01-01"
    )
    assert result == "val"


def test_report_issue_link_tracker_build_url_raises_returns_empty():
    link = ReportIssueLink()
    with (
        patch(
            "overture_airflow_provider.links.parse_report_issue_xcom",
            return_value={"target": "owner/repo", "provider": "github"},
        ),
        patch("overture_airflow_provider.links.get_tracker") as mock_get_tracker,
        patch("overture_airflow_provider.links._read_xcom", return_value={}),
        patch.object(link, "_spark_context", return_value=("GLUE", "https://console/run/1")),
    ):
        mock_get_tracker.return_value.build_url.side_effect = RuntimeError("network error")
        result = link.get_link(MagicMock(), ti_key=MagicMock())
    assert result == ""


def test_spark_context_bad_json_returns_empty():
    link = ReportIssueLink()
    with patch("overture_airflow_provider.links._read_xcom", return_value="{bad json"):
        platform, url = link._spark_context(MagicMock(), MagicMock(), None)
    assert platform == "" and url == ""


def test_spark_context_non_dict_returns_empty():
    link = ReportIssueLink()
    # valid JSON but not a dict — triggers the isinstance guard
    with patch("overture_airflow_provider.links._read_xcom", return_value='"just-a-string"'):
        platform, url = link._spark_context(MagicMock(), MagicMock(), None)
    assert platform == "" and url == ""
