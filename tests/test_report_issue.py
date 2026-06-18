"""Tests for the pure report-issue backend and ReportIssueConfig validation.

Airflow-free: runs under the lightweight test venv.
"""

import json
from urllib.parse import parse_qs, urlsplit

import pytest

from overture_airflow_provider._report_issue import (
    DEFAULT_PROVIDER,
    GitHubIssueTracker,
    IssueContext,
    IssueTracker,
    available_providers,
    get_tracker,
    parse_report_issue_xcom,
    register_tracker,
)
from overture_airflow_provider.config import ReportIssueConfig


class TestRegistry:
    def test_github_registered_by_default(self):
        assert "github" in available_providers()
        assert DEFAULT_PROVIDER == "github"
        assert isinstance(get_tracker("github"), GitHubIssueTracker)

    def test_lookup_is_case_insensitive(self):
        assert get_tracker("GitHub") is get_tracker("github")

    def test_unknown_provider_returns_none(self):
        assert get_tracker("does-not-exist") is None

    def test_register_custom_tracker(self):
        class JiraIssueTracker(IssueTracker):
            name = "jira-test"

            def validate_target(self, target):
                if "/" not in target:
                    raise ValueError("need base/project")

            def build_url(self, target, ctx):
                base, project = target.split("/", 1)
                return f"https://{base}/secure/CreateIssue.jspa?pid={project}"

        register_tracker(JiraIssueTracker())
        assert "jira-test" in available_providers()
        url = get_tracker("jira-test").build_url("jira.example.com/PROJ", IssueContext())
        assert url == "https://jira.example.com/secure/CreateIssue.jspa?pid=PROJ"


class TestGitHubTracker:
    def _tracker(self):
        return GitHubIssueTracker()

    def test_validate_accepts_owner_repo(self):
        self._tracker().validate_target("OvertureMaps/overture-airflow-provider")

    @pytest.mark.parametrize(
        "bad", ["", "  ", "noslash", "too/many/slashes", "/leading", "trailing/"]
    )
    def test_validate_rejects_bad_targets(self, bad):
        with pytest.raises(ValueError):
            self._tracker().validate_target(bad)

    def test_build_url_blank_target(self):
        assert self._tracker().build_url("", IssueContext()) == ""

    def test_build_url_encodes_context(self):
        ctx = IssueContext(
            dag_id="my_dag",
            task_id="my_job.execute_spark_job",
            run_id="manual__2026",
            platform="GLUE_SEDONA",
            job_url="https://console.aws/glue/jr_1",
            labels=("bug", "spark"),
        )
        url = self._tracker().build_url("owner/repo", ctx)
        split = urlsplit(url)
        assert split.netloc == "github.com"
        assert split.path == "/owner/repo/issues/new"
        q = parse_qs(split.query)
        assert q["title"][0] == "[job failure] my_dag / my_job.execute_spark_job"
        assert "GLUE_SEDONA" in q["body"][0]
        assert "jr_1" in q["body"][0]
        assert "manual__2026" in q["body"][0]
        assert q["labels"][0] == "bug,spark"

    def test_build_url_omits_labels_when_empty(self):
        url = self._tracker().build_url("owner/repo", IssueContext(labels=("", "  ")))
        assert "labels=" not in url


class TestParseXcom:
    def test_none_and_empty(self):
        assert parse_report_issue_xcom(None) == {}
        assert parse_report_issue_xcom("") == {}

    def test_dict_passthrough(self):
        d = {"provider": "github", "target": "o/r"}
        assert parse_report_issue_xcom(d) is d

    def test_json_string(self):
        raw = json.dumps({"provider": "github", "target": "o/r"})
        assert parse_report_issue_xcom(raw) == {"provider": "github", "target": "o/r"}

    def test_malformed_json(self):
        assert parse_report_issue_xcom("not-json{") == {}

    def test_non_object_json(self):
        assert parse_report_issue_xcom("[1, 2]") == {}


class TestReportIssueConfig:
    def test_disabled_is_default_and_skips_validation(self):
        cfg = ReportIssueConfig()
        assert cfg.enabled is False
        assert cfg.active is False

    def test_enabled_requires_target(self):
        with pytest.raises(ValueError, match="target"):
            ReportIssueConfig(enabled=True)

    def test_enabled_rejects_bad_github_target(self):
        with pytest.raises(ValueError):
            ReportIssueConfig(enabled=True, target="noslash")

    def test_enabled_unknown_provider(self):
        with pytest.raises(ValueError, match="not registered"):
            ReportIssueConfig(enabled=True, provider="nope", target="x")

    def test_active_and_payload(self):
        cfg = ReportIssueConfig(
            enabled=True,
            target="owner/repo",
            labels=["bug"],
            extra={"k": "v"},
        )
        assert cfg.active is True
        assert cfg.to_operator_payload() == {
            "provider": "github",
            "target": "owner/repo",
            "labels": ["bug"],
            "extra": {"k": "v"},
        }

    def test_disabled_with_target_is_not_active(self):
        cfg = ReportIssueConfig(enabled=False, target="owner/repo")
        assert cfg.active is False
