"""Pure, pluggable backend for the opt-in "Report Issue" operator link.

Deliberately Airflow- and SDK-free (stdlib only) so URL construction can be
unit-tested without an Airflow install, mirroring ``_failures.py``. The Airflow
glue (the ``BaseOperatorLink``) lives in ``links.py``.

Extensibility
-------------
Issue trackers are strategies behind :class:`IssueTracker`. GitHub ships built
in; adding another tracker (e.g. Jira) is a subclass plus a ``register_tracker``
call — no changes to the link, operator, or config wiring::

    class JiraIssueTracker(IssueTracker):
        name = "jira"

        def validate_target(self, target: str) -> None:
            ...  # raise ValueError on a bad project key / base URL

        def build_url(self, target: str, ctx: IssueContext) -> str:
            ...  # return a pre-filled "create issue" URL

    register_tracker(JiraIssueTracker())

``extra`` on the config/context carries tracker-specific knobs (Jira base URL,
issue type, …) without widening the shared surface.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from urllib.parse import quote, urlencode

#: XCom key the operator writes (early, so it survives a failed run) carrying
#: the report-issue config the link needs to build a target URL.
REPORT_ISSUE_XCOM_KEY = "report_issue"

#: Built-in tracker name; the default when a config omits ``provider``.
DEFAULT_PROVIDER = "github"


@dataclass
class IssueContext:
    """Run context a tracker turns into a pre-filled "create issue" URL."""

    dag_id: str = ""
    task_id: str = ""
    run_id: str = ""
    platform: str = ""
    job_url: str = ""
    labels: tuple[str, ...] = ()
    extra: dict = field(default_factory=dict)

    def title(self) -> str:
        return f"[job failure] {self.dag_id} / {self.task_id}".strip(" /")

    def body_lines(self) -> list[str]:
        lines = [
            "Reporting a Spark job failure surfaced by overture-airflow-provider.",
            "",
            "### Run context",
            f"- DAG: `{self.dag_id}`",
            f"- Task: `{self.task_id}`",
        ]
        if self.run_id:
            lines.append(f"- Run: `{self.run_id}`")
        if self.platform:
            lines.append(f"- Platform: `{self.platform}`")
        if self.job_url:
            lines.append(f"- Job console: {self.job_url}")
        lines += [
            "",
            "### What happened",
            "_Paste the classified failure block from the task log._",
        ]
        return lines

    def clean_labels(self) -> list[str]:
        return [str(label).strip() for label in self.labels if str(label).strip()]


class IssueTracker(ABC):
    """Strategy that validates a target and builds a "create issue" URL."""

    #: Stable provider key used in config and the registry.
    name: str = ""

    @abstractmethod
    def validate_target(self, target: str) -> None:
        """Raise ``ValueError`` when ``target`` is missing or malformed."""

    @abstractmethod
    def build_url(self, target: str, ctx: IssueContext) -> str:
        """Return a pre-filled URL, or ``""`` when ``target`` is blank."""


class GitHubIssueTracker(IssueTracker):
    """GitHub issues. ``target`` is an ``"owner/repo"`` slug."""

    name = "github"

    def validate_target(self, target: str) -> None:
        repo = (target or "").strip()
        if not repo:
            raise ValueError('github target repository must be set ("owner/repo")')
        if repo.count("/") != 1 or repo.startswith("/") or repo.endswith("/"):
            raise ValueError('github target must be in "owner/repo" form')

    def build_url(self, target: str, ctx: IssueContext) -> str:
        repo = (target or "").strip().strip("/")
        if not repo:
            return ""
        params = {"title": ctx.title(), "body": "\n".join(ctx.body_lines())}
        labels = ctx.clean_labels()
        if labels:
            params["labels"] = ",".join(labels)
        return f"https://github.com/{repo}/issues/new?{urlencode(params, quote_via=quote)}"


_REGISTRY: dict[str, IssueTracker] = {}


def register_tracker(tracker: IssueTracker) -> None:
    """Register (or replace) a tracker by its ``name``."""
    if not tracker.name:
        raise ValueError("IssueTracker.name must be set")
    _REGISTRY[tracker.name.strip().lower()] = tracker


def get_tracker(name: str) -> IssueTracker | None:
    """Return the registered tracker for ``name`` (or ``None``)."""
    return _REGISTRY.get((name or "").strip().lower())


def parse_report_issue_xcom(raw) -> dict:
    """Coerce a raw XCom value into a config dict, tolerating bad input.

    Airflow 2.x hands back the JSON string the operator pushed; Airflow 3.x may
    deserialize it to a dict directly. Anything unparseable yields ``{}`` so the
    link renders nothing rather than erroring in the web server.
    """
    if not raw:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}
    return value if isinstance(value, dict) else {}


def available_providers() -> list[str]:
    """Sorted list of registered provider names."""
    return sorted(_REGISTRY)


register_tracker(GitHubIssueTracker())
