"""Uniform platform job-failure enrichment, classification, and formatting.

Platform job failures (Glue / Wherobots / Databricks) surface as bare, noisy
Airflow exceptions whose real root cause is buried in platform logs. This module
gives the provider a small, platform-agnostic vocabulary for reporting failures
the same way everywhere:

- ``FailureInfo``: a structured snapshot of a failed run.
- ``classify_failure``: decides who is most likely at fault.
- ``apply_heuristics``: maps known error patterns to actionable hints.
- ``format_failure``: renders a concise, consistent failure message.
- ``bounded_tail``: trims a verbose log/trace down to a readable tail.

The classifier never inspects platform internals; it works purely from signals
the orchestration layer already has (whether the job actually launched, and
whether the failure came from the deferral/trigger machinery). Per-platform
root-cause extraction lives elsewhere (a ``describe_failure`` seam on each
handler), so this module stays free of Airflow and platform SDK imports.
"""

from __future__ import annotations

import re
from dataclasses import dataclass

#: The job launched, so the failure is in the downstream Spark job itself, not
#: the provider/submit path.
DOWNSTREAM_JOB = "downstream-job"
#: The job never launched: a submission or configuration fault (often a provider
#: bug or bad caller config).
SUBMIT_CONFIG = "submit/config"
#: The deferral/trigger machinery failed: a polling/Triggerer fault, diagnosed
#: from the Triggerer logs rather than the task logs.
TRIGGER_POLLING = "trigger/polling"
#: The platform itself errored out (e.g. Databricks ``INTERNAL_ERROR`` life-cycle
#: state): infrastructure-side, not the provider and not the downstream job.
PLATFORM_INFRA = "platform/infra"

#: One-line explanation rendered next to the failure header per classification.
_CLASSIFICATION_HEADERS = {
    DOWNSTREAM_JOB: "downstream job error, not a provider/submit fault",
    SUBMIT_CONFIG: "submit/config failure, likely a provider or configuration fault",
    TRIGGER_POLLING: "trigger/polling failure, see Triggerer logs (not task logs)",
    PLATFORM_INFRA: "platform/infra fault, not a provider or downstream job error",
}

# Canonical terminal run states. Handlers normalise platform-native states
# (Glue ``JobRunState``, Databricks ``result_state``/``life_cycle_state``,
# Wherobots run status) onto these so messages read the same everywhere.
FAILED = "FAILED"
TIMEOUT = "TIMEOUT"
CANCELLED = "CANCELLED"
#: Databricks ``life_cycle_state == "INTERNAL_ERROR"``: platform-side failure.
INTERNAL_ERROR = "INTERNAL_ERROR"

# Platform names (match ``FailureInfo.platform``). Used to scope heuristics to
# the platform(s) where a given error pattern can actually occur.
PLATFORM_GLUE = "GLUE"
PLATFORM_DATABRICKS = "DATABRICKS"
PLATFORM_WHEROBOTS = "WHEROBOTS"

#: Default number of trailing characters to keep from a root-cause/log tail.
_DEFAULT_ROOT_CAUSE_CHARS = 2000


@dataclass
class FailureInfo:
    """Structured snapshot of a failed platform run.

    ``platform`` is the upper-cased family name (``GLUE`` / ``WHEROBOTS`` /
    ``DATABRICKS``). ``job_ref`` identifies the job (Glue job name, Databricks
    job/run name, Wherobots run name). ``classification`` is one of the module
    constants and drives the header in :func:`format_failure`.
    """

    platform: str
    job_ref: str
    run_id: str | None = None
    state: str = "FAILED"
    console_url: str | None = None
    reason: str | None = None
    root_cause: str | None = None
    classification: str = DOWNSTREAM_JOB
    hint: str | None = None


# Ordered heuristics. First match wins, searched case-insensitively against the
# combined reason + root-cause text. Each entry is scoped to the platform(s)
# where the pattern can actually occur; an empty ``platforms`` set means it
# applies to all platforms (e.g. generic Spark/S3 errors). Patterns are
# deliberately specific to avoid mislabelling; order specific before generic.
@dataclass(frozen=True)
class _Heuristic:
    pattern: re.Pattern[str]
    hint: str
    platforms: frozenset[str] = frozenset()  # empty => all platforms


_HEURISTICS: list[_Heuristic] = [
    _Heuristic(
        re.compile(r"access\s*denied|is not authorized to perform|accessdenied", re.I),
        "IAM: the job role is missing a permission. Check its policy for the denied action/resource.",
        # AWS S3/IAM denials can surface on any platform that touches S3.
        frozenset(),
    ),
    _Heuristic(
        re.compile(r"unauthenticated|invalid access token|expired token|\b401\b", re.I),
        "Auth: credentials were rejected. Check the Airflow connection token/secret.",
        frozenset({PLATFORM_DATABRICKS, PLATFORM_WHEROBOTS}),
    ),
    _Heuristic(
        re.compile(r"permission denied|permissiondenied", re.I),
        "Permissions: the principal lacks access. Check workspace/job permissions.",
        frozenset({PLATFORM_DATABRICKS}),
    ),
    _Heuristic(
        re.compile(r"cluster_not_found|cluster .{0,40}does not exist", re.I),
        "Cluster config: the referenced cluster doesn't exist. Check the cluster id/policy.",
        frozenset({PLATFORM_DATABRICKS}),
    ),
    _Heuristic(
        re.compile(r"invalid_parameter_value|invalidparametervalue", re.I),
        "Config: an API parameter is invalid. Check the cluster/job spec.",
        frozenset({PLATFORM_DATABRICKS}),
    ),
    _Heuristic(
        re.compile(r"nosuchbucket|specified bucket does not exist", re.I),
        "S3: the bucket is missing. Check the artifact/asset S3 paths.",
        frozenset(),
    ),
    _Heuristic(
        re.compile(r"out\s*of\s*memory|outofmemoryerror|java heap space|gc overhead limit", re.I),
        "OOM: increase worker size/cores or reduce partition size.",
        frozenset(),
    ),
    _Heuristic(
        re.compile(r"throttlingexception|rate exceeded|toomanyrequests", re.I),
        "Throttling: an upstream API rate-limited the request. Add retries/backoff.",
        frozenset(),
    ),
    _Heuristic(
        re.compile(r"entitynotfound", re.I),
        "Not found: a referenced Glue job/resource is missing. Check job_name and region.",
        frozenset({PLATFORM_GLUE}),
    ),
    _Heuristic(
        re.compile(r"resourcedoesnotexist|resource_does_not_exist", re.I),
        "Not found: a referenced Databricks resource is missing. Check ids and paths.",
        frozenset({PLATFORM_DATABRICKS}),
    ),
]


def apply_heuristics(*texts: str | None, platform: str | None = None) -> str | None:
    """Return an actionable hint for the first known error pattern found.

    Searches the concatenation of all non-empty ``texts`` (e.g. reason and
    root-cause). When ``platform`` is given, platform-scoped heuristics that
    don't apply to it are skipped (unscoped heuristics always apply). Returns
    ``None`` when nothing matches.
    """
    blob = "\n".join(t for t in texts if t)
    if not blob:
        return None
    for h in _HEURISTICS:
        if platform is not None and h.platforms and platform not in h.platforms:
            continue
        if h.pattern.search(blob):
            return h.hint
    return None


def classify_failure(
    *,
    run_launched: bool,
    is_trigger_failure: bool = False,
    is_platform_internal_error: bool = False,
) -> str:
    """Classify a failure from signals the orchestration layer already holds.

    Precedence (first match wins):

    - ``is_trigger_failure`` (a deferral/polling crash): ``trigger/polling``.
    - ``is_platform_internal_error`` (e.g. Databricks ``INTERNAL_ERROR``):
      ``platform/infra``.
    - the run launched (early run-id XCom present): ``downstream-job``.
    - otherwise the job never launched: ``submit/config``.
    """
    if is_trigger_failure:
        return TRIGGER_POLLING
    if is_platform_internal_error:
        return PLATFORM_INFRA
    if run_launched:
        return DOWNSTREAM_JOB
    return SUBMIT_CONFIG


def bounded_tail(text: str | None, max_chars: int = _DEFAULT_ROOT_CAUSE_CHARS) -> str | None:
    """Return the trailing ``max_chars`` of ``text`` (the actionable end of a log).

    Returns ``None`` for empty input. When truncation happens, the result is
    prefixed with an explicit marker so the reader knows earlier output was
    dropped. The real error in a Spark stack trace is almost always at the tail.
    """
    if not text:
        return None
    text = text.strip()
    if not text:
        return None
    if len(text) <= max_chars:
        return text
    return f"...(truncated, showing last {max_chars} chars)\n{text[-max_chars:]}"


def format_failure(info: FailureInfo) -> str:
    """Render ``info`` as a concise, uniform multi-line failure message."""
    header = _CLASSIFICATION_HEADERS.get(info.classification, info.classification)
    lines = [
        f"Spark job {info.state} on {info.platform} ({header}).",
        f"  run:     {info.run_id or '<unknown>'}      state: {info.state}",
    ]
    if info.reason:
        lines.append(f"  reason:  {info.reason}")
    if info.root_cause:
        lines.append(f"  cause:   {info.root_cause}")
    if info.hint:
        lines.append(f"  hint:    {info.hint}")
    if info.console_url:
        lines.append(f"  console: {info.console_url}")
    return "\n".join(lines)
