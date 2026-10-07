"""Tests for ``_failures``: classifier, formatter, and log-tail bounding."""

import pytest

from overture_airflow_provider._failures import (
    CANCELLED,
    DOWNSTREAM_JOB,
    FAILED,
    GLUE_CAPACITY_ERROR_RE,
    INTERNAL_ERROR,
    PLATFORM_CAPACITY,
    PLATFORM_DATABRICKS,
    PLATFORM_GLUE,
    PLATFORM_INFRA,
    PLATFORM_WHEROBOTS,
    RETRYABLE_CLASSIFICATIONS,
    SUBMIT_CONFIG,
    TIMEOUT,
    TRIGGER_POLLING,
    FailureInfo,
    apply_heuristics,
    bounded_tail,
    classify_failure,
    format_failure,
    is_retryable,
)


class TestClassifyFailure:
    def test_launched_run_is_downstream(self):
        assert classify_failure(run_launched=True) == DOWNSTREAM_JOB

    def test_unlaunched_run_is_submit_config(self):
        assert classify_failure(run_launched=False) == SUBMIT_CONFIG

    def test_trigger_failure_wins_over_launched(self):
        assert classify_failure(run_launched=True, is_trigger_failure=True) == TRIGGER_POLLING

    def test_trigger_failure_wins_over_unlaunched(self):
        assert classify_failure(run_launched=False, is_trigger_failure=True) == TRIGGER_POLLING

    def test_platform_internal_error_is_platform_infra(self):
        assert (
            classify_failure(run_launched=True, is_platform_internal_error=True) == PLATFORM_INFRA
        )

    def test_trigger_failure_wins_over_platform_internal_error(self):
        assert (
            classify_failure(
                run_launched=True,
                is_trigger_failure=True,
                is_platform_internal_error=True,
            )
            == TRIGGER_POLLING
        )

    def test_platform_internal_error_wins_over_unlaunched(self):
        assert (
            classify_failure(run_launched=False, is_platform_internal_error=True) == PLATFORM_INFRA
        )

    def test_capacity_never_started_is_platform_capacity(self):
        # The run was launched (a run id exists) but the platform rejected it
        # for capacity before any job code ran.
        assert (
            classify_failure(run_launched=True, is_capacity_never_started=True) == PLATFORM_CAPACITY
        )

    def test_capacity_never_started_wins_over_platform_internal_error(self):
        assert (
            classify_failure(
                run_launched=True,
                is_capacity_never_started=True,
                is_platform_internal_error=True,
            )
            == PLATFORM_CAPACITY
        )

    def test_trigger_failure_wins_over_capacity_never_started(self):
        assert (
            classify_failure(
                run_launched=True,
                is_trigger_failure=True,
                is_capacity_never_started=True,
            )
            == TRIGGER_POLLING
        )


class TestIsRetryable:
    @pytest.mark.parametrize("classification", [SUBMIT_CONFIG, PLATFORM_CAPACITY])
    def test_never_ran_classifications_are_retryable(self, classification):
        info = FailureInfo(platform="GLUE", job_ref="job", classification=classification)
        assert is_retryable(info) is True
        assert classification in RETRYABLE_CLASSIFICATIONS

    @pytest.mark.parametrize("classification", [DOWNSTREAM_JOB, TRIGGER_POLLING, PLATFORM_INFRA])
    def test_launched_classifications_are_not_retryable(self, classification):
        info = FailureInfo(platform="GLUE", job_ref="job", classification=classification)
        assert is_retryable(info) is False


class TestGlueCapacityErrorPattern:
    @pytest.mark.parametrize(
        "message",
        [
            "Exceeded maximum concurrent compute",
            "exceeded maximum concurrent compute",
            "Resource unavailable",
            "ResourceUnavailable",
            "Insufficient capacity in us-west-2",
        ],
    )
    def test_matches_glue_capacity_messages(self, message):
        assert GLUE_CAPACITY_ERROR_RE.search(message)

    @pytest.mark.parametrize(
        "message",
        [
            "Job run failed with exit code 1",
            "ConcurrentRunsExceededException",  # start_job_run API error, not a run failure
            "java.lang.OutOfMemoryError",
        ],
    )
    def test_ignores_other_messages(self, message):
        assert GLUE_CAPACITY_ERROR_RE.search(message) is None


class TestStateConstants:
    def test_canonical_state_values(self):
        assert FAILED == "FAILED"
        assert TIMEOUT == "TIMEOUT"
        assert CANCELLED == "CANCELLED"
        assert INTERNAL_ERROR == "INTERNAL_ERROR"


class TestApplyHeuristics:
    @pytest.mark.parametrize("empty", [(), (None,), ("", None, "   ")])
    def test_no_text_returns_none(self, empty):
        assert apply_heuristics(*empty) is None

    def test_unknown_text_returns_none(self):
        assert apply_heuristics("some totally novel error") is None

    @pytest.mark.parametrize(
        "text,fragment",
        [
            ("S3 AccessDenied on DeleteObject", "IAM:"),
            ("User is not authorized to perform: glue:GetJob", "IAM:"),
            ("CLUSTER_NOT_FOUND: cluster 0123 missing", "Cluster config:"),
            ("INVALID_PARAMETER_VALUE: node_type_id", "Config:"),
            ("com.amazonaws...NoSuchBucket", "S3:"),
            ("java.lang.OutOfMemoryError: Java heap space", "OOM:"),
            ("ThrottlingException: Rate exceeded", "Throttling:"),
            ("Exceeded maximum concurrent compute", "Glue capacity:"),
            ("ResourceDoesNotExist: job 42", "Not found:"),
            ("Unauthenticated: invalid access token", "Auth:"),
            ("PermissionDenied: cannot attach", "Permissions:"),
        ],
    )
    def test_known_patterns_map_to_hint(self, text, fragment):
        assert apply_heuristics(text).startswith(fragment)

    def test_searches_across_multiple_texts(self):
        assert apply_heuristics("FAILED", None, "...AccessDenied...").startswith("IAM:")

    def test_first_match_wins_specific_before_generic(self):
        # AccessDenied (earlier) wins over a co-occurring not-found phrase.
        text = "AccessDenied; also EntityNotFound"
        assert apply_heuristics(text).startswith("IAM:")


class TestHeuristicPlatformScoping:
    def test_databricks_only_pattern_skipped_for_glue(self):
        assert apply_heuristics("CLUSTER_NOT_FOUND", platform=PLATFORM_GLUE) is None

    def test_databricks_only_pattern_hits_for_databricks(self):
        assert apply_heuristics("CLUSTER_NOT_FOUND", platform=PLATFORM_DATABRICKS).startswith(
            "Cluster config:"
        )

    def test_glue_only_pattern_skipped_for_databricks(self):
        assert apply_heuristics("EntityNotFound", platform=PLATFORM_DATABRICKS) is None

    def test_glue_only_pattern_hits_for_glue(self):
        assert apply_heuristics("EntityNotFound", platform=PLATFORM_GLUE).startswith("Not found:")

    def test_glue_capacity_hint_is_glue_scoped(self):
        text = "Exceeded maximum concurrent compute"
        hint = apply_heuristics(text, platform=PLATFORM_GLUE)
        assert hint.startswith("Glue capacity:")
        assert "safe to retry" in hint
        assert apply_heuristics(text, platform=PLATFORM_DATABRICKS) is None
        assert apply_heuristics(text, platform=PLATFORM_WHEROBOTS) is None

    def test_unscoped_pattern_applies_to_any_platform(self):
        for platform in (PLATFORM_GLUE, PLATFORM_DATABRICKS, PLATFORM_WHEROBOTS):
            assert apply_heuristics("java.lang.OutOfMemoryError", platform=platform).startswith(
                "OOM:"
            )

    def test_no_platform_considers_all_heuristics(self):
        # Without a platform filter, a databricks-scoped pattern still matches.
        assert apply_heuristics("INVALID_PARAMETER_VALUE").startswith("Config:")


class TestFailureInfoDefaults:
    def test_minimal_construction_defaults(self):
        info = FailureInfo(platform="GLUE", job_ref="metrics_job")
        assert info.run_id is None
        assert info.state == "FAILED"
        assert info.console_url is None
        assert info.reason is None
        assert info.root_cause is None
        assert info.classification == DOWNSTREAM_JOB


class TestBoundedTail:
    @pytest.mark.parametrize("empty", [None, "", "   "])
    def test_empty_returns_none(self, empty):
        assert bounded_tail(empty) is None

    def test_short_text_returned_stripped_untruncated(self):
        assert bounded_tail("  boom  ") == "boom"

    def test_long_text_is_tail_truncated_with_marker(self):
        text = "x" * 50 + "ACTUAL_ERROR_AT_END"
        out = bounded_tail(text, max_chars=10)
        assert out.startswith("...(truncated, showing last 10 chars)\n")
        assert out.endswith("R_AT_END")
        assert len(out.splitlines()[-1]) == 10


class TestFormatFailure:
    def test_full_message(self):
        info = FailureInfo(
            platform="DATABRICKS",
            job_ref="metrics_job",
            run_id="run-123",
            state="FAILED",
            console_url="https://example/run/123",
            reason="state_message here",
            root_cause="AccessDenied on DeleteObject",
            classification=DOWNSTREAM_JOB,
            hint="IAM: check the role policy.",
        )
        out = format_failure(info)
        assert out.splitlines()[0] == (
            "Spark job FAILED on DATABRICKS (downstream job error, not a provider/submit fault)."
        )
        assert "  run:     run-123      state: FAILED" in out
        assert "  reason:  state_message here" in out
        assert "  cause:   AccessDenied on DeleteObject" in out
        assert "  hint:    IAM: check the role policy." in out
        assert "  console: https://example/run/123" in out

    def test_optional_lines_omitted_when_absent(self):
        info = FailureInfo(
            platform="GLUE",
            job_ref="metrics_job",
            run_id="jr_1",
            classification=SUBMIT_CONFIG,
        )
        out = format_failure(info)
        assert "reason:" not in out
        assert "cause:" not in out
        assert "hint:" not in out
        assert "console:" not in out
        assert "run:     jr_1" in out

    def test_unknown_run_id_renders_placeholder(self):
        info = FailureInfo(platform="GLUE", job_ref="metrics_job")
        assert "run:     <unknown>" in format_failure(info)

    @pytest.mark.parametrize(
        "classification,fragment",
        [
            (DOWNSTREAM_JOB, "downstream job error"),
            (SUBMIT_CONFIG, "submit/config failure"),
            (TRIGGER_POLLING, "see Triggerer logs"),
            (PLATFORM_INFRA, "platform/infra fault"),
            (PLATFORM_CAPACITY, "the run never started, safe to retry"),
        ],
    )
    def test_header_reflects_classification(self, classification, fragment):
        info = FailureInfo(platform="WHEROBOTS", job_ref="job", classification=classification)
        assert fragment in format_failure(info).splitlines()[0]
