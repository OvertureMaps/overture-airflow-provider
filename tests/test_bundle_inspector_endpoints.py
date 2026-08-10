"""Tests for the bundle inspector's framework-agnostic endpoint logic.

Airflow-free at the seams that matter: monkeypatches
``airflow.configuration.conf`` (imported into ``_endpoints`` by name) rather
than reading real ``airflow.cfg``, and stubs out the boto3 S3 client, so it
runs under the lightweight test venv.
"""

from datetime import UTC, datetime

import pytest

from overture_airflow_provider.plugins.bundle_inspector import _endpoints as ep


class _FakeConf:
    """Stand-in for ``airflow.configuration.conf``, keyed by ``(section, key)``."""

    def __init__(self, values: dict):
        self._values = values

    def get(self, section, key, fallback=None):
        return self._values.get((section, key), fallback)


def _conf(**bundle_inspector_options):
    return _FakeConf({("bundle_inspector", k): v for k, v in bundle_inspector_options.items()})


@pytest.fixture(autouse=True)
def _clear_s3_client_cache():
    # `_s3_client` is `lru_cache`d; drop any stale fake client between tests.
    ep._s3_client.cache_clear()
    yield
    ep._s3_client.cache_clear()


class TestGetConfig:
    def test_defaults_to_dev_with_no_namespace(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b"))
        cfg = ep.get_config()
        assert cfg == {
            "bucket": "b",
            "athena_output_bucket": None,
            "environment": "dev",
            "namespace": "",
            "namespace_prefix": "",
        }

    def test_missing_bucket_is_rejected(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf())
        with pytest.raises(ep.ApiError) as exc_info:
            ep.get_config()
        assert exc_info.value.status == 500

    def test_dev_reads_namespace_from_conf(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b", user="alice"))
        cfg = ep.get_config()
        assert cfg["namespace"] == "alice"
        assert cfg["namespace_prefix"] == "alice/"

    def test_user_override_wins_over_conf(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b", user="alice"))
        cfg = ep.get_config(user_override="bob")
        assert cfg["namespace"] == "bob"

    def test_non_dev_ignores_namespace(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b", environment="prod", user="alice"))
        cfg = ep.get_config(user_override="bob")
        assert cfg["namespace"] == ""
        assert cfg["namespace_prefix"] == ""


class TestListChildren:
    def test_root_returns_known_stages_without_s3_call(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b"))
        monkeypatch.setattr(
            ep,
            "_s3_client",
            lambda: (_ for _ in ()).throw(AssertionError("should not call S3 at root")),
        )
        result = ep.list_children("", user=None)
        assert result["depth"] == 0
        assert {c["name"] for c in result["children"]} == {
            "theme_promote/",
            "theme_stage/",
            "release_candidate/",
        }

    def test_non_root_lists_and_filters_via_s3(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b"))

        class _FakeClient:
            def get_paginator(self, name):
                class _P:
                    def paginate(self, **kwargs):
                        yield {
                            "CommonPrefixes": [{"Prefix": "theme_promote/theme=places/"}],
                            "Contents": [
                                {
                                    "Key": "theme_promote/success",
                                    "Size": 0,
                                    "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                                }
                            ],
                        }

                return _P()

        monkeypatch.setattr(ep, "_s3_client", lambda: _FakeClient())
        result = ep.list_children("theme_promote/", user=None)
        assert result["stage"] == "theme_promote"
        assert result["hierarchy"] == ["theme", "schema", "run"]
        names = {c["name"] for c in result["children"]}
        assert "theme=places/" in names


class TestS3Proxy:
    def test_missing_key_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep.s3proxy(key="", user=None, range_header=None, head_only=False)
        assert exc_info.value.status == 400

    def test_path_traversal_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep.s3proxy(key="../secret.parquet", user=None, range_header=None, head_only=False)
        assert exc_info.value.status == 400

    def test_absolute_path_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep.s3proxy(key="/etc/passwd.parquet", user=None, range_header=None, head_only=False)
        assert exc_info.value.status == 400

    def test_disallowed_suffix_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep.s3proxy(key="file.exe", user=None, range_header=None, head_only=False)
        assert exc_info.value.status == 400

    def test_allowed_suffix_reaches_s3(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b"))

        class _FakeClient:
            def head_object(self, **kwargs):
                return {"ContentType": "application/octet-stream", "ContentLength": 123}

        monkeypatch.setattr(ep, "_s3_client", lambda: _FakeClient())
        result = ep.s3proxy(key="a/b.parquet", user=None, range_header=None, head_only=True)
        assert result.status == 200
        assert result.headers["Content-Length"] == "123"


class TestAthenaQuery:
    def test_missing_sql_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep.athena_query(sql="", user=None)
        assert exc_info.value.status == 400

    def test_uses_configured_output_bucket(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b", athena_output_bucket="athena-out"))
        captured = {}

        def fake_athena_start(output_bucket, sql):
            captured["output_bucket"] = output_bucket
            return {"query_id": "abc"}

        monkeypatch.setattr(ep.s3, "athena_start", fake_athena_start)
        result = ep.athena_query(sql="select 1", user=None)
        assert result == {"query_id": "abc"}
        assert captured["output_bucket"] == "athena-out"

    def test_falls_back_to_default_output_bucket(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b"))
        captured = {}

        def fake_athena_start(output_bucket, sql):
            captured["output_bucket"] = output_bucket
            return {"query_id": "abc"}

        monkeypatch.setattr(ep.s3, "athena_start", fake_athena_start)
        ep.athena_query(sql="select 1", user=None)
        assert captured["output_bucket"] == "overture-bundle-inspector-athena-output-dev"

    def test_error_result_raises_api_error(self, monkeypatch):
        monkeypatch.setattr(ep, "conf", _conf(s3_bucket="b"))
        monkeypatch.setattr(ep.s3, "athena_start", lambda output_bucket, sql: {"error": "boom"})
        with pytest.raises(ep.ApiError) as exc_info:
            ep.athena_query(sql="select 1", user=None)
        assert exc_info.value.status == 400


class TestValidateQueryId:
    def test_missing_query_id_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep._validate_query_id("")
        assert exc_info.value.status == 400

    def test_malformed_query_id_is_rejected(self):
        with pytest.raises(ep.ApiError) as exc_info:
            ep._validate_query_id("not-a-uuid")
        assert exc_info.value.status == 400

    def test_valid_uuid_passes(self):
        ep._validate_query_id("12345678-1234-1234-1234-123456789abc")
