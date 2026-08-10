"""Tests for the bundle inspector's S3 listing/filtering helpers.

Airflow-free and boto3-free: exercises pure logic and a fake paginator-based
S3 client, so it runs under the lightweight test venv.
"""

from datetime import UTC, datetime

import pytest

from overture_airflow_provider.plugins.bundle_inspector import s3


class _FakePaginator:
    """Fake boto3 paginator that replays canned pages regardless of kwargs."""

    def __init__(self, pages):
        self._pages = pages

    def paginate(self, **kwargs):
        yield from self._pages


class _FakeClient:
    """Fake boto3 S3 client backed by canned `list_objects_v2` pages."""

    def __init__(self, pages):
        self._pages = pages

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return _FakePaginator(self._pages)


class TestDepthAndHierarchy:
    def test_get_depth_root(self):
        assert s3.get_depth("") == 0

    def test_get_depth_nested(self):
        assert s3.get_depth("theme_promote/theme=places/") == 2

    def test_hierarchy_for_known_stage(self):
        assert s3.hierarchy_for_stage("theme_promote") == ["theme", "schema", "run"]

    def test_hierarchy_falls_back_to_default(self):
        assert s3.hierarchy_for_stage("unknown_stage") == s3.DEFAULT_HIERARCHY

    def test_run_depth_for_stage(self):
        # theme_promote: ["theme", "schema", "run"] -> run at index 2 -> depth 4
        assert s3.run_depth_for_stage("theme_promote") == 4

    def test_run_depth_falls_back_when_no_run_level(self):
        s3.STAGE_HIERARCHY["_no_run"] = ["theme"]
        try:
            assert s3.run_depth_for_stage("_no_run") == 2
        finally:
            del s3.STAGE_HIERARCHY["_no_run"]


class TestFilterBundleLevel:
    def test_filters_hive_partition_level(self):
        items = [
            {"name": "theme=places/", "type": "directory"},
            {"name": "not-hive/", "type": "directory"},
        ]
        filtered = s3.filter_bundle_level(items, "theme_promote", 0)
        assert filtered == [items[0]]

    def test_component_level_applies_allowlist(self):
        items = [
            {"name": "data/", "type": "directory"},
            {"name": "junk/", "type": "directory"},
            {"name": "success", "type": "file"},
        ]
        filtered = s3.filter_bundle_level(items, "theme_stage", 2)
        names = {item["name"] for item in filtered}
        assert names == {"data/", "success"}

    def test_negative_depth_passes_through(self):
        items = [{"name": "anything/", "type": "directory"}]
        assert s3.filter_bundle_level(items, "theme_promote", -1) == items

    def test_no_allowlist_passes_through_at_leaf(self):
        items = [{"name": "whatever/", "type": "directory"}]
        # theme_promote has no STAGE_COMPONENTS entry, so leaf level passes through
        assert s3.filter_bundle_level(items, "theme_promote", 3) == items


class TestListPrefix:
    def test_splits_directories_and_files(self):
        pages = [
            {
                "CommonPrefixes": [{"Prefix": "ns/theme_promote/theme=places/"}],
                "Contents": [
                    {
                        "Key": "ns/theme_promote/metadata.json",
                        "Size": 42,
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                    },
                    # Exact-prefix match (no name) must be skipped.
                    {
                        "Key": "ns/theme_promote/",
                        "Size": 0,
                        "LastModified": datetime(2024, 1, 1, tzinfo=UTC),
                    },
                ],
            }
        ]
        client = _FakeClient(pages)
        items = s3.list_prefix(client, "bucket", "ns/theme_promote/")
        assert items == [
            {"name": "theme=places/", "type": "directory"},
            {
                "name": "metadata.json",
                "type": "file",
                "size": 42,
                "last_modified": "2024-01-01T00:00:00+00:00",
            },
        ]


class TestListUsers:
    def test_lists_top_level_prefixes(self):
        pages = [{"CommonPrefixes": [{"Prefix": "alice/"}, {"Prefix": "bob/"}]}]
        client = _FakeClient(pages)
        assert s3.list_users(client, "bucket") == ["alice", "bob"]


class TestCountObjects:
    def test_sums_key_count_across_pages(self):
        pages = [{"KeyCount": 3}, {"KeyCount": 2}]
        client = _FakeClient(pages)
        assert s3.count_objects(client, "bucket", "ns/") == 5


class TestListTree:
    def test_builds_nested_tree_and_truncates_files(self):
        pages = [
            {
                "Contents": [
                    {"Key": "ns/a/1.parquet"},
                    {"Key": "ns/a/2.parquet"},
                    {"Key": "ns/a/3.parquet"},
                    {"Key": "ns/a/4.parquet"},
                    {"Key": "ns/b/only.json"},
                ]
            }
        ]
        client = _FakeClient(pages)
        tree = s3.list_tree(client, "bucket", "ns/", max_files_per_dir=2)
        assert tree["dirs"]["a"]["files"] == ["1.parquet", "2.parquet"]
        assert tree["dirs"]["a"]["_omitted"] == 2
        assert tree["dirs"]["b"]["files"] == ["only.json"]
        assert "_omitted" not in tree["dirs"]["b"]
        assert tree["_total_keys"] == 5
        assert tree["_truncated"] is False


class TestFindAndPresignFile:
    def test_find_file_key_matches_suffix(self):
        pages = [{"Contents": [{"Key": "ns/a.txt"}, {"Key": "ns/b.parquet"}]}]
        client = _FakeClient(pages)
        assert s3.find_file_key(client, "bucket", "ns/", ".parquet") == "ns/b.parquet"

    def test_find_file_key_no_match_returns_none(self):
        client = _FakeClient([{"Contents": [{"Key": "ns/a.txt"}]}])
        assert s3.find_file_key(client, "bucket", "ns/", ".parquet") is None

    def test_presign_file_returns_none_when_no_match(self):
        client = _FakeClient([{"Contents": []}])
        assert s3.presign_file(client, "bucket", "ns/", ".parquet") is None


class TestParseS3Uri:
    def test_parses_bucket_and_key(self):
        assert s3._parse_s3_uri("s3://my-bucket/some/key.csv") == ("my-bucket", "some/key.csv")

    def test_rejects_non_s3_uri(self):
        with pytest.raises(ValueError):
            s3._parse_s3_uri("https://example.com/key.csv")

    def test_rejects_missing_key(self):
        with pytest.raises(ValueError):
            s3._parse_s3_uri("s3://my-bucket")
