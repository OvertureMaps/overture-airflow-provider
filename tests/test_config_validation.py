"""Tests for fail-fast validation of required config dataclass fields.

Required fields must reject empty/whitespace-only values at construction time
with a clear, field-named ``ValueError`` rather than failing later with a
cryptic S3/CodeArtifact error. Optional fields and the internal ``unset()``
disabled placeholders must still construct fine.
"""

import dataclasses

import pytest

from overture_airflow_provider.config import (
    ArtifactStoreConfig,
    PackageRegistryConfig,
)


class TestPackageRegistryConfig:
    def test_valid_constructs(self):
        cfg = PackageRegistryConfig(
            domain_owner="123456789012",
            domain="my-pypi",
            repository="my-repo",
            region="us-east-1",
        )
        assert cfg.domain == "my-pypi"

    def test_defaults_region_and_optional_maven(self):
        cfg = PackageRegistryConfig(
            domain_owner="123456789012", domain="my-pypi", repository="my-repo"
        )
        assert cfg.region == "us-east-1"
        assert cfg.maven_repository == ""
        assert cfg.maven_repository_path == ""

    @pytest.mark.parametrize("field_name", ["domain_owner", "domain", "repository", "region"])
    def test_empty_required_field_raises(self, field_name):
        kwargs = {
            "domain_owner": "123456789012",
            "domain": "my-pypi",
            "repository": "my-repo",
            "region": "us-east-1",
            field_name: "",
        }
        with pytest.raises(
            ValueError, match=f"PackageRegistryConfig.{field_name} must not be empty"
        ):
            PackageRegistryConfig(**kwargs)

    def test_whitespace_only_is_rejected(self):
        with pytest.raises(ValueError, match="PackageRegistryConfig.domain must not be empty"):
            PackageRegistryConfig(domain_owner="o", domain="   ", repository="r")

    def test_unset_placeholder_skips_validation(self):
        cfg = PackageRegistryConfig.unset()
        assert cfg.domain_owner == ""
        assert cfg.domain == ""
        assert cfg.repository == ""

    def test_validate_initvar_not_a_stored_field(self):
        names = {f.name for f in dataclasses.fields(PackageRegistryConfig)}
        assert "_validate" not in names


class TestArtifactStoreConfig:
    def test_valid_constructs(self):
        cfg = ArtifactStoreConfig(s3_bucket="my-bucket")
        assert cfg.s3_bucket == "my-bucket"
        assert cfg.s3_root == "spark-agnostic-operator"

    def test_empty_s3_bucket_raises(self):
        with pytest.raises(ValueError, match="ArtifactStoreConfig.s3_bucket must not be empty"):
            ArtifactStoreConfig(s3_bucket="")

    def test_whitespace_only_is_rejected(self):
        with pytest.raises(ValueError, match="ArtifactStoreConfig.s3_bucket must not be empty"):
            ArtifactStoreConfig(s3_bucket="   ")

    def test_unset_placeholder_skips_validation(self):
        cfg = ArtifactStoreConfig.unset()
        assert cfg.s3_bucket == ""
        assert cfg.s3_root == "spark-agnostic-operator"

    def test_validate_initvar_not_a_stored_field(self):
        names = {f.name for f in dataclasses.fields(ArtifactStoreConfig)}
        assert "_validate" not in names
