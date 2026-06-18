"""Tests for python_package_utils, focused on keeping the CodeArtifact auth
token out of process arguments and log/exception output (issue #36)."""

import logging
import sys
import types
from unittest.mock import MagicMock, patch

import pytest
from packaging.version import Version

from overture_airflow_provider.python_package_utils import (
    CodeArtifactPyPiClient,
    PackageVersionStrategy,
    PyPiDownloader,
    mask_url_credentials,
)

_TOKEN = "SUPERSECRETAUTHTOKEN"
_INDEX_URL = (
    f"https://aws:{_TOKEN}@domain-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"
)


def _make_fake_sh(pip_impl):
    """Build a fake ``sh`` module exposing ``pip`` and ``ErrorReturnCode``."""
    fake_sh = types.ModuleType("sh")

    class ErrorReturnCode(Exception):
        def __init__(self, stderr: bytes = b""):
            super().__init__("pip failed")
            self.stderr = stderr

    fake_sh.pip = pip_impl
    fake_sh.ErrorReturnCode = ErrorReturnCode
    return fake_sh


def _make_downloader(tmp_path):
    client = MagicMock()
    client.get_url.return_value = _INDEX_URL
    return PyPiDownloader(client, str(tmp_path))


def test_mask_url_credentials_redacts_password():
    assert mask_url_credentials(_INDEX_URL) == (
        "https://aws:***@domain-123.d.codeartifact.us-east-1.amazonaws.com/pypi/repo/simple/"
    )
    assert _TOKEN not in mask_url_credentials(_INDEX_URL)


def test_mask_url_credentials_handles_free_text():
    text = f"could not connect to {_INDEX_URL} (timeout)"
    masked = mask_url_credentials(text)
    assert _TOKEN not in masked
    assert "***@" in masked


def test_mask_url_credentials_noop_without_credentials():
    url = "https://pypi.org/simple/"
    assert mask_url_credentials(url) == url


def test_token_passed_via_env_not_argv(tmp_path, monkeypatch):
    captured = {}

    def fake_pip(*args, **kwargs):
        captured["args"] = args
        captured["env"] = kwargs.get("_env")

    monkeypatch.setitem(sys.modules, "sh", _make_fake_sh(fake_pip))

    _make_downloader(tmp_path).download_packages(["mypkg"], "3.11")

    # Token must never appear in the command-line arguments.
    assert all(_TOKEN not in str(arg) for arg in captured["args"])
    assert "--index-url" not in captured["args"]
    # Token is delivered to pip via the environment instead.
    assert captured["env"]["PIP_INDEX_URL"] == _INDEX_URL


def test_error_message_masks_token(tmp_path, monkeypatch, caplog):
    fake_sh = _make_fake_sh(None)

    def fake_pip(*args, **kwargs):
        raise fake_sh.ErrorReturnCode(stderr=f"ERROR: failed fetching {_INDEX_URL}".encode())

    fake_sh.pip = fake_pip
    monkeypatch.setitem(sys.modules, "sh", fake_sh)

    downloader = _make_downloader(tmp_path)
    with caplog.at_level(logging.ERROR), pytest.raises(fake_sh.ErrorReturnCode):
        downloader.download_packages(["mypkg"], "3.11")

    assert _TOKEN not in caplog.text
    assert "***@" in caplog.text


def test_download_packages_noop_when_empty(tmp_path, monkeypatch):
    called = {"pip": False}

    def fake_pip(*args, **kwargs):
        called["pip"] = True

    monkeypatch.setitem(sys.modules, "sh", _make_fake_sh(fake_pip))

    _make_downloader(tmp_path).download_packages([], "3.11")
    assert called["pip"] is False


# ─── CodeArtifactPyPiClient version resolution ────────────────────────────────


@pytest.fixture
def _client():
    """CodeArtifactPyPiClient — no boto3 calls at construction time."""
    return CodeArtifactPyPiClient(
        domain_owner="123", domain="dom", repository="repo", region_name="us-east-1"
    )


def test_resolve_custom_returns_version_as_is(_client):
    result = _client.resolve_package_version(
        "pkg", PackageVersionStrategy.CUSTOM, custom_version="1.2.3"
    )
    assert result == "1.2.3"


def test_resolve_custom_without_version_raises(_client):
    with pytest.raises(ValueError, match="custom_version must be specified"):
        _client.resolve_package_version("pkg", PackageVersionStrategy.CUSTOM)


@pytest.mark.parametrize("non_stable", ["1.1.0a1", "2.0.0.dev1"])
def test_resolve_latest_stable_skips_non_stable(_client, non_stable):
    from packaging.version import Version

    stable = Version("1.0.0")
    with patch.object(_client, "get_package_versions", return_value=[Version(non_stable), stable]):
        result = _client.resolve_package_version("pkg", PackageVersionStrategy.LATEST_STABLE)
    assert result == "1.0.0"


def test_resolve_latest_in_branch_matches(_client):
    versions = [
        Version("0.0.1.dev0+mybranch.1"),
        Version("0.0.1.dev0+otherbranch.1"),
        Version("1.0.0"),
    ]
    with patch.object(_client, "get_package_versions", return_value=versions):
        result = _client.resolve_package_version(
            "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="mybranch"
        )
    assert result == "0.0.1.dev0+mybranch.1"


def test_resolve_latest_in_branch_hyphen_normalised(_client):
    """Branch names with hyphens are normalised to remove them for local-version matching."""
    versions = [Version("0.0.1.dev0+mybranch.1")]
    with patch.object(_client, "get_package_versions", return_value=versions):
        result = _client.resolve_package_version(
            "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="my-branch"
        )
    assert result == "0.0.1.dev0+mybranch.1"


def test_resolve_latest_in_branch_no_match_returns_none_str(_client):
    versions = [Version("1.0.0")]
    with patch.object(_client, "get_package_versions", return_value=versions):
        result = _client.resolve_package_version(
            "pkg", PackageVersionStrategy.LATEST_IN_BRANCH, branch="missing"
        )
    assert result == "None"
