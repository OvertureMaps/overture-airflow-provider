"""Tests for DatabricksSdkHook unified-auth connection mapping."""

import os
import types

import pytest

from overture_airflow_provider.hooks import DatabricksSdkHook, _truthy


def _hook(monkeypatch, **conn_fields):
    conn = types.SimpleNamespace(
        host="https://example.cloud.databricks.com",
        login=None,
        password=None,
        extra_dejson={},
    )
    for key, value in conn_fields.items():
        setattr(conn, key, value)
    monkeypatch.setattr(
        "overture_airflow_provider._airflow_compat.BaseHook.get_connection",
        staticmethod(lambda conn_id: conn),
    )
    return DatabricksSdkHook("databricks_default")


class TestTruthy:
    @pytest.mark.parametrize(
        "value,expected",
        [
            (True, True),
            ("true", True),
            ("True", True),
            ("1", True),
            (False, False),
            ("false", False),
            ("", False),
            (None, False),
        ],
    )
    def test_truthy(self, value, expected):
        assert _truthy(value) is expected


class TestAuthEnv:
    def test_personal_access_token(self, monkeypatch):
        env = _hook(monkeypatch, password="dapiTOKEN")._auth_env()
        assert env == {
            "DATABRICKS_HOST": "https://example.cloud.databricks.com",
            "DATABRICKS_AUTH_TYPE": "pat",
            "DATABRICKS_TOKEN": "dapiTOKEN",
        }

    def test_oauth_m2m_service_principal(self, monkeypatch):
        env = _hook(
            monkeypatch,
            login="client-id",
            password="client-secret",
            extra_dejson={"service_principal_oauth": True},
        )._auth_env()
        assert env["DATABRICKS_AUTH_TYPE"] == "oauth-m2m"
        assert env["DATABRICKS_CLIENT_ID"] == "client-id"
        assert env["DATABRICKS_CLIENT_SECRET"] == "client-secret"
        # OAuth selected -> no PAT fallback leaked
        assert "DATABRICKS_TOKEN" not in env

    def test_oauth_missing_secret_raises(self, monkeypatch):
        with pytest.raises(ValueError, match="service_principal_oauth"):
            _hook(
                monkeypatch,
                login="client-id",
                extra_dejson={"service_principal_oauth": True},
            )._auth_env()

    def test_oauth_flag_as_string(self, monkeypatch):
        env = _hook(
            monkeypatch,
            login="cid",
            password="sec",
            extra_dejson={"service_principal_oauth": "true"},
        )._auth_env()
        assert env["DATABRICKS_CLIENT_ID"] == "cid"

    def test_azure_service_principal_no_token_conflict(self, monkeypatch):
        # Azure SP connections carry a password (the client secret); the hook
        # must NOT also set DATABRICKS_TOKEN or the SDK gets conflicting creds.
        env = _hook(
            monkeypatch,
            login="azure-client-id",
            password="azure-client-secret",
            extra_dejson={"azure_tenant_id": "tenant-123"},
        )._auth_env()
        assert env["ARM_TENANT_ID"] == "tenant-123"
        assert env["ARM_CLIENT_ID"] == "azure-client-id"
        assert env["ARM_CLIENT_SECRET"] == "azure-client-secret"
        assert env["DATABRICKS_AUTH_TYPE"] == "azure-client-secret"
        assert "DATABRICKS_TOKEN" not in env
        assert "DATABRICKS_CLIENT_ID" not in env

    def test_azure_missing_secret_raises(self, monkeypatch):
        with pytest.raises(ValueError, match="azure_tenant_id"):
            _hook(
                monkeypatch,
                login="azure-client-id",
                extra_dejson={"azure_tenant_id": "tenant-123"},
            )._auth_env()

    def test_federated_k8s_via_login_sentinel(self, monkeypatch):
        env = _hook(
            monkeypatch,
            login="federated_k8s",
            extra_dejson={"client_id": "oidc-client"},
        )._auth_env()
        assert env["DATABRICKS_CLIENT_ID"] == "oidc-client"
        assert "DATABRICKS_TOKEN" not in env

    def test_federated_k8s_without_client_id(self, monkeypatch):
        # Falls through to host-only; the SDK auto-discovers in-cluster identity.
        env = _hook(monkeypatch, extra_dejson={"federated_k8s": True})._auth_env()
        assert env == {"DATABRICKS_HOST": "https://example.cloud.databricks.com"}

    def test_default_host_only(self, monkeypatch):
        env = _hook(monkeypatch)._auth_env()
        assert env == {"DATABRICKS_HOST": "https://example.cloud.databricks.com"}


class TestGetWorkspaceClient:
    def test_injects_then_restores_env(self, monkeypatch):
        import databricks.sdk as sdk

        seen = {}

        def _fake_client():
            seen["token"] = os.environ.get("DATABRICKS_TOKEN")
            seen["host"] = os.environ.get("DATABRICKS_HOST")
            return types.SimpleNamespace(clusters=None)

        monkeypatch.setattr(sdk, "WorkspaceClient", _fake_client)
        monkeypatch.delenv("DATABRICKS_TOKEN", raising=False)

        hook = _hook(monkeypatch, password="dapiTOKEN")
        with hook.get_workspace_client():
            pass

        # auth env was visible to the SDK during the context...
        assert seen["token"] == "dapiTOKEN"
        assert seen["host"] == "https://example.cloud.databricks.com"
        # ...and restored (removed) afterward
        assert "DATABRICKS_TOKEN" not in os.environ

    def test_restores_preexisting_env_value(self, monkeypatch):
        import databricks.sdk as sdk

        monkeypatch.setattr(
            sdk, "WorkspaceClient", lambda *a, **k: types.SimpleNamespace(clusters=None)
        )
        monkeypatch.setenv("DATABRICKS_TOKEN", "preexisting")

        hook = _hook(monkeypatch, password="dapiTOKEN")
        with hook.get_workspace_client():
            assert os.environ["DATABRICKS_TOKEN"] == "dapiTOKEN"

        # original value restored, not clobbered
        assert os.environ["DATABRICKS_TOKEN"] == "preexisting"

    def test_masks_ambient_auth_vars(self, monkeypatch):
        # A stale ambient PAT / config profile must not contaminate an OAuth
        # connection: the context masks all managed auth vars.
        import databricks.sdk as sdk

        seen = {}

        def _fake_client():
            seen["token"] = os.environ.get("DATABRICKS_TOKEN")
            seen["profile"] = os.environ.get("DATABRICKS_CONFIG_PROFILE")
            seen["client_id"] = os.environ.get("DATABRICKS_CLIENT_ID")
            return types.SimpleNamespace(clusters=None)

        monkeypatch.setattr(sdk, "WorkspaceClient", _fake_client)
        monkeypatch.setenv("DATABRICKS_TOKEN", "stale-pat")
        monkeypatch.setenv("DATABRICKS_CONFIG_PROFILE", "stale-profile")

        hook = _hook(
            monkeypatch,
            login="cid",
            password="sec",
            extra_dejson={"service_principal_oauth": True},
        )
        with hook.get_workspace_client():
            pass

        assert seen["token"] is None
        assert seen["profile"] is None
        assert seen["client_id"] == "cid"
        # ambient values restored afterward
        assert os.environ["DATABRICKS_TOKEN"] == "stale-pat"
        assert os.environ["DATABRICKS_CONFIG_PROFILE"] == "stale-profile"
