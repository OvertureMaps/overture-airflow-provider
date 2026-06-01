"""Airflow hooks for the Overture Spark provider."""

import os
from contextlib import contextmanager

from overture_airflow_provider._airflow_compat import BaseHook

# Every Databricks/Azure auth env var the SDK's unified auth might read. The
# client context masks all of these so a stale ambient value (e.g. a developer's
# DATABRICKS_TOKEN or DATABRICKS_CONFIG_PROFILE) can never contaminate the mode
# selected from the Airflow connection.
_MANAGED_ENV = (
    "DATABRICKS_HOST",
    "DATABRICKS_TOKEN",
    "DATABRICKS_CLIENT_ID",
    "DATABRICKS_CLIENT_SECRET",
    "DATABRICKS_AUTH_TYPE",
    "DATABRICKS_CONFIG_PROFILE",
    "ARM_TENANT_ID",
    "ARM_CLIENT_ID",
    "ARM_CLIENT_SECRET",
)


def _truthy(value) -> bool:
    """Interpret an Airflow connection ``extra`` flag as a boolean.

    Connection extras are JSON, so a flag may arrive as a real ``bool`` or as a
    string such as ``"true"`` / ``"false"``.
    """
    if isinstance(value, str):
        return value.strip().lower() in ("1", "true", "yes", "on")
    return bool(value)


class DatabricksSdkHook(BaseHook):
    """Create a ``databricks-sdk`` ``WorkspaceClient`` from an Airflow connection.

    Maps the Airflow connection onto the SDK's *unified authentication* so the
    provider works with the full range of Databricks auth methods rather than
    personal access tokens only. Exactly one mode is selected (most specific
    first) so the SDK is never handed conflicting credentials:

    1. **Azure service principal** — ``extra.azure_tenant_id`` set; ``login`` /
       ``password`` are the Entra ID client id / secret.
    2. **OAuth M2M service principal** — ``extra.service_principal_oauth``
       truthy; ``login`` / ``password`` are the client id / secret.
    3. **Federated OIDC** (in-cluster, e.g. EKS/AKS/GKE) — ``login`` is
       ``"federated_k8s"`` or ``extra.federated_k8s`` truthy; the SDK
       auto-discovers the workload-identity token (optional ``extra.client_id``).
    4. **Personal access token** — ``password`` set (fallback).
    5. **Default** — host only; the SDK falls back to its own config discovery.

    Auth is injected via environment variables for the duration of the client
    context and restored afterward, which is how the SDK's unified-auth
    discovery resolves credentials uniformly across clouds. The context also
    masks any pre-existing Databricks/Azure auth env vars so ambient state can't
    override the connection. This makes the context process-global and **not
    thread-safe**: the yielded client must be used only inside the ``with``
    block, and the block should not spawn subprocesses or overlapping contexts.
    """

    def __init__(self, databricks_conn_id: str = "databricks_default"):
        super().__init__()
        self.databricks_conn_id = databricks_conn_id

    def _auth_env(self) -> dict:
        """Map the Airflow connection to SDK unified-auth environment variables.

        Raises ``ValueError`` if a service-principal mode is selected but the
        connection is missing the client id / secret it requires (so we fail
        loud rather than silently degrading to ambient/default credentials).
        """
        conn = self.get_connection(self.databricks_conn_id)
        extra = conn.extra_dejson or {}
        env = {"DATABRICKS_HOST": conn.host}

        if extra.get("azure_tenant_id"):
            if not (conn.login and conn.password):
                raise ValueError(
                    f"Databricks connection {self.databricks_conn_id!r} sets "
                    "azure_tenant_id but is missing the service-principal client id "
                    "(login) and/or secret (password)"
                )
            env["DATABRICKS_AUTH_TYPE"] = "azure-client-secret"
            env["ARM_TENANT_ID"] = extra.get("azure_tenant_id")
            env["ARM_CLIENT_ID"] = conn.login
            env["ARM_CLIENT_SECRET"] = conn.password
        elif _truthy(extra.get("service_principal_oauth")):
            if not (conn.login and conn.password):
                raise ValueError(
                    f"Databricks connection {self.databricks_conn_id!r} requests "
                    "service_principal_oauth but is missing the client id (login) "
                    "and/or secret (password)"
                )
            env["DATABRICKS_AUTH_TYPE"] = "oauth-m2m"
            env["DATABRICKS_CLIENT_ID"] = conn.login
            env["DATABRICKS_CLIENT_SECRET"] = conn.password
        elif conn.login == "federated_k8s" or _truthy(extra.get("federated_k8s")):
            # In-cluster workload identity: the SDK discovers the OIDC token. An
            # explicit client_id is optional; without it the SDK uses its default
            # federated discovery (ambient creds are still masked by the context).
            client_id = extra.get("client_id")
            if client_id:
                env["DATABRICKS_CLIENT_ID"] = client_id
        elif conn.password:
            env["DATABRICKS_AUTH_TYPE"] = "pat"
            env["DATABRICKS_TOKEN"] = conn.password

        return {k: v for k, v in env.items() if v is not None}

    @contextmanager
    def get_workspace_client(self):
        """Yield a ``WorkspaceClient`` authenticated from the connection.

        Connection-derived auth env vars are set for the duration of the
        ``with`` block; all managed Databricks/Azure auth env vars are masked
        and the previous environment is restored on exit.
        """
        # Lazy import: the databricks-sdk lives in the optional [databricks] extra.
        from databricks.sdk import WorkspaceClient

        auth_env = self._auth_env()
        previous = {key: os.environ.get(key) for key in _MANAGED_ENV}
        try:
            for key in _MANAGED_ENV:
                os.environ.pop(key, None)
            os.environ.update(auth_env)
            yield WorkspaceClient()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value
