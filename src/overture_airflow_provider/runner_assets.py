"""Bundled runner-script resolution and S3 upload utilities.

Two canonical operations:

- :func:`get_runner_path` — resolve the local filesystem path to a bundled
  runner script via :mod:`importlib.resources`.
- :func:`upload_runners_to_s3` — content-hash-keyed idempotent S3 upload for
  Glue and Wherobots runner scripts.

**Databricks runner:** The Databricks runner is a Workspace Notebook and must
be deployed to the Databricks workspace separately from S3. Use
:func:`get_runner_path` to obtain the source file and deploy it via your CI/CD
pipeline or the :func:`upload_databricks_runner_to_workspace` helper.

**Databricks cluster init script:** Likewise deployed to the workspace, not
S3. Use :func:`get_databricks_init_script_path` to obtain the source file or
:func:`upload_databricks_init_script_to_workspace` to deploy it directly.
"""

import hashlib
import importlib.resources
import pathlib
import tempfile
from typing import Any

from overture_airflow_provider.runners import SCALA_RUNNER_SOURCE

_RUNNER_FILES: dict[str, str] = {
    "glue": "job_runner_glue.py",
    "glue_scala": "job_runner_glue.scala",  # materialised from SCALA_RUNNER_SOURCE
    "databricks": "job_runner_databricks.py",
    "wherobots": "job_runner_wherobots.py",
}

# Matches DatabricksConfig.cluster_init_script_name's default (config.py).
_DATABRICKS_INIT_SCRIPT_NAME = "agnostic_operator_cluster_init_databricks.sh"


def get_runner_path(platform: str) -> pathlib.Path:
    """Return the local filesystem path to a bundled runner script.

    For ``"glue_scala"`` the Scala source is written to a fresh temporary file
    on every call; the caller is responsible for cleanup. Prefer
    :func:`upload_runners_to_s3` for S3 uploads — it handles the temp file
    lifecycle automatically.

    Args:
        platform: One of ``"glue"``, ``"glue_scala"``, ``"databricks"``,
            ``"wherobots"``.

    Returns:
        Absolute :class:`pathlib.Path` to the runner file.

    Raises:
        KeyError: For unrecognised platform names.
    """
    if platform not in _RUNNER_FILES:
        raise KeyError(f"Unknown runner platform {platform!r}. Valid: {sorted(_RUNNER_FILES)}")

    if platform == "glue_scala":
        tmp = tempfile.NamedTemporaryFile(
            mode="w",
            suffix="-job_runner_glue.scala",
            delete=False,
            encoding="utf-8",
        )
        tmp.write(SCALA_RUNNER_SOURCE)
        tmp.close()
        return pathlib.Path(tmp.name)

    pkg_path = importlib.resources.files("overture_airflow_provider.runners").joinpath(
        _RUNNER_FILES[platform]
    )
    return pathlib.Path(str(pkg_path))


def _file_sha256(path: pathlib.Path) -> str:
    """Return the hex SHA-256 digest of a local file."""
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(65_536), b""):
            h.update(chunk)
    return h.hexdigest()


def upload_runners_to_s3(
    s3_client: Any,
    bucket: str,
    prefix: str,
    *,
    overrides: dict[str, str] | None = None,
    platforms: list[str] | None = None,
) -> dict[str, str]:
    """Upload bundled runner scripts to S3 with content-hash keyed paths.

    The S3 key is ``{prefix}/runners/{sha256[:12]}-{name}``, so an identical
    file already present in S3 is detected via a ``HEAD`` check and skipped.
    This makes repeated calls fully idempotent.

    Args:
        s3_client: Boto3 S3 client instance.
        bucket: Target S3 bucket name.
        prefix: Key prefix under which runners are stored
            (e.g. ``"spark-agnostic-operator"``).
        overrides: Optional ``{platform: s3_uri}`` mapping. When a platform
            appears here its URI is returned as-is and the bundled file is not
            uploaded.
        platforms: Subset of platforms to process. Defaults to all four:
            ``"glue"``, ``"glue_scala"``, ``"databricks"``, ``"wherobots"``.

    Returns:
        Dict mapping platform name → ``s3://bucket/key`` URI.
    """
    overrides = overrides or {}
    platforms = platforms or list(_RUNNER_FILES.keys())
    result: dict[str, str] = {}
    _tmp_paths: list[pathlib.Path] = []

    try:
        for platform in platforms:
            if platform in overrides:
                result[platform] = overrides[platform]
                continue

            local_path = get_runner_path(platform)
            if platform == "glue_scala":
                _tmp_paths.append(local_path)

            sha = _file_sha256(local_path)[:12]
            name = _RUNNER_FILES[platform]
            s3_key = f"{prefix}/runners/{sha}-{name}"
            s3_uri = f"s3://{bucket}/{s3_key}"

            try:
                s3_client.head_object(Bucket=bucket, Key=s3_key)
                print(f"Runner already cached in S3, skipping upload: {s3_uri}")
            except s3_client.exceptions.ClientError as exc:
                if exc.response["Error"]["Code"] == "404":
                    s3_client.upload_file(str(local_path), bucket, s3_key)
                    print(f"Runner uploaded to S3: {s3_uri}")
                else:
                    raise

            result[platform] = s3_uri
    finally:
        for tmp in _tmp_paths:
            try:
                tmp.unlink()
            except OSError:
                pass

    return result


def upload_databricks_runner_to_workspace(
    databricks_host: str,
    databricks_token: str,
    workspace_path: str,
    *,
    overwrite: bool = True,
) -> None:
    """Upload the bundled Databricks runner to a Databricks Workspace path.

    Uses the Databricks Workspace Import API (``/api/2.0/workspace/import``).

    Args:
        databricks_host: Databricks workspace URL
            (e.g. ``"https://my-workspace.azuredatabricks.net"``).
        databricks_token: Databricks personal access token.
        workspace_path: Target workspace path **without** ``.py`` extension
            (Databricks notebook convention), e.g.
            ``"/Workspace/Shared/my-app/job_runner_databricks"``.
        overwrite: Whether to overwrite an existing notebook. Default ``True``.
    """
    import base64

    import requests

    runner_path = get_runner_path("databricks")
    source = runner_path.read_text(encoding="utf-8")
    encoded = base64.b64encode(source.encode()).decode()

    url = databricks_host.rstrip("/") + "/api/2.0/workspace/import"
    resp = requests.post(
        url,
        headers={"Authorization": f"Bearer {databricks_token}"},
        json={
            "path": workspace_path,
            "language": "PYTHON",
            "format": "SOURCE",
            "content": encoded,
            "overwrite": overwrite,
        },
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Databricks runner uploaded to workspace: {workspace_path}")


def get_databricks_init_script_path() -> pathlib.Path:
    """Return the local filesystem path to the bundled Databricks cluster
    init script.

    Mirrors :func:`get_runner_path` for the job runners: the script ships as a
    plain file under ``overture_airflow_provider.runners`` and is resolved via
    :mod:`importlib.resources`.

    Returns:
        Absolute :class:`pathlib.Path` to the init script.
    """
    pkg_path = importlib.resources.files("overture_airflow_provider.runners").joinpath(
        _DATABRICKS_INIT_SCRIPT_NAME
    )
    return pathlib.Path(str(pkg_path))


def upload_databricks_init_script_to_workspace(
    databricks_host: str,
    databricks_token: str,
    workspace_path: str,
    *,
    overwrite: bool = True,
) -> None:
    """Upload the bundled Databricks cluster init script to a Workspace path.

    Uses the Databricks Workspace Import API (``/api/2.0/workspace/import``)
    with ``format="RAW"`` (the script is a plain shell file, not a notebook).

    Args:
        databricks_host: Databricks workspace URL
            (e.g. ``"https://my-workspace.azuredatabricks.net"``).
        databricks_token: Databricks personal access token.
        workspace_path: Target workspace path for the script, e.g.
            ``"/Shared/my-app/agnostic_operator_cluster_init_databricks.sh"``.
            Should match ``DatabricksConfig.cluster_init_script_name`` under
            ``DatabricksConfig.workspace_scripts_path_template``.
        overwrite: Whether to overwrite an existing file. Default ``True``.
    """
    import base64

    import requests

    source = get_databricks_init_script_path().read_text(encoding="utf-8")
    encoded = base64.b64encode(source.encode()).decode()

    url = databricks_host.rstrip("/") + "/api/2.0/workspace/import"
    resp = requests.post(
        url,
        headers={"Authorization": "Bearer " + databricks_token},
        json={
            "path": workspace_path,
            "format": "RAW",
            "content": encoded,
            "overwrite": overwrite,
        },
        timeout=30,
    )
    resp.raise_for_status()
    print(f"Databricks cluster init script uploaded to workspace: {workspace_path}")
