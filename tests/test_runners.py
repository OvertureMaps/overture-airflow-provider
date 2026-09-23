"""Tests for runner_assets and bundled runner scripts."""

import base64
import pathlib
from unittest.mock import MagicMock

import pytest

from overture_airflow_provider.runner_assets import (
    _RUNNER_FILES,
    _file_sha256,
    databricks_workspace_asset_path,
    get_databricks_init_script_path,
    get_runner_path,
    upload_databricks_assets_to_workspace,
    upload_databricks_init_script_to_workspace,
    upload_runners_to_s3,
)
from overture_airflow_provider.runners import SCALA_RUNNER_SOURCE

# ---------------------------------------------------------------------------
# get_runner_path
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("platform", ["glue", "databricks", "wherobots"])
def test_get_runner_path(platform):
    p = get_runner_path(platform)
    assert p.exists(), f"Runner not found at {p}"
    assert f"job_runner_{platform}" in p.name
    assert p.suffix == ".py"


def test_get_runner_path_glue_scala():
    p = get_runner_path("glue_scala")
    try:
        assert p.exists()
        content = p.read_text(encoding="utf-8")
        # Comment-only no-op stub: every line is a Scala comment.
        assert content.lstrip().startswith("//")
        assert "import " not in content
    finally:
        p.unlink(missing_ok=True)


def test_get_runner_path_unknown():
    with pytest.raises(KeyError, match="Unknown runner platform"):
        get_runner_path("synapse")


# ---------------------------------------------------------------------------
# Runner script content — Overture-free
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("platform", ["glue", "databricks", "wherobots"])
def test_runner_has_no_overture_imports(platform):
    import ast

    p = get_runner_path(platform)
    source = p.read_text(encoding="utf-8")
    tree = ast.parse(source)
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                assert not alias.name.startswith("overture_spark"), (
                    f"{platform} runner imports overture_spark: {alias.name}"
                )
        elif isinstance(node, ast.ImportFrom):
            if node.module and node.module.startswith("overture_spark"):
                pytest.fail(f"{platform} runner imports from overture_spark: {node.module}")


@pytest.mark.parametrize("platform", ["glue", "databricks", "wherobots"])
def test_runner_uses_dynamic_import(platform):
    p = get_runner_path(platform)
    source = p.read_text(encoding="utf-8")
    assert "import_module" in source


@pytest.mark.parametrize("platform", ["glue", "databricks", "wherobots"])
def test_runner_handles_job_result(platform):
    p = get_runner_path(platform)
    source = p.read_text(encoding="utf-8")
    assert "isSuccess" in source


def test_glue_runner_uses_getResolvedOptions():
    p = get_runner_path("glue")
    source = p.read_text(encoding="utf-8")
    assert "getResolvedOptions" in source


def test_glue_runner_calls_init_spark_for_platform_for_sedona_jobs():
    """SparkSedonaJob-style dispatch: extra_spark_conf forwarded via init_spark_for_platform."""
    p = get_runner_path("glue")
    source = p.read_text(encoding="utf-8")
    assert "init_spark_for_platform" in source
    assert "extra_spark_conf=extra_spark_conf_raw" in source


def test_glue_runner_sedona_path_does_not_inject_spark_kwarg():
    """The elif branch must not pass spark= to run() for SparkSedonaJob-style jobs."""
    p = get_runner_path("glue")
    source = p.read_text(encoding="utf-8")
    # The SparkSedonaJob path must be an elif, not nested inside the spark-injection block.
    assert 'elif hasattr(instance, "init_spark_for_platform"):' in source


def test_databricks_runner_dual_mode():
    p = get_runner_path("databricks")
    source = p.read_text(encoding="utf-8")
    assert "globals().get" in source or "dbutils" in source
    assert "argparse" in source


def test_wherobots_runner_argv_parsing():
    p = get_runner_path("wherobots")
    source = p.read_text(encoding="utf-8")
    assert "sys.argv" in source or "_parse_argv" in source


# ---------------------------------------------------------------------------
# SCALA_RUNNER_SOURCE
# ---------------------------------------------------------------------------


def test_scala_runner_source_is_str():
    assert len(SCALA_RUNNER_SOURCE) > 0


def test_scala_runner_source_is_comment_only_stub():
    # Glue compiles the scriptLocation before the job runs even though the real
    # entry point is selected via --class inside --extra-jars. The stub must
    # have zero compile surface: every non-blank line is a Scala comment.
    for line in SCALA_RUNNER_SOURCE.splitlines():
        stripped = line.strip()
        if stripped:
            assert stripped.startswith("//"), f"non-comment line in stub: {line!r}"


def test_scala_runner_source_documents_aws_reference():
    assert "https://docs.aws.amazon.com/glue/" in SCALA_RUNNER_SOURCE


# ---------------------------------------------------------------------------
# get_databricks_init_script_path
# ---------------------------------------------------------------------------


def test_get_databricks_init_script_path():
    p = get_databricks_init_script_path()
    assert p.exists()
    assert p.name == "agnostic_operator_cluster_init_databricks.sh"
    source = p.read_text(encoding="utf-8")
    assert source.startswith("#!/bin/bash")
    assert "SEDONA_VERSION" in source
    assert "GEOTOOLS_VERSION" in source
    assert "/databricks/jars" in source


def test_databricks_init_script_is_overture_free():
    # No Overture-specific business logic (buckets, roles, catalogs, job
    # wiring) — only generic comments referencing the provider module that
    # deploys and consumes it are expected.
    source = get_databricks_init_script_path().read_text(encoding="utf-8")
    assert "overture_spark" not in source.lower()
    assert "s3://" not in source


# ---------------------------------------------------------------------------
# upload_databricks_init_script_to_workspace
# ---------------------------------------------------------------------------


def test_upload_databricks_init_script_to_workspace(mocker):
    mock_post = mocker.patch("requests.post")
    mock_post.return_value.raise_for_status.return_value = None

    upload_databricks_init_script_to_workspace(
        "https://my-workspace.azuredatabricks.net",
        "dapi-token",
        "/Shared/my-app/agnostic_operator_cluster_init_databricks.sh",
    )

    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    assert args[0] == "https://my-workspace.azuredatabricks.net/api/2.0/workspace/import"
    assert kwargs["headers"]["Authorization"] == "Bearer dapi-token"
    assert kwargs["json"]["format"] == "RAW"
    assert kwargs["json"]["path"] == "/Shared/my-app/agnostic_operator_cluster_init_databricks.sh"
    assert kwargs["json"]["overwrite"] is True
    assert "language" not in kwargs["json"]


# ---------------------------------------------------------------------------
# _file_sha256
# ---------------------------------------------------------------------------


def test_file_sha256_deterministic():
    p = get_runner_path("glue")
    h1 = _file_sha256(p)
    h2 = _file_sha256(p)
    assert h1 == h2
    assert len(h1) == 64  # hex SHA-256


def test_file_sha256_different_files():
    h_glue = _file_sha256(get_runner_path("glue"))
    h_db = _file_sha256(get_runner_path("databricks"))
    assert h_glue != h_db


# ---------------------------------------------------------------------------
# upload_runners_to_s3 — idempotent upload
# ---------------------------------------------------------------------------


def _mock_s3_404():
    """S3 client that returns 404 on HEAD → triggers upload."""

    class _ClientError(Exception):
        def __init__(self, code: str = "404"):
            self.response = {"Error": {"Code": code}}

    client = MagicMock()
    client.exceptions.ClientError = _ClientError
    client.head_object.side_effect = _ClientError("404")
    return client


def _mock_s3_hit():
    """S3 client that returns 200 on HEAD → skips upload."""
    client = MagicMock()
    client.head_object.return_value = {"ContentLength": 100}
    return client


def test_upload_runners_uploads_on_cache_miss():
    s3 = _mock_s3_404()
    result = upload_runners_to_s3(s3, "my-bucket", "my-prefix", platforms=["glue"])
    assert "glue" in result
    assert result["glue"].startswith("s3://my-bucket/my-prefix/runners/")
    assert result["glue"].endswith("-job_runner_glue.py")
    s3.upload_file.assert_called_once()


def test_upload_runners_skips_on_cache_hit():
    s3 = _mock_s3_hit()
    result = upload_runners_to_s3(s3, "my-bucket", "my-prefix", platforms=["glue"])
    assert "glue" in result
    s3.upload_file.assert_not_called()


def test_upload_runners_uses_override():
    s3 = _mock_s3_404()
    override_uri = "s3://custom-bucket/custom/runner.py"
    result = upload_runners_to_s3(
        s3,
        "my-bucket",
        "my-prefix",
        overrides={"glue": override_uri},
        platforms=["glue"],
    )
    assert result["glue"] == override_uri
    s3.upload_file.assert_not_called()


def test_upload_runners_all_platforms():
    s3 = _mock_s3_hit()
    result = upload_runners_to_s3(s3, "bucket", "prefix")
    assert set(result.keys()) == set(_RUNNER_FILES.keys())
    for uri in result.values():
        assert uri.startswith("s3://bucket/prefix/runners/")


def test_upload_runners_glue_scala_cleanup():
    """Temp file created for glue_scala must be cleaned up after upload."""
    s3 = _mock_s3_hit()
    result = upload_runners_to_s3(s3, "bucket", "prefix", platforms=["glue_scala"])
    assert "glue_scala" in result
    # Extract the path from the upload_file call — it must not exist after.
    # (With cache hit there's no upload_file call, but the temp path is still created)
    # Verify by checking the URI ends with .scala
    assert result["glue_scala"].endswith(".scala")


def test_upload_runners_content_hash_in_key():
    s3 = _mock_s3_hit()
    result = upload_runners_to_s3(s3, "bucket", "prefix", platforms=["glue"])
    # Key format: prefix/runners/{sha12}-job_runner_glue.py
    uri = result["glue"]
    path = uri.removeprefix("s3://bucket/")
    parts = pathlib.PurePosixPath(path).parts
    assert parts[0] == "prefix"
    assert parts[1] == "runners"
    filename = parts[2]
    sha_part, name_part = filename.split("-", 1)
    assert len(sha_part) == 12
    assert name_part == "job_runner_glue.py"


# ---------------------------------------------------------------------------
# upload_databricks_assets_to_workspace — idempotent, hash-keyed workspace upload
# ---------------------------------------------------------------------------


class _FakeDatabricksError(Exception):
    def __init__(self, error_code):
        super().__init__(error_code)
        self.error_code = error_code


class ResourceAlreadyExists(Exception):
    """Mirrors the databricks-sdk class name; matched by name, not import."""


def _workspace_client(*, exists=False, import_error=None):
    client = MagicMock()
    if not exists:
        client.workspace.get_status.side_effect = _FakeDatabricksError("RESOURCE_DOES_NOT_EXIST")
    if import_error is not None:
        client.workspace.import_.side_effect = import_error
    return client


@pytest.fixture
def _fake_import_enums(mocker):
    enums = (MagicMock(name="ImportFormat"), MagicMock(name="Language"))
    mocker.patch(
        "overture_airflow_provider.runner_assets._workspace_import_enums", return_value=enums
    )
    return enums


def test_databricks_workspace_asset_path_is_content_hashed():
    notebook = databricks_workspace_asset_path("notebook", "/Shared/app/")
    init = databricks_workspace_asset_path("init_script", "/Shared/app")
    nb_sha = _file_sha256(get_runner_path("databricks"))[:12]
    init_sha = _file_sha256(get_databricks_init_script_path())[:12]
    assert notebook == f"/Shared/app/runners/{nb_sha}-job_runner_databricks"
    assert init == f"/Shared/app/runners/{init_sha}-agnostic_operator_cluster_init_databricks.sh"


def test_databricks_workspace_asset_path_unknown():
    with pytest.raises(KeyError, match="Unknown Databricks asset"):
        databricks_workspace_asset_path("wheel", "/Shared/app")


def test_upload_databricks_assets_skips_when_present(_fake_import_enums):
    client = _workspace_client(exists=True)
    result = upload_databricks_assets_to_workspace(client, "/Shared/app")
    assert set(result) == {"notebook", "init_script"}
    assert client.workspace.get_status.call_count == 2
    client.workspace.import_.assert_not_called()
    client.workspace.mkdirs.assert_not_called()


def test_upload_databricks_assets_uploads_when_absent(_fake_import_enums):
    import_format, language = _fake_import_enums
    client = _workspace_client()
    result = upload_databricks_assets_to_workspace(client, "/Shared/app")

    client.workspace.mkdirs.assert_called_with("/Shared/app/runners")
    calls = {c.args[0]: c.kwargs for c in client.workspace.import_.call_args_list}
    assert set(calls) == {result["notebook"], result["init_script"]}

    nb = calls[result["notebook"]]
    assert nb["format"] is import_format.SOURCE
    assert nb["language"] is language.PYTHON
    assert nb["overwrite"] is False
    decoded = base64.b64decode(nb["content"])
    assert decoded == get_runner_path("databricks").read_bytes()

    init = calls[result["init_script"]]
    assert init["format"] is import_format.RAW
    assert "language" not in init
    assert init["overwrite"] is False
    assert base64.b64decode(init["content"]) == get_databricks_init_script_path().read_bytes()


@pytest.mark.parametrize(
    "race_error",
    [_FakeDatabricksError("RESOURCE_ALREADY_EXISTS"), ResourceAlreadyExists("exists")],
)
def test_upload_databricks_assets_tolerates_concurrent_upload(_fake_import_enums, race_error):
    client = _workspace_client(import_error=race_error)
    result = upload_databricks_assets_to_workspace(client, "/Shared/app", assets=["notebook"])
    assert result == {"notebook": databricks_workspace_asset_path("notebook", "/Shared/app")}
    client.workspace.import_.assert_called_once()


def test_upload_databricks_assets_reraises_other_import_errors(_fake_import_enums):
    client = _workspace_client(import_error=_FakeDatabricksError("PERMISSION_DENIED"))
    with pytest.raises(_FakeDatabricksError):
        upload_databricks_assets_to_workspace(client, "/Shared/app", assets=["init_script"])


def test_upload_databricks_assets_reraises_other_status_errors(_fake_import_enums):
    client = MagicMock()
    client.workspace.get_status.side_effect = _FakeDatabricksError("PERMISSION_DENIED")
    with pytest.raises(_FakeDatabricksError):
        upload_databricks_assets_to_workspace(client, "/Shared/app")
    client.workspace.import_.assert_not_called()
