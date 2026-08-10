"""Framework-agnostic request handling for the bundle inspector API.

These functions contain the actual routing logic (config resolution, param
parsing, calls into :mod:`s3`) with no Flask or FastAPI imports. Airflow 2
loads Flask blueprints; Airflow 3 loads FastAPI apps; each framework adapter
(``flask_app.py`` / ``fastapi_app.py``) is a thin wrapper over this module so
neither web framework becomes a hard import for the Airflow major version
that doesn't ship it.
"""

import os
import re
from functools import lru_cache

from airflow.configuration import conf

from . import s3

# ``airflow.cfg`` section for this plugin's settings (see provider_info.py's
# "config" entry for the full option list). Airflow merges this section's
# defaults/descriptions from that entry, so `conf.get` works even before an
# operator has set anything explicitly, and every option is overridable via
# the standard `AIRFLOW__BUNDLE_INSPECTOR__<OPTION>` env var without any
# extra wiring here.
CONFIG_SECTION = "bundle_inspector"

QUERY_ID_PATTERN = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
S3PROXY_ALLOWED_SUFFIXES = (".pmtiles", ".parquet", ".json", ".csv", ".md")


class ApiError(Exception):
    """Raised by an endpoint function; adapters translate this to an HTTP error."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status
        self.message = message


class S3ObjectResponse:
    """Headers plus an optional body stream for a proxied S3 object.

    ``body`` is ``None`` for HEAD requests (headers only).
    """

    def __init__(self, headers: dict, status: int, body=None):
        self.headers = headers
        self.status = status
        self.body = body


@lru_cache(maxsize=1)
def _s3_client():
    import boto3

    return boto3.client("s3")


def get_config(user_override: str | None = None) -> dict:
    """Read bucket and namespace config from ``airflow.cfg``'s ``bundle_inspector`` section.

    Called inside request handlers, never at import time, so a config change
    (or its `AIRFLOW__BUNDLE_INSPECTOR__*` env var override) takes effect
    without restarting the plugin.

    The environment is read from the ``environment`` option so the namespace
    decision uses the same source of truth. In ``dev``, the namespace is
    taken from the ``user`` option (or overridden by ``user_override``).
    """
    bucket = conf.get(CONFIG_SECTION, "s3_bucket", fallback=None)
    if not bucket:
        raise ApiError(
            500,
            f"[{CONFIG_SECTION}] s3_bucket is not set in airflow.cfg "
            f"(or AIRFLOW__{CONFIG_SECTION.upper()}__S3_BUCKET)",
        )
    athena_output_bucket = conf.get(CONFIG_SECTION, "athena_output_bucket", fallback=None)
    environment = conf.get(CONFIG_SECTION, "environment", fallback="dev")
    if environment == "dev":
        namespace = (
            user_override
            if user_override is not None
            else conf.get(CONFIG_SECTION, "user", fallback="")
        )
    else:
        namespace = ""
    namespace_prefix = f"{namespace}/" if namespace else ""
    return {
        "bucket": bucket,
        "athena_output_bucket": athena_output_bucket,
        "environment": environment,
        "namespace": namespace,
        "namespace_prefix": namespace_prefix,
    }


def config(user: str | None) -> dict:
    """Return bucket, environment, and namespace for display."""
    cfg = get_config(user_override=user)
    return {
        "bucket": cfg["bucket"],
        "environment": cfg["environment"],
        "namespace": cfg["namespace"],
        "aws_region": os.environ.get("AWS_REGION", "us-west-2"),
    }


def list_children(prefix: str, user: str | None) -> dict:
    """List children at a given prefix in the bundle hierarchy.

    Args:
        prefix: Relative path from namespace root (e.g. "theme_promote/theme=places/").
            Empty string returns the known pipeline stages.
        user: Override namespace (dev only).
    """
    cfg = get_config(user_override=user)
    depth = s3.get_depth(prefix)

    # Root listing: return known stages as synthetic entries (no S3 call)
    if depth == 0:
        children = [{"name": f"{s}/", "type": "directory"} for s in s3.KNOWN_STAGES]
        return {
            "prefix": prefix,
            "depth": depth,
            "has_success": False,
            "has_metadata": False,
            "children": children,
        }

    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    items = s3.list_prefix(client, cfg["bucket"], full_prefix)

    # Determine stage from the first segment of the prefix
    stage = prefix.split("/")[0]
    relative_depth = depth - 1  # depth within the stage hierarchy
    filtered = s3.filter_bundle_level(items, stage, relative_depth)

    has_success = any(item["name"] == "success" for item in items)
    has_metadata = any(item["name"] == "metadata.json" for item in items)

    return {
        "prefix": prefix,
        "depth": depth,
        "stage": stage,
        "run_depth": s3.run_depth_for_stage(stage),
        "hierarchy": s3.hierarchy_for_stage(stage),
        "has_success": has_success,
        "has_metadata": has_metadata,
        "children": filtered,
    }


def list_users() -> dict:
    """List user namespaces (top-level prefixes). Only useful in dev."""
    cfg = get_config()
    if cfg["environment"] != "dev":
        return {"users": []}
    client = _s3_client()
    return {"users": s3.list_users(client, cfg["bucket"])}


def theme_types(prefix: str, user: str | None) -> dict:
    """Discover theme=X/type=Y/ Hive partitions under a component prefix.

    Recurses through any non-theme, non-type partition levels (e.g.
    ``dataset=``) until it reaches ``theme=`` directories or leaf data. The
    returned ``theme_dirs`` values include the full relative sub-path so the
    frontend can build narrowed prefixes without knowing the intermediate
    structure.

    Args:
        prefix: Relative path to a component (e.g. ".../partition_bboxes/").
        user: Override namespace (dev only).
    """
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()

    def partition_value(name):
        """Extract the value from a Hive partition or bare directory name."""
        stripped = name.rstrip("/")
        if "=" in stripped:
            return stripped.split("=", 1)[1]
        return stripped

    def find_theme_dirs(s3_prefix, path_so_far="", labels_so_far=()):
        """Recurse until we find theme= or bare (non-Hive) directories.

        Returns a list of (label, relative_path) tuples where label is
        a composite of all partition values along the path (e.g.
        "DadosAbertos / buildings") and relative_path is the full
        sub-path from the component root.
        """
        items = s3.list_prefix(client, cfg["bucket"], s3_prefix)
        dirs = [d for d in items if d["type"] == "directory" and not d["name"].startswith(".")]

        # theme_stage uses bare dir names ("buildings/") instead of
        # Hive-style ("theme=buildings/"). Detect both.
        theme_dirs_here = [
            d for d in dirs if d["name"].startswith("theme=") or "=" not in d["name"]
        ]

        if theme_dirs_here:
            result = []
            for d in theme_dirs_here:
                dir_name = d["name"].rstrip("/")
                val = partition_value(dir_name)
                label = " / ".join((*labels_so_far, val)) if labels_so_far else val
                rel = f"{path_so_far}/{dir_name}" if path_so_far else dir_name
                result.append((label, rel))
            return sorted(result, key=lambda e: e[0])

        # No theme= dirs found -- recurse through other partition dirs
        result = []
        for d in dirs:
            dir_name = d["name"].rstrip("/")
            val = partition_value(dir_name)
            child_prefix = f"{s3_prefix}{dir_name}/"
            child_path = f"{path_so_far}/{dir_name}" if path_so_far else dir_name
            result.extend(find_theme_dirs(child_prefix, child_path, (*labels_so_far, val)))
        return sorted(result, key=lambda e: e[0])

    theme_entries = find_theme_dirs(full_prefix)

    themes = []  # display labels (e.g. "DadosAbertos / buildings")
    theme_dirs = []  # relative sub-paths (for building prefixes)
    types_by_theme = {}  # keyed by display label (unique due to composite)
    for label, rel_path in theme_entries:
        themes.append(label)
        theme_dirs.append(rel_path)
        theme_prefix = f"{full_prefix}{rel_path}/"
        children = s3.list_prefix(client, cfg["bucket"], theme_prefix)
        type_entries = sorted(
            (partition_value(d["name"]), d["name"].rstrip("/"))
            for d in children
            if d["type"] == "directory" and d["name"].startswith("type=")
        )
        types_by_theme[label] = [e[1] for e in type_entries]

    return {
        "themes": themes,
        "theme_dirs": theme_dirs,
        "types_by_theme": types_by_theme,
    }


def count(prefix: str, user: str | None) -> dict:
    """Count objects under a prefix (for component detail view)."""
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    total = s3.count_objects(client, cfg["bucket"], full_prefix)
    return {"prefix": prefix, "count": total}


def tree(prefix: str, user: str | None) -> dict:
    """Return an abridged directory tree under a prefix."""
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    return s3.list_tree(client, cfg["bucket"], full_prefix)


def component_data(prefix: str, user: str | None) -> dict:
    """Load parquet data from a component and return as GeoJSON."""
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    try:
        return s3.load_parquet_geojson(client, cfg["bucket"], full_prefix)
    except Exception as e:
        raise ApiError(500, str(e)) from e


def parquet_stats(prefix: str, user: str | None) -> dict:
    """File count, size stats, and schema for parquet files under a prefix."""
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    try:
        return s3.parquet_stats(client, cfg["bucket"], full_prefix)
    except Exception as e:
        raise ApiError(500, str(e)) from e


def presign(prefix: str, suffix: str, user: str | None) -> dict:
    """Generate a presigned S3 URL for a file under a component prefix."""
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    url = s3.presign_file(client, cfg["bucket"], full_prefix, suffix)
    if not url:
        raise ApiError(404, "No matching file found")
    return {"url": url}


def resolve(prefix: str, suffix: str, user: str | None) -> dict:
    """Find a file by suffix under a prefix and return its key.

    Returns the key relative to the namespace prefix, for use with s3proxy.
    """
    cfg = get_config(user_override=user)
    full_prefix = f"{cfg['namespace_prefix']}{prefix}"
    client = _s3_client()
    full_key = s3.find_file_key(client, cfg["bucket"], full_prefix, suffix)
    if not full_key:
        raise ApiError(404, "No matching file found")
    ns = cfg["namespace_prefix"]
    relative_key = full_key[len(ns) :] if full_key.startswith(ns) else full_key
    return {"key": relative_key}


def s3proxy(
    key: str, user: str | None, range_header: str | None, head_only: bool
) -> S3ObjectResponse:
    """Proxy (optionally ranged) requests to an object under the user's namespace.

    Restricted to an allowlist of file extensions and rejects path traversal,
    since the key is attacker-controlled input from the browser.
    """
    if not key:
        raise ApiError(400, "key is required")
    if ".." in key or key.startswith("/"):
        raise ApiError(400, "invalid key")
    if not key.endswith(S3PROXY_ALLOWED_SUFFIXES):
        raise ApiError(400, f"key must end with one of {S3PROXY_ALLOWED_SUFFIXES}")

    cfg = get_config(user_override=user)
    full_key = f"{cfg['namespace_prefix']}{key}"
    client = _s3_client()
    params = {"Bucket": cfg["bucket"], "Key": full_key}

    if head_only:
        try:
            head_resp = client.head_object(**params)
        except Exception as e:
            raise ApiError(404, str(e)) from e
        return S3ObjectResponse(
            headers={
                "Content-Type": head_resp.get("ContentType", "application/octet-stream"),
                "Content-Length": str(head_resp["ContentLength"]),
                "Accept-Ranges": "bytes",
            },
            status=200,
        )

    if range_header:
        params["Range"] = range_header

    try:
        resp = client.get_object(**params)
    except Exception as e:
        raise ApiError(404, str(e)) from e

    headers = {
        "Content-Type": resp.get("ContentType", "application/octet-stream"),
        "Content-Length": str(resp["ContentLength"]),
        "Accept-Ranges": "bytes",
    }
    if "ContentRange" in resp:
        headers["Content-Range"] = resp["ContentRange"]

    return S3ObjectResponse(
        headers=headers,
        status=206 if range_header else 200,
        body=_stream_s3_body(resp["Body"]),
    )


def _stream_s3_body(body, chunk_size: int = 65536):
    """Yield chunks from an S3 streaming body, closing it when exhausted."""
    try:
        while True:
            chunk = body.read(chunk_size)
            if not chunk:
                break
            yield chunk
    finally:
        body.close()


def athena_query(sql: str, user: str | None) -> dict:
    """Start an Athena query and return the execution ID.

    The browser polls ``athena_query_status`` until the query completes.
    """
    if not sql:
        raise ApiError(400, "sql is required")
    cfg = get_config(user_override=user)
    output_bucket = cfg["athena_output_bucket"] or (
        f"overture-bundle-inspector-athena-output-{cfg['environment']}"
    )
    result = s3.athena_start(output_bucket, sql)
    if "error" in result:
        raise ApiError(400, result["error"])
    return result


def _validate_query_id(query_id: str) -> None:
    if not query_id:
        raise ApiError(400, "query_id is required")
    if not QUERY_ID_PATTERN.match(query_id):
        raise ApiError(400, "invalid query_id")


def athena_query_status(query_id: str) -> dict:
    """Check the status of an Athena query execution.

    Returns state (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED). On
    SUCCEEDED, includes query_id and download_url. On FAILED/CANCELLED,
    includes error.
    """
    _validate_query_id(query_id)
    result = s3.athena_status(query_id)
    if "error" in result:
        raise ApiError(400, result["error"])
    return result


def athena_csv_proxy(query_id: str, range_header: str | None, head_only: bool) -> S3ObjectResponse:
    """Proxy (optionally ranged) requests to an Athena result CSV on S3."""
    _validate_query_id(query_id)

    try:
        location = s3.athena_output_location(query_id)
    except Exception as e:
        raise ApiError(502, "failed to look up Athena output location") from e
    if location is None:
        raise ApiError(404, "no result location found for query_id")

    output_bucket, key = location
    client = _s3_client()
    params = {"Bucket": output_bucket, "Key": key}

    if head_only:
        try:
            head_resp = client.head_object(**params)
        except Exception as e:
            raise ApiError(404, str(e)) from e
        return S3ObjectResponse(
            headers={
                "Content-Type": "text/csv",
                "Content-Length": str(head_resp["ContentLength"]),
                "Accept-Ranges": "bytes",
            },
            status=200,
        )

    if range_header:
        params["Range"] = range_header

    try:
        resp = client.get_object(**params)
    except Exception as e:
        raise ApiError(404, str(e)) from e

    headers = {
        "Content-Type": "text/csv",
        "Content-Length": str(resp["ContentLength"]),
        "Accept-Ranges": "bytes",
    }
    if "ContentRange" in resp:
        headers["Content-Range"] = resp["ContentRange"]

    return S3ObjectResponse(
        headers=headers,
        status=206 if range_header else 200,
        body=_stream_s3_body(resp["Body"]),
    )
