"""S3 listing, filtering, and path parsing for the bundle inspector."""

import io
import json
import logging
import re

logger = logging.getLogger(__name__)

# Update when pipeline stages change. See STAGE_HIERARCHY below for valid stages.
KNOWN_STAGES = ["theme_promote", "theme_stage", "release_candidate"]

STAGE_HIERARCHY = {
    "theme_promote": ["theme", "schema", "run"],
    "theme_stage": ["theme", "run"],
    "release_candidate": ["release", "run"],
}
DEFAULT_HIERARCHY = ["theme", "schema", "run"]


def get_depth(prefix: str) -> int:
    """Return the depth of a prefix in the bundle hierarchy.

    Depth 0 is root (empty prefix). Each trailing-slash segment adds one.
    """
    if not prefix:
        return 0
    return prefix.rstrip("/").count("/") + 1


def hierarchy_for_stage(stage: str) -> list[str]:
    """Return the hierarchy levels for a stage."""
    return STAGE_HIERARCHY.get(stage, DEFAULT_HIERARCHY)


def run_depth_for_stage(stage: str) -> int:
    """Return the depth at which run-level nodes appear for a stage.

    The hierarchy lists levels under the stage, so the run-level depth is
    1 (for the stage itself) + index of "run" + 1.
    """
    h = hierarchy_for_stage(stage)
    try:
        return h.index("run") + 2  # +1 for stage prefix, +1 for 1-based depth
    except ValueError:
        return len(h) + 1


def _pattern_for_level(level_name: str) -> re.Pattern:
    """Return a regex pattern matching a directory for a hierarchy level.

    Levels prefixed with ``!`` match bare directory names (no key=value).
    theme_stage uses bare theme dirs until it is standardized.
    """
    if level_name.startswith("!"):
        # Bare directory: match anything that is NOT a Hive partition
        return re.compile(r"^[^=]+/$")
    return re.compile(rf"^{re.escape(level_name)}=.+/$")


# Components to show at the run level for each stage.
# None means show everything (no filtering).
STAGE_COMPONENTS = {
    "theme_stage": {"data/", "success", "metadata.json", "summary.md"},
}


def filter_bundle_level(items: list[dict], stage: str, depth: int) -> list[dict]:
    """Filter listing results to match expected bundle hierarchy patterns.

    Uses the stage's hierarchy to determine what pattern to expect at each
    relative depth (0-indexed from the first level inside the stage).
    At component level (depth == len(hierarchy)), applies STAGE_COMPONENTS
    allowlist if one exists for the stage.
    Negative depths pass everything through.
    """
    h = hierarchy_for_stage(stage)
    if depth < 0:
        return items
    if depth >= len(h):
        allowed = STAGE_COMPONENTS.get(stage)
        if allowed is not None:
            return [item for item in items if item["name"] in allowed]
        return items
    pattern = _pattern_for_level(h[depth])
    return [item for item in items if pattern.match(item["name"])]


def list_prefix(client, bucket: str, prefix: str) -> list[dict]:
    """List immediate children at an S3 prefix.

    Args:
        client: boto3 S3 client
        bucket: S3 bucket name
        prefix: Full S3 prefix (e.g. "adam/theme_promote/"). Child names
            are returned relative to this prefix.

    Returns:
        List of dicts with name, type, and optionally size/last_modified.
    """
    paginator = client.get_paginator("list_objects_v2")
    items = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/"):
        for cp in page.get("CommonPrefixes", []):
            full = cp["Prefix"]
            name = full[len(prefix) :]
            items.append({"name": name, "type": "directory"})

        for obj in page.get("Contents", []):
            key = obj["Key"]
            name = key[len(prefix) :]
            if not name:
                continue
            items.append(
                {
                    "name": name,
                    "type": "file",
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                }
            )

    return items


def list_users(client, bucket: str) -> list[str]:
    """List top-level prefixes in the bucket (user namespaces in dev)."""
    paginator = client.get_paginator("list_objects_v2")
    users = []
    for page in paginator.paginate(Bucket=bucket, Prefix="", Delimiter="/"):
        users.extend(cp["Prefix"].rstrip("/") for cp in page.get("CommonPrefixes", []))
    return users


def count_objects(client, bucket: str, prefix: str) -> int:
    """Count objects under a prefix (non-recursive via pagination)."""
    paginator = client.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        total += page.get("KeyCount", 0)
    return total


def list_tree(
    client,
    bucket: str,
    prefix: str,
    max_keys: int = 5000,
    max_files_per_dir: int = 3,
) -> dict:
    """Build an abridged directory tree under *prefix*.

    Lists up to *max_keys* objects (no delimiter), groups them into a
    nested tree, and truncates each directory to *max_files_per_dir*
    leaf files. Directories that had files omitted include an
    ``"_omitted"`` count.

    Returns a dict of ``{"dirs": {...}, "files": [...], "_omitted": N}``
    where ``dirs`` maps directory names to nested dicts of the same shape.
    """
    paginator = client.get_paginator("list_objects_v2")
    keys: list[str] = []

    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"MaxItems": max_keys}
    ):
        for obj in page.get("Contents", []):
            rel = obj["Key"][len(prefix) :]
            if rel:
                keys.append(rel)

    root: dict = {"dirs": {}, "files": []}

    for key in keys:
        parts = key.split("/")
        node = root
        for part in parts[:-1]:
            if part not in node["dirs"]:
                node["dirs"][part] = {"dirs": {}, "files": []}
            node = node["dirs"][part]
        filename = parts[-1]
        if filename:
            node["files"].append(filename)

    def _trim(node: dict) -> dict:
        trimmed_dirs = {}
        for name in sorted(node["dirs"]):
            trimmed_dirs[name] = _trim(node["dirs"][name])
        total_files = len(node["files"])
        kept = sorted(node["files"])[:max_files_per_dir]
        result: dict = {"dirs": trimmed_dirs, "files": kept}
        omitted = total_files - len(kept)
        if omitted > 0:
            result["_omitted"] = omitted
        return result

    trimmed = _trim(root)
    trimmed["_total_keys"] = len(keys)
    trimmed["_truncated"] = len(keys) >= max_keys
    return trimmed


# -- Parquet / GeoJSON loading ------------------------------------------------


def _wkb_to_geojson(wkb: bytes) -> dict | None:
    """Convert WKB bytes to a GeoJSON geometry dict via shapely."""
    if not wkb:
        return None
    try:
        from shapely import wkb as shapely_wkb
        from shapely.geometry import mapping

        geom = shapely_wkb.loads(wkb)
        return mapping(geom)
    except Exception:
        logger.warning("Failed to parse WKB geometry (%d bytes)", len(wkb))
        return None


def load_parquet_geojson(client, bucket: str, prefix: str) -> dict:
    """Sample the first parquet file under a prefix and return a GeoJSON FeatureCollection.

    Reads only the first discovered parquet file (up to MAX_ROWS rows).
    Handles two formats:
    - GeoParquet with a WKB geometry column (detected via Arrow metadata)
    - Plain columns named xmin/ymin/xmax/ymax (converted to bbox polygons)
    """
    # Recursive listing (no delimiter) to find parquet files at any depth
    paginator = client.get_paginator("list_objects_v2")
    parquet_keys = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                parquet_keys.append(obj["Key"])
    if not parquet_keys:
        return {"type": "FeatureCollection", "features": []}

    import pyarrow.parquet as pq

    MAX_ROWS = 10_000

    key = parquet_keys[0]
    response = client.get_object(Bucket=bucket, Key=key)
    table = pq.read_table(io.BytesIO(response["Body"].read()))
    if len(table) > MAX_ROWS:
        logger.warning("Parquet %s has %d rows, truncating to %d", key, len(table), MAX_ROWS)
        table = table.slice(0, MAX_ROWS)

    # Detect geometry column from GeoParquet metadata
    geo_col = None
    arrow_meta = table.schema.metadata or {}
    geo_meta = arrow_meta.get(b"geo")
    if geo_meta:
        geo_col = json.loads(geo_meta).get("primary_column", "geometry")
    elif "geometry" in table.column_names:
        geo_col = "geometry"

    # Convert via to_pylist() so values are native Python types (JSON-serializable)
    data = {c: table.column(c).to_pylist() for c in table.column_names}

    features = []

    if geo_col and geo_col in table.column_names:
        geo_values = data.pop(geo_col)
        prop_cols = list(data.keys())
        for i, wkb in enumerate(geo_values):
            geom = _wkb_to_geojson(wkb)
            props = {c: data[c][i] for c in prop_cols}
            features.append({"type": "Feature", "geometry": geom, "properties": props})
    elif all(c in data for c in ("xmin", "ymin", "xmax", "ymax")):
        bbox_cols = {"xmin", "ymin", "xmax", "ymax"}
        prop_cols = [c for c in data if c not in bbox_cols]
        for i in range(len(table)):
            xmin, ymin = data["xmin"][i], data["ymin"][i]
            xmax, ymax = data["xmax"][i], data["ymax"][i]
            props = {c: data[c][i] for c in prop_cols}
            features.append(
                {
                    "type": "Feature",
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [
                            [
                                [xmin, ymin],
                                [xmax, ymin],
                                [xmax, ymax],
                                [xmin, ymax],
                                [xmin, ymin],
                            ]
                        ],
                    },
                    "properties": props,
                }
            )

    return {"type": "FeatureCollection", "features": features}


def parquet_stats(client, bucket: str, prefix: str) -> dict:
    """Compute file stats and read schema from parquet files under a prefix.

    Returns dict with file_count, total_size, min_size, max_size, avg_size,
    and schema (list of {name, type} dicts from the first file's Arrow schema).
    """
    paginator = client.get_paginator("list_objects_v2")
    sizes = []
    first_key = None
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                sizes.append(obj["Size"])
                if first_key is None:
                    first_key = obj["Key"]

    if not sizes:
        return {"file_count": 0}

    schema_fields = []
    if first_key:
        try:
            import pyarrow.parquet as pq

            # Read only the Parquet footer via boto3 (avoids PyArrow's S3
            # filesystem which doesn't inherit boto3's credentials).
            # Footer layout: [...data...][footer thrift][footer_len 4B LE]["PAR1" 4B]
            file_size = client.head_object(Bucket=bucket, Key=first_key)["ContentLength"]
            trailer = client.get_object(
                Bucket=bucket,
                Key=first_key,
                Range=f"bytes={file_size - 8}-{file_size - 1}",
            )["Body"].read()
            footer_len = int.from_bytes(trailer[:4], "little")
            footer_start = file_size - footer_len - 8
            footer_bytes = client.get_object(
                Bucket=bucket,
                Key=first_key,
                Range=f"bytes={footer_start}-{file_size - 1}",
            )["Body"].read()
            # BytesIO containing only the footer + trailer: pq.read_metadata
            # reads offsets from the end, so this works without the full file.
            metadata = pq.read_metadata(io.BytesIO(footer_bytes))
            schema = metadata.schema.to_arrow_schema()
            for field in schema:
                schema_fields.append({"name": field.name, "type": str(field.type)})
        except Exception as e:
            logger.warning("Failed to read parquet schema from %s: %s", first_key, e)

    return {
        "file_count": len(sizes),
        "total_size": sum(sizes),
        "min_size": min(sizes),
        "max_size": max(sizes),
        "avg_size": sum(sizes) // len(sizes),
        "schema": schema_fields,
    }


def find_file_key(client, bucket: str, prefix: str, suffix: str) -> str | None:
    """Find the first file matching suffix under prefix and return its S3 key."""
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(suffix):
                return obj["Key"]
    return None


def presign_file(client, bucket: str, prefix: str, suffix: str, expires: int = 3600) -> str | None:
    """Find a file matching suffix under prefix and return a presigned GET URL."""
    key = find_file_key(client, bucket, prefix, suffix)
    if key:
        return client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires,
        )
    return None


# -- Athena query --------------------------------------------------------


def _parse_s3_uri(uri: str) -> tuple[str, str]:
    """Split an ``s3://bucket/key`` URI into a ``(bucket, key)`` tuple."""
    if not uri.startswith("s3://"):
        raise ValueError(f"not an s3:// URI: {uri!r}")
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    if not bucket or not key:
        raise ValueError(f"malformed s3:// URI: {uri!r}")
    return bucket, key


def athena_start(output_bucket: str, sql: str) -> dict:
    """Start an Athena query and return the execution ID.

    Returns:
        Dict with "query_id" on success, or "error" on failure.
    """
    import boto3

    output_location = f"s3://{output_bucket}/athena_results/"
    client = boto3.client("athena")

    try:
        resp = client.start_query_execution(
            QueryString=sql,
            ResultConfiguration={"OutputLocation": output_location},
        )
    except Exception as e:
        return {"error": str(e)}

    return {"query_id": resp["QueryExecutionId"]}


def athena_output_location(query_id: str) -> tuple[str, str] | None:
    """Look up the actual (bucket, key) of a query's result CSV.

    Reads ``QueryExecution.ResultConfiguration.OutputLocation`` from
    GetQueryExecution rather than assuming a path, since a workgroup with
    enforced configuration silently redirects results to its own configured
    location regardless of what the query requested.

    Returns:
        ``(bucket, key)``, or ``None`` if the query execution has no
        recorded output location.
    """
    import boto3

    client = boto3.client("athena")
    resp = client.get_query_execution(QueryExecutionId=query_id)
    location = resp["QueryExecution"].get("ResultConfiguration", {}).get("OutputLocation")
    if not location:
        return None
    return _parse_s3_uri(location)


def athena_status(query_id: str) -> dict:
    """Check the status of an Athena query execution.

    Returns:
        Dict with "state" (QUEUED, RUNNING, SUCCEEDED, FAILED, CANCELLED)
        and, on completion, "download_url" or "error".
    """
    import boto3

    client = boto3.client("athena")

    try:
        resp = client.get_query_execution(QueryExecutionId=query_id)
    except Exception as e:
        return {"state": "FAILED", "error": str(e)}

    status = resp["QueryExecution"]["Status"]
    query_state = status["State"]

    if query_state == "SUCCEEDED":
        location = resp["QueryExecution"].get("ResultConfiguration", {}).get("OutputLocation")
        if not location:
            return {
                "state": "FAILED",
                "error": "Query succeeded but has no recorded result location",
            }
        try:
            bucket, key = _parse_s3_uri(location)
        except ValueError as e:
            return {"state": "FAILED", "error": str(e)}
        s3 = boto3.client("s3")
        download_url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=3600,
        )
        return {
            "state": query_state,
            "query_id": query_id,
            "download_url": download_url,
        }

    if query_state in ("FAILED", "CANCELLED"):
        reason = status.get("StateChangeReason", query_state)
        return {"state": query_state, "error": reason}

    return {"state": query_state}
