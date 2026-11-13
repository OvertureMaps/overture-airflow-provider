"""Airflow-free matching payload and provenance validation."""

import fnmatch
import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import urlsplit, urlunsplit


def object_value(value: Any, label: str) -> dict:
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError(f"{label} must be a mapping or JSON object")
    return value


def table_names(tables: str | Sequence[str]) -> list[str]:
    names = tables.split(",") if isinstance(tables, str) else list(tables)
    if not names or any(not isinstance(name, str) or not name.strip() for name in names):
        raise ValueError("tables must contain non-empty table names")
    names = [name.strip() for name in names]
    if len(set(names)) != len(names):
        raise ValueError("tables must not contain duplicates")
    return names


def path_map(value: Any, tables: Sequence[str], label: str) -> dict[str, str]:
    paths = object_value(value, label)
    if set(paths) != set(tables):
        raise ValueError(f"{label} must contain exactly the required tables {list(tables)}")
    if any(not isinstance(path, str) or not path.strip() for path in paths.values()):
        raise ValueError(f"{label} contains an empty or non-string path")
    return paths


def optional_value(value: Any) -> bool:
    return value is not None and str(value).strip().lower() not in ("", "none", "null")


def selection(value: Any, tables: Sequence[str], scenario: str) -> dict:
    data = object_value(value, "matching input selection")
    branch = data.get("branch")
    paths = data.get("override_paths")
    if optional_value(branch) and paths:
        raise ValueError("Specify branch or override_paths, not both")
    if paths:
        return {"paths": path_map(paths, tables, "override_paths"), "branch": "", "version": ""}
    if not isinstance(branch, str) or not optional_value(branch):
        raise ValueError("Matching input requires branch or non-empty override_paths")
    if scenario.lower() != "overture":
        raise ValueError("Non-overture scenarios require override_paths")
    version = data.get("version")
    return {"paths": {}, "branch": branch, "version": version if optional_value(version) else ""}


def load_uri(path: str) -> str:
    if path.startswith("s3:"):
        return path.replace("s3:", "s3a:", 1)
    if path.startswith("wasbs:"):
        return path.replace("wasbs:", "abfss:", 1).replace(".blob.", ".dfs.")
    return path


def output_root(paths: Mapping[str, str]) -> str:
    roots = set()
    for path in paths.values():
        uri = urlsplit(path.rstrip("/"))
        parts = uri.path.strip("/").split("/")
        if not uri.scheme or not uri.netloc or len(parts) < 3 or uri.query or uri.fragment:
            raise ValueError(f"Cannot derive matching metadata root from {path!r}")
        roots.add(urlunsplit((uri.scheme, uri.netloc, "/" + "/".join(parts[:-2]), "", "")))
    if len(roots) != 1:
        raise ValueError("Matching results must share one metadata root")
    return roots.pop()


def matcher_parameters(
    *,
    input_paths: dict[str, str],
    baseline_paths: dict[str, str],
    scenario: str,
    theme: str,
    provider: str,
    matcher_env: str,
    output_path: str,
    run_id: str,
    override_config_path: str | None,
    embeddings_cache_table: str | None,
    match_history: str | None,
    output_debug: str | None,
    previous_metrics_path: str | None,
) -> dict[str, str]:
    for label, value in (("provider", provider), ("output_path", output_path), ("run_id", run_id)):
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{label} must be a non-empty runtime string")
    parameters = {
        "scenario": scenario,
        "theme": theme,
        "provider": provider,
        "input": json.dumps(input_paths),
        "baseline": json.dumps(baseline_paths),
        "environment": matcher_env,
        "outputPath": output_path,
        "overrideConfigPath": override_config_path if optional_value(override_config_path) else "",
        "runId": run_id,
    }
    for key, value in (
        ("embeddingsCacheTable", embeddings_cache_table),
        ("matchHistory", match_history),
        ("outputDebug", output_debug),
        ("previousMetricsPath", previous_metrics_path),
    ):
        if optional_value(value):
            parameters[key] = str(value).lower() if isinstance(value, bool) else str(value)
    return parameters


def child_run_id(dag_id: str, run_id: str, task_id: str, operation: str, conf: dict) -> str:
    identity = json.dumps([dag_id, run_id, task_id, operation, conf], sort_keys=True)
    return "matching__" + hashlib.sha256(identity.encode()).hexdigest()[:40]


def export_paths(outputs: Sequence[Any], tables: Sequence[str]) -> dict[str, str]:
    paths = {}
    for output in outputs:
        rows = object_value(output, "Corpus export response").get("data_export")
        if not isinstance(rows, list) or len(rows) != 1:
            raise ValueError("Each Corpus export must return exactly one data_export row")
        row = rows[0]
        if not isinstance(row, dict):
            raise ValueError("Invalid Corpus export row")
        table = row.get("TableName")
        if not isinstance(table, str) or table in paths:
            raise ValueError("Missing or duplicate table in Corpus export response")
        paths[table] = row.get("exportPath")
    return path_map(paths, tables, "Corpus exports")


def _check_metadata(value: Any) -> None:
    if isinstance(value, dict):
        if "_error" in value:
            raise ValueError("Provenance contains an upstream metadata error")
        for item in value.values():
            _check_metadata(item)
    elif isinstance(value, list):
        for item in value:
            _check_metadata(item)


def validate_provenance(receipt: Any, root: str, inputs: dict, baseline: dict) -> str:
    """Require published metadata plus concrete lineage for every supplied path."""
    data = object_value(receipt, "provenance receipt")
    if data.get("metadata_root") != root:
        raise ValueError("Provenance receipt metadata_root does not match matching results")
    _check_metadata(data)
    for label, expected in (("inputs", inputs), ("baselines", baseline)):
        entries = data.get(label)
        if not isinstance(entries, list) or not entries:
            raise ValueError(f"Provenance receipt requires concrete {label}")
        concrete = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"Invalid provenance {label} entry")
            path = entry.get("path")
            metadata = entry.get("metadata")
            if (
                not isinstance(path, str)
                or not path
                or any(char in path for char in "*?[")
                or not isinstance(metadata, dict)
                or not metadata
            ):
                raise ValueError(
                    f"Provenance {label} requires concrete paths and non-empty metadata"
                )
            concrete.append(load_uri(path))
        patterns = [load_uri(path) for path in expected.values()]
        if any(
            not any(fnmatch.fnmatchcase(path, pattern) for path in concrete) for pattern in patterns
        ):
            raise ValueError(f"Provenance {label} does not cover every required input path")
        if any(
            not any(fnmatch.fnmatchcase(path, pattern) for pattern in patterns) for path in concrete
        ):
            raise ValueError(f"Provenance {label} contains unrelated input paths")
    return root
