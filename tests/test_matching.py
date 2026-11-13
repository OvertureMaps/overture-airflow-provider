import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from overture_airflow_provider._matching import (
    child_run_id,
    export_paths,
    load_uri,
    matcher_parameters,
    output_root,
    path_map,
    selection,
    table_names,
    validate_provenance,
)

TABLES = ["division", "division_area"]
INPUTS = {table: f"s3://feeds/provider=linz/run=one/data/type={table}" for table in TABLES}
BASELINE = {table: f"s3://baseline/corpus/type={table}" for table in TABLES}
RESULTS = {table: f"s3://matches/run=one/final/type={table}" for table in TABLES}


def receipt(inputs=INPUTS, baseline=BASELINE):
    return {
        "metadata_root": "s3://matches/run=one",
        "inputs": [
            {"path": path, "metadata": {"source": {"version": "one"}}} for path in inputs.values()
        ],
        "baselines": [
            {"path": path, "metadata": {"corpus": {"snapshot": "123"}}}
            for path in baseline.values()
        ],
    }


def parameters(**overrides):
    return matcher_parameters(
        **{
            "input_paths": INPUTS,
            "baseline_paths": BASELINE,
            "scenario": "overture",
            "theme": "divisions",
            "provider": "linz",
            "matcher_env": "test",
            "output_path": "s3://matches",
            "run_id": "run=one",
            "override_config_path": "",
            "embeddings_cache_table": None,
            "match_history": "none",
            "output_debug": None,
            "previous_metrics_path": "",
            **overrides,
        }
    )


def test_legacy_input_forms_and_table_contract():
    for data in ({"override_paths": INPUTS}, {"override_paths": json.dumps(INPUTS)}):
        assert selection(json.dumps(data), TABLES, "overture")["paths"] == INPUTS
    assert selection({"branch": "main"}, TABLES, "overture") == {
        "branch": "main",
        "version": "",
        "paths": {},
    }
    assert selection({"branch": "test", "version": 123}, TABLES, "overture")["version"] == 123
    assert table_names("division, division_area") == TABLES


@pytest.mark.parametrize(
    "data",
    [
        {},
        {"branch": "main", "override_paths": INPUTS},
        {"override_paths": {}},
        {"override_paths": {"division": ""}},
        {"override_paths": {**INPUTS, "place": "s3://other"}},
        {"branch": False},
        [],
        "[]",
    ],
)
def test_bad_input_selection_fails(data):
    with pytest.raises((ValueError, TypeError)):
        selection(data, TABLES, "overture")


def test_non_overture_requires_direct_paths():
    with pytest.raises(ValueError, match="Non-overture"):
        selection({"branch": "main"}, TABLES, "test")
    assert selection({"override_paths": INPUTS}, TABLES, "test")["paths"] == INPUTS


@pytest.mark.parametrize("tables", ["", [], ["division", "division"], [" "]])
def test_invalid_tables(tables):
    with pytest.raises(ValueError):
        table_names(tables)


def test_matcher_parameters_preserve_places_optional_semantics():
    data = parameters(
        theme="places",
        match_history=False,
        output_debug="true",
        embeddings_cache_table="catalog.embeddings",
        previous_metrics_path="s3://metrics/old",
    )
    assert data["matchHistory"] == "false"
    assert data["outputDebug"] == "true"
    assert data["embeddingsCacheTable"] == "catalog.embeddings"
    assert data["previousMetricsPath"] == "s3://metrics/old"
    assert json.loads(data["input"]) == INPUTS
    assert parameters()["overrideConfigPath"] == ""
    assert not set(parameters()) & {"matchHistory", "outputDebug", "previousMetricsPath"}


@pytest.mark.parametrize("field", ["run_id", "provider", "output_path"])
def test_runtime_identity_is_required(field):
    with pytest.raises(ValueError, match=field):
        parameters(**{field: ""})


def test_result_paths_and_metadata_root_preserve_legacy_layout():
    assert path_map(json.dumps(RESULTS), TABLES, "results") == RESULTS
    assert output_root(RESULTS) == "s3://matches/run=one"
    with pytest.raises(ValueError, match="share one"):
        output_root({**RESULTS, "division_area": "s3://other/run=two/final/type=division_area"})
    with pytest.raises(ValueError, match="required tables"):
        path_map({}, TABLES, "results")
    with pytest.raises(ValueError, match="derive"):
        output_root({"place": "s3://bucket/one"})


def test_export_results_require_every_table_and_one_row_per_run():
    outputs = [
        json.dumps({"data_export": [{"TableName": table, "exportPath": BASELINE[table]}]})
        for table in TABLES
    ]
    assert export_paths(outputs, TABLES) == BASELINE
    for bad in ([outputs[0]], [outputs[0], outputs[0]], [None], [{"data_export": []}]):
        with pytest.raises(ValueError):
            export_paths(bad, TABLES)


def test_load_uri_compatibility_and_child_identity_isolation():
    assert load_uri("s3://bucket/path") == "s3a://bucket/path"
    assert (
        load_uri("wasbs://data@account.blob.core.windows.net/path")
        == "abfss://data@account.dfs.core.windows.net/path"
    )
    first = child_run_id("dag", "run", "outer.linz.load", "division", {"a": 1})
    assert first == child_run_id("dag", "run", "outer.linz.load", "division", {"a": 1})
    assert first != child_run_id("dag", "run", "outer.osm.load", "division", {"a": 1})
    assert first != child_run_id("dag", "run", "outer.linz.load", "division_area", {"a": 1})


def test_provenance_covers_concrete_inputs_and_expanded_wildcards():
    assert validate_provenance(receipt(), output_root(RESULTS), INPUTS, BASELINE) == output_root(
        RESULTS
    )
    wildcard = {
        table: path.replace("provider=linz", "provider=*") for table, path in INPUTS.items()
    }
    assert validate_provenance(receipt(), output_root(RESULTS), wildcard, BASELINE)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda data: data.update(metadata_root="s3://wrong/run"),
        lambda data: data.update(inputs=[]),
        lambda data: data["inputs"].pop(),
        lambda data: data["inputs"][0].update(path="s3://unrelated/data"),
        lambda data: data["inputs"][0].update(metadata={}),
        lambda data: data["inputs"][0].update(path="s3://feeds/provider=*/data"),
        lambda data: data["baselines"][0]["metadata"].update(nested={"_error": "access denied"}),
    ],
)
def test_incomplete_or_failed_provenance_cannot_release_load(mutate):
    data = deepcopy(receipt())
    mutate(data)
    with pytest.raises(ValueError):
        validate_provenance(data, output_root(RESULTS), INPUTS, BASELINE)


def test_glue_payload_matches_legacy_and_injects_places_files():
    from overture_airflow_provider._matching_glue import _glue_parameters

    conf = _glue_parameters.function(
        parameters(), "data_paths.json", "s3://caller/libpostal.tar.gz"
    )
    assert conf["--outputPath"] == "s3a://matches"
    assert conf["--overrideConfigPath"] == "none"
    assert conf["--outputResultFile"] == "data_paths.json"
    assert conf["--extra-files"] == "s3://caller/libpostal.tar.gz"
    assert json.loads(conf["--baseline"]) == BASELINE
    with pytest.raises(ValueError, match="filename"):
        _glue_parameters.function(parameters(), "", "")


def test_glue_manifest_reads_the_invocations_explicit_path(mocker):
    from io import BytesIO

    from overture_airflow_provider._matching_glue import _read_manifest

    client = mocker.patch("boto3.client").return_value
    client.get_object.return_value = {"Body": BytesIO(json.dumps(RESULTS).encode())}
    assert _read_manifest.function(parameters(), "data_paths.json") == RESULTS
    client.get_object.assert_called_once_with(Bucket="matches", Key="data_paths.json")
    client.get_object.side_effect = PermissionError("denied")
    with pytest.raises(PermissionError):
        _read_manifest.function(parameters(), "data_paths.json")


def test_load_plan_preserves_corpus_contract_and_does_not_infer_spark_platform():
    from overture_airflow_provider.matching_taskgroup import _load_plan

    context = {"ti": SimpleNamespace(dag_id="test", run_id="run", task_id="outer.linz.plan")}
    plan = _load_plan.function(
        RESULTS,
        {"branch": "baseline", "version": 123},
        "overture",
        "experiment",
        "linz",
        "divisions",
        "30",
        "main",
        True,
        None,
        **context,
    )
    assert plan["load"] and plan["create"]
    assert plan["create_conf"]["BaseVersion"] == 123
    assert plan["create_conf"]["BaseBranchName"] == "baseline"
    assert plan["loads"][0]["conf"] == {
        "ThemeName": "divisions",
        "TableName": "division",
        "BranchName": "experiment",
        "InputPath": load_uri(RESULTS["division"]),
        "IdField": "id",
        "Source": "linz",
        "ExpirationPeriodDays": 30,
    }
    no_load = _load_plan.function(
        RESULTS,
        {"branch": "", "version": ""},
        "overture",
        "",
        "linz",
        "divisions",
        0,
        "main",
        False,
        "GLUE_v5",
        **context,
    )
    assert no_load["loads"] == [] and no_load["load"] is False


def test_child_exports_read_only_exact_mapped_runs(mocker):
    from overture_airflow_provider.matching_taskgroup import _exported_paths

    ti = Mock(dag_id="parent", run_id="parent_run")
    ti.xcom_pull.side_effect = ["linz_division", "linz_area"]
    ti.get_task_states.return_value = {
        "parent_run": {
            "outer.linz.input.export_0": "success",
            "outer.linz.input.export_1": "success",
        }
    }
    get_one = mocker.patch("overture_airflow_provider.matching_taskgroup.XCom.get_one")
    get_one.side_effect = [
        {"data_export": [{"TableName": table, "exportPath": BASELINE[table]}]} for table in TABLES
    ]
    result = _exported_paths.function(
        "outer.linz.input.export", "export_dag", "output", "return_value", TABLES, [{}, {}], ti=ti
    )
    assert result == BASELINE
    assert [call.kwargs["run_id"] for call in get_one.call_args_list] == [
        "linz_division",
        "linz_area",
    ]
    assert [call.kwargs["map_indexes"] for call in ti.xcom_pull.call_args_list] == [0, 1]
    ti.xcom_pull.side_effect = [None, "other_provider"]
    with pytest.raises(ValueError, match="Missing"):
        _exported_paths.function(
            "outer.linz.input.export",
            "export_dag",
            "output",
            "return_value",
            TABLES,
            [{}, {}],
            ti=ti,
        )
    ti.xcom_pull.side_effect = ["linz_division", "linz_area"]
    get_one.side_effect = [
        {"data_export": [{"TableName": table, "exportPath": BASELINE[table]}]}
        for table in reversed(TABLES)
    ]
    with pytest.raises(ValueError, match="required tables"):
        _exported_paths.function(
            "outer.linz.input.export",
            "export_dag",
            "output",
            "return_value",
            TABLES,
            [{}, {}],
            ti=ti,
        )


@pytest.mark.parametrize("states", [{}, {"load_0": "success", "load_1": "skipped"}])
def test_load_receipts_cannot_override_missing_or_skipped_task_states(states):
    from overture_airflow_provider.matching_taskgroup import _loaded

    ti = Mock(dag_id="parent", run_id="parent_run")
    ti.xcom_pull.side_effect = ["child_0", "child_1"]
    ti.get_task_states.return_value = {"parent_run": states}
    with pytest.raises(ValueError, match="Every mapped child"):
        _loaded.function({"loads": [{}, {}], "result_paths": RESULTS}, "load", ti=ti)


def test_missing_load_receipts_and_ambiguous_completion_fail():
    from overture_airflow_provider.matching_taskgroup import _complete, _loaded

    ti = Mock()
    ti.xcom_pull.return_value = None
    with pytest.raises(ValueError, match="Missing"):
        _loaded.function({"loads": [{}, {}], "result_paths": RESULTS}, "outer.linz.load", ti=ti)
    for loaded, skipped in ((None, None), ({}, {})):
        with pytest.raises(ValueError):
            _complete.function(loaded, skipped)
    assert _complete.function(None, {"loaded": False}) == {"loaded": False}
