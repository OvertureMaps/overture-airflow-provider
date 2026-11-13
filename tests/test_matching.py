import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from overture_airflow_provider._airflow_compat import AirflowFailException
from overture_airflow_provider._matching import (
    child_run_id,
    export_paths,
    load_uri,
    matcher_parameters,
    output_root,
    path_map,
    selection,
    table_names,
)

TABLES = ["division", "division_area"]
INPUTS = {table: f"s3://feeds/provider=linz/run=one/data/type={table}" for table in TABLES}
BASELINE = {table: f"s3://baseline/corpus/type={table}" for table in TABLES}
RESULTS = {table: f"s3://matches/run=one/final/type={table}" for table in TABLES}
CONTEXT = {"ti": SimpleNamespace(dag_id="test", run_id="run", task_id="outer.linz.requests")}


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
    # Corpus DAGs type the version as a string; snapshot IDs often arrive as ints.
    assert selection({"branch": "test", "version": 123}, TABLES, "overture")["version"] == "123"
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
    for bad in ([outputs[0]], [outputs[0], None], [outputs[0], {"data_export": []}]):
        with pytest.raises(ValueError):
            export_paths(bad, TABLES)
    # Each response must belong to the table requested at the same position.
    with pytest.raises(ValueError, match="'division' returned table 'division_area'"):
        export_paths(outputs[::-1], TABLES)


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


def test_provenance_must_report_the_root_it_published():
    from overture_airflow_provider.matching_taskgroup import _provenance_ready

    root = output_root(RESULTS)
    assert _provenance_ready.function(root, root) == root
    # None is what a skipped publishing task leaves behind.
    for published in (None, "s3://matches/run=other", {"metadata_root": root}):
        with pytest.raises(AirflowFailException, match="metadata root"):
            _provenance_ready.function(published, root)


def test_matcher_arguments_match_legacy_and_inject_places_files():
    from overture_airflow_provider.matching_spark import (
        _matcher_arguments,
        spark_matcher_task_factory,
    )

    conf = _matcher_arguments.function(
        parameters(), "data_paths.json", "s3://caller/libpostal.tar.gz"
    )
    assert conf["--outputPath"] == "s3a://matches"
    assert conf["--overrideConfigPath"] == "none"
    assert conf["--outputResultFile"] == "data_paths.json"
    assert conf["--extra-files"] == "s3://caller/libpostal.tar.gz"
    assert json.loads(conf["--baseline"]) == BASELINE
    for name in ("", "nested/data_paths.json"):
        with pytest.raises(ValueError, match="filename"):
            spark_matcher_task_factory(output_result_file=name)


def test_manifest_is_read_from_the_invocations_output_path(mocker):
    from overture_airflow_provider.matching_spark import _read_manifest

    read = mocker.patch("overture_core.cloud.aws.object.get_object_bytes")
    read.return_value = json.dumps(RESULTS).encode()
    assert _read_manifest.function(parameters(output_path="s3a://matches/linz/"), "out.json") == (
        RESULTS
    )
    read.assert_called_once_with("matches", "linz/out.json")
    read.return_value = None
    with pytest.raises(FileNotFoundError, match="s3://matches/data_paths.json"):
        _read_manifest.function(parameters(), "data_paths.json")
    with pytest.raises(ValueError, match="S3"):
        _read_manifest.function(parameters(output_path="gs://matches"), "data_paths.json")


def test_load_and_create_requests_preserve_the_corpus_contract():
    from overture_airflow_provider.matching_taskgroup import _create_requests, _load_requests

    loads = _load_requests.function(
        RESULTS, "overture", "experiment", "linz", "divisions", "30", None, True, **CONTEXT
    )
    assert [request["conf"] for request in loads] == [
        {
            "ThemeName": "divisions",
            "TableName": table,
            "BranchName": "experiment",
            "InputPath": load_uri(RESULTS[table]),
            "IdField": "id",
            "Source": "linz",
            "ExpirationPeriodDays": 30,
        }
        for table in TABLES
    ]
    baseline = {"paths": {}, "branch": "baseline", "version": "123"}
    (create,) = _create_requests.function(
        TABLES, baseline, "overture", "experiment", "divisions", "main", "GLUE_v5", True, **CONTEXT
    )
    assert create["conf"] == {
        "ThemeName": "divisions",
        "TableNames": "division,division_area",
        "BranchName": "experiment",
        "BaseBranchName": "baseline",
        "BaseVersion": "123",
        "SparkImpl": "GLUE_v5",
    }
    direct = {"paths": BASELINE, "branch": "", "version": ""}
    # No creation for the default branch, and nothing at all for a match-only run.
    for scenario, branch in (("overture", "main"), ("overture", ""), ("test", "experiment")):
        assert not _create_requests.function(
            TABLES, direct, scenario, branch, "divisions", "main", None, True, **CONTEXT
        )
    for scenario, branch in (("overture", ""), ("overture", "none"), ("test", "experiment")):
        assert not _load_requests.function(
            RESULTS, scenario, branch, "linz", "divisions", 0, None, True, **CONTEXT
        )
    with pytest.raises(AirflowFailException, match="non-negative"):
        _load_requests.function(
            RESULTS, "overture", "main", "linz", "divisions", -1, None, True, **CONTEXT
        )


def test_a_rendered_overture_scenario_cannot_load_or_create_without_provenance():
    from overture_airflow_provider.matching_taskgroup import _create_requests, _load_requests

    # The scenario can be a template, so only the run-time value can decide this.
    with pytest.raises(AirflowFailException, match="provenance_task_factory"):
        _load_requests.function(
            RESULTS, "overture", "main", "linz", "divisions", 0, None, False, **CONTEXT
        )
    baseline = {"paths": {}, "branch": "main", "version": ""}
    with pytest.raises(AirflowFailException, match="provenance_task_factory"):
        _create_requests.function(
            TABLES, baseline, "overture", "experiment", "divisions", "main", None, False, **CONTEXT
        )
    # Match-only runs never needed a publisher.
    assert not _load_requests.function(
        RESULTS, "test", "main", "linz", "divisions", 0, None, False, **CONTEXT
    )


def test_loads_wait_for_every_requested_branch_creation():
    from overture_airflow_provider.matching_taskgroup import _branch_ready

    # No creation was requested, so nothing is looked up.
    _branch_ready.function([], "create_branch", ti=Mock(spec=[]))
    ti = Mock(dag_id="parent", run_id="parent_run")
    ti.xcom_pull.return_value = "child_0"
    ti.get_task_states.return_value = {"parent_run": {"create_branch_0": "success"}}
    _branch_ready.function([{}], "create_branch", ti=ti)
    ti.get_task_states.return_value = {"parent_run": {"create_branch_0": "skipped"}}
    with pytest.raises(AirflowFailException, match="Every mapped child"):
        _branch_ready.function([{}], "create_branch", ti=ti)


def test_export_requests_cover_each_table_only_for_branch_selections():
    from overture_airflow_provider.matching_taskgroup import _export_parameters

    parsed = selection({"branch": "main", "version": 123}, TABLES, "overture")
    requests = _export_parameters.function(
        parsed, TABLES, "divisions", "parquet", True, None, **CONTEXT
    )
    assert [request["conf"] for request in requests] == [
        {
            "ThemeName": "divisions",
            "TableName": table,
            "BranchName": "main",
            "Version": "123",
            "DataFormat": "parquet",
            "CombineOutput": True,
        }
        for table in TABLES
    ]
    assert len({request["trigger_run_id"] for request in requests}) == len(TABLES)
    direct = selection({"override_paths": INPUTS}, TABLES, "overture")
    assert not _export_parameters.function(
        direct, TABLES, "divisions", "parquet", True, None, **CONTEXT
    )


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
        "outer.linz.input.export", "export_dag", "output", "return_value", TABLES, ti=ti
    )
    assert result == BASELINE
    assert [call.kwargs["run_id"] for call in get_one.call_args_list] == [
        "linz_division",
        "linz_area",
    ]
    assert [call.kwargs["map_indexes"] for call in ti.xcom_pull.call_args_list] == [0, 1]
    ti.xcom_pull.side_effect = [None, "other_provider"]
    # Contract violations are permanent: a retry cannot change the outcome.
    with pytest.raises(AirflowFailException, match="Missing"):
        _exported_paths.function(
            "outer.linz.input.export", "export_dag", "output", "return_value", TABLES, ti=ti
        )
    ti.xcom_pull.side_effect = ["linz_division", "linz_area"]
    get_one.side_effect = [
        {"data_export": [{"TableName": table, "exportPath": BASELINE[table]}]}
        for table in reversed(TABLES)
    ]
    with pytest.raises(AirflowFailException, match="returned table"):
        _exported_paths.function(
            "outer.linz.input.export", "export_dag", "output", "return_value", TABLES, ti=ti
        )


@pytest.mark.parametrize("states", [{}, {"load_0": "success", "load_1": "skipped"}])
def test_completion_requires_every_mapped_load_to_succeed(states):
    from overture_airflow_provider.matching_taskgroup import _complete

    ti = Mock(dag_id="parent", run_id="parent_run")
    ti.xcom_pull.side_effect = ["child_0", "child_1"]
    ti.get_task_states.return_value = {"parent_run": states}
    with pytest.raises(AirflowFailException, match="Every mapped child"):
        _complete.function(RESULTS, [{}, {}], "load", ti=ti)


def test_completion_distinguishes_loaded_match_only_and_missing_requests():
    from overture_airflow_provider.matching_taskgroup import _complete, _selected_paths

    ti = Mock(dag_id="parent", run_id="parent_run")
    ti.xcom_pull.side_effect = ["child_0", "child_1"]
    ti.get_task_states.return_value = {"parent_run": {"load_0": "success", "load_1": "success"}}
    assert _complete.function(RESULTS, [{}, {}], "load", ti=ti) == {
        "loaded": True,
        "result_paths": RESULTS,
        "corpus_run_ids": ["child_0", "child_1"],
    }
    assert _complete.function(RESULTS, [], "load", ti=Mock()) == {
        "loaded": False,
        "result_paths": RESULTS,
        "corpus_run_ids": [],
    }
    # None is what a skipped request task leaves behind; it must not read as match-only.
    with pytest.raises(AirflowFailException, match="missing"):
        _complete.function(RESULTS, None, "load", ti=Mock())
    ti.xcom_pull.side_effect = None
    ti.xcom_pull.return_value = None
    with pytest.raises(AirflowFailException, match="Missing"):
        _complete.function(RESULTS, [{}, {}], "outer.linz.load", ti=ti)

    direct = {"paths": INPUTS, "branch": "", "version": ""}
    export = {"paths": {}, "branch": "main", "version": ""}
    assert _selected_paths.function(direct, None) == INPUTS
    assert _selected_paths.function(export, BASELINE) == BASELINE
    with pytest.raises(AirflowFailException, match="Exactly one"):
        _selected_paths.function(export, None)
