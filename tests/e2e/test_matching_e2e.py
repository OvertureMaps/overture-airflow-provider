"""Execute real Airflow branches and mapping with credential-free remote adapters."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from overture_airflow_provider import CorpusDagConfig
from overture_airflow_provider._airflow_compat import DAG, TaskGroup, TriggerRule, task
from overture_airflow_provider.matching_taskgroup import matching_task_group

pytestmark = pytest.mark.e2e
TABLES = ("division", "division_area")


@task
def publish_provenance(input_paths, baseline_paths, result_paths, metadata_root, parameters):
    return metadata_root


@task.short_circuit(ignore_downstream_trigger_rules=False)
def changed(selected):
    return selected


@task
def finalized(completion):
    return completion


@task
def fake_match(parameters, failure):
    if failure == "matcher":
        raise ValueError("simulated matcher failure")
    if failure == "matcher_skip":
        raise AirflowSkipException("simulated unexpected matcher skip")
    root = parameters["outputPath"] + "/" + parameters["runId"]
    return {table: f"{root}/final/type={table}" for table in TABLES}


@task
def check_inputs(input_paths, baseline_paths, failure):
    if failure == "pre_match_skip":
        raise AirflowSkipException("simulated unexpected pre-match skip")
    return True


@pytest.mark.parametrize(
    "failure,selected,branch",
    [
        ("", True, "main"),
        ("", False, "main"),
        ("", "osm", "main"),
        ("", True, "experiment"),
        ("load", True, "main"),
        ("load_skip", True, "main"),
        ("export_skip", True, "main"),
        ("provenance", True, "main"),
        ("provenance_skip", True, "main"),
        ("matcher", True, "main"),
        ("matcher_skip", True, "main"),
        ("pre_match_skip", True, "main"),
        ("create", True, "experiment"),
        ("", True, ""),
    ],
)
def test_matching_execution_states(monkeypatch, failure, selected, branch):
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

    import overture_airflow_provider.matching_taskgroup as matching

    remote = {}
    export_versions = set()
    creates = []
    loads = []

    def execute_trigger(self, context):
        conf = self.conf
        if self.trigger_dag_id == "simulated_export":
            export_versions.add(conf["Version"])
            remote[self.trigger_run_id] = {
                "data_export": [
                    {
                        "TableName": conf["TableName"],
                        "exportPath": f"s3://baseline/{conf['BranchName']}/type={conf['TableName']}",
                    }
                ]
            }
        elif self.trigger_dag_id == "simulated_load":
            loads.append((self.task_id, conf["TableName"], conf["Source"]))
            if failure == "load" and conf["TableName"] == "division_area":
                raise ValueError("simulated mapped load failure")
        else:
            creates.append((conf["BranchName"], conf["BaseBranchName"], conf["BaseVersion"]))
            if failure == "create":
                raise ValueError("simulated branch creation failure")
        context["ti"].xcom_push(key="trigger_run_id", value=self.trigger_run_id)
        if conf.get("TableName") == "division_area" and (
            (failure == "load_skip" and self.trigger_dag_id == "simulated_load")
            or (failure == "export_skip" and self.trigger_dag_id == "simulated_export")
        ):
            raise AirflowSkipException("simulated unexpected mapped trigger skip")

    monkeypatch.setattr(TriggerDagRunOperator, "execute", execute_trigger)
    monkeypatch.setattr(
        matching, "XCom", SimpleNamespace(get_one=lambda **kwargs: remote[kwargs["run_id"]])
    )

    def provenance_factory(**kwargs):
        result = publish_provenance(**kwargs)
        if failure in ("provenance", "provenance_skip"):

            @task
            def rejected(receipt):
                if failure == "provenance_skip":
                    raise AirflowSkipException("simulated unexpected provenance skip")
                raise ValueError("simulated provenance publication failure")

            return rejected(result)
        return result

    dag_id = f"matching_states_{failure or 'ok'}_{selected}_{branch or 'no_load'}"
    with DAG(
        dag_id,
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
        render_template_as_native_obj=True,
    ) as dag:
        completions = []
        with TaskGroup("providers"):
            for provider in ("osm", "linz"):
                with TaskGroup(provider):
                    gate = changed(selected is True or selected == provider)
                    result = matching_task_group(
                        group_id="arbitrary",
                        scenario="overture",
                        theme="divisions",
                        provider=provider,
                        input={
                            "override_paths": {
                                table: f"s3://feeds/{provider}/data/type={table}"
                                for table in TABLES
                            }
                        },
                        baseline={"branch": "main", "version": 123},
                        output_path=f"s3://matches/{provider}",
                        run_id="{{ run_id }}",
                        tables=TABLES,
                        matcher_env="test",
                        matcher_task_factory=lambda *, parameters: fake_match(parameters, failure),
                        corpus=CorpusDagConfig(
                            export_dag_id="simulated_export",
                            load_dag_id="simulated_load",
                            export_output_task_id="output",
                            create_branch_dag_id="simulated_create",
                            create_branch=True,
                        ),
                        output_corpus_branch=branch,
                        provenance_task_factory=provenance_factory,
                        pre_match_task_factory=lambda **kwargs: check_inputs(
                            **kwargs, failure=failure
                        ),
                    )
                    gate >> result.group
                    completions.append(finalized(result.complete))
        ingest_done = EmptyOperator(task_id="ingest_done", trigger_rule=TriggerRule.NONE_FAILED)
        completions >> ingest_done

    from airflow.dag_processing.bundles.manager import DagBundlesManager
    from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db

    DagBundlesManager().sync_bundles_to_db()
    bag = DagBag(collect_dags=False)
    bag.bag_dag(dag)
    sync_bag_to_db(bag, "dags-folder", None)
    run = dag.test()
    states = {(ti.task_id, ti.map_index): ti.state for ti in run.get_task_instances()}
    expected = "failed" if selected and failure else "success"
    assert run.state == expected
    for provider in ("osm", "linz"):
        complete_id = f"providers.{provider}.arbitrary.load_results._complete"
        final_id = f"providers.{provider}.finalized"
        if not (selected is True or selected == provider):
            assert all(
                state == "skipped"
                for (task_id, _), state in states.items()
                if task_id.startswith(f"providers.{provider}.arbitrary.")
            )
            assert states[(complete_id, -1)] == "skipped"
            assert states[(final_id, -1)] == "skipped"
        elif failure:
            assert states[(complete_id, -1)] != "success"
            assert states[(final_id, -1)] != "success"
        else:
            assert states[(complete_id, -1)] == "success"
            assert states[(final_id, -1)] == "success"
    if selected and failure in ("", "load", "load_skip") and branch:
        assert {(source, table) for _, table, source in loads} == {
            (provider, table)
            for provider in ("osm", "linz")
            if selected is True or selected == provider
            for table in TABLES
        }
        assert all(f"providers.{source}." in task_id for task_id, _, source in loads)
    else:
        assert not loads
    # The DAG renders natively and the caller passed an int; Corpus DAGs need a string.
    assert export_versions <= {"123"}
    expected_creates = 2 if selected and branch == "experiment" and failure in ("", "create") else 0
    assert sorted(creates) == [("experiment", "main", "123")] * expected_creates
