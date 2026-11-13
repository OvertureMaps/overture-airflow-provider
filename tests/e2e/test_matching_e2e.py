"""Run matching groups through real Airflow scheduling, with simulated child DAG runs."""

from datetime import UTC, datetime

import pytest
from airflow.sdk.exceptions import AirflowSkipException

from overture_airflow_provider import child_dag_run_task_group, matching_task_group
from overture_airflow_provider._airflow_compat import (
    DAG,
    EmptyOperator,
    TaskGroup,
    TriggerDagRunOperator,
    TriggerRule,
    task,
)

pytestmark = pytest.mark.e2e
TABLES = ("division", "division_area")
PROVIDERS = ("osm", "linz")


@task.short_circuit(ignore_downstream_trigger_rules=False)
def changed(selected):
    return selected


@task
def check_inputs(input_paths, baseline_paths, failure):
    if failure == "pre_match":
        raise ValueError("simulated pre-match failure")
    return True


@task
def fake_match(input_paths, baseline_paths, provider, failure):
    if failure == "matcher":
        raise ValueError("simulated matcher failure")
    if failure == "matcher_skip":
        raise AirflowSkipException("simulated matcher skip")
    return {table: f"s3://matches/{provider}/type={table}" for table in TABLES}


@task
def publish(input_paths, baseline_paths, result_paths, failure):
    if failure == "provenance":
        raise ValueError("simulated provenance failure")
    return "published"


@task
def load_requests(provider, result_paths, load):
    if not load:
        return []
    return [
        {"table": table, "path": path, "source": provider, "version": "123"}
        for table, path in result_paths.items()
    ]


@task
def finalized(completion):
    return completion


@pytest.mark.parametrize(
    "failure,selected,load",
    [
        ("", True, True),
        ("", False, True),
        ("", "osm", True),
        ("", True, False),
        ("pre_match", True, True),
        ("matcher", True, True),
        ("matcher_skip", True, True),
        ("provenance", True, True),
        ("load", True, True),
        ("load_skip", True, True),
    ],
)
def test_matching_execution_states(monkeypatch, failure, selected, load):
    loads = []
    versions = set()

    def execute_trigger(self, context):
        conf = self.conf
        if failure == "load_skip" and conf["table"] == "division_area":
            raise AirflowSkipException("simulated trigger skip, before the run ID is pushed")
        context["ti"].xcom_push(key="trigger_run_id", value=self.trigger_run_id)
        loads.append((self.task_id, conf["source"], conf["table"], self.trigger_run_id))
        versions.add(conf["version"])
        if failure == "load" and conf["table"] == "division_area":
            raise ValueError("simulated child run failure")

    monkeypatch.setattr(TriggerDagRunOperator, "execute", execute_trigger)

    def sink_factory(provider):
        def build(*, result_paths):
            return child_dag_run_task_group(
                "load",
                trigger_dag_id="simulated_load",
                requests=load_requests(provider, result_paths, load),
            ).run_ids

        return build

    with DAG(
        f"matching_states_{failure or 'ok'}_{selected}_{load}",
        schedule=None,
        start_date=datetime(2026, 1, 1, tzinfo=UTC),
        default_args={"retries": 0},
        render_template_as_native_obj=True,
    ) as dag:
        completions = []
        with TaskGroup("providers"):
            for provider in PROVIDERS:
                with TaskGroup(provider):
                    gate = changed(selected is True or selected == provider)
                    result = matching_task_group(
                        group_id="arbitrary",
                        tables=TABLES,
                        input={table: f"s3://feeds/{provider}/type={table}" for table in TABLES},
                        baseline={table: f"s3://baseline/type={table}" for table in TABLES},
                        pre_match_task_factory=lambda **paths: check_inputs(
                            **paths, failure=failure
                        ),
                        matcher_task_factory=lambda provider=provider, **paths: fake_match(
                            **paths, provider=provider, failure=failure
                        ),
                        provenance_task_factory=lambda **paths: publish(**paths, failure=failure),
                        sink_task_factory=sink_factory(provider),
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

    # A skip stops the steps after it without failing the run; it is never a completion.
    assert run.state == (
        "failed" if selected and failure and failure != "matcher_skip" else "success"
    )
    for provider in PROVIDERS:
        prefix = f"providers.{provider}.arbitrary."
        complete = states[(prefix + "sink.load.run_ids", -1)]
        final = states[(f"providers.{provider}.finalized", -1)]
        if not (selected is True or selected == provider):
            assert all(
                state == "skipped"
                for (task_id, _), state in states.items()
                if task_id.startswith(prefix)
            )
            assert final == "skipped"
        elif failure:
            assert complete != "success" and final != "success"
        else:
            assert complete == "success" and final == "success"
    if failure in ("pre_match", "matcher", "matcher_skip"):
        assert all(
            states[(f"providers.{provider}.arbitrary.result_paths", -1)] != "success"
            for provider in PROVIDERS
        )

    loaded = selected and load and failure in ("", "load", "load_skip")
    expected = {
        (provider, table)
        for provider in PROVIDERS
        if loaded and (selected is True or selected == provider)
        for table in TABLES
        if not (failure == "load_skip" and table == "division_area")
    }
    assert {(source, table) for _, source, table, _ in loads} == expected
    assert all(task_id.startswith(f"providers.{source}.") for task_id, source, _, _ in loads)
    # The DAG renders natively; a mapped conf must still arrive as the request built it.
    assert versions <= {"123"}
    run_ids = [run_id for *_, run_id in loads]
    assert len(set(run_ids)) == len(run_ids)
    assert all(run_id.startswith("child__") for run_id in run_ids)
