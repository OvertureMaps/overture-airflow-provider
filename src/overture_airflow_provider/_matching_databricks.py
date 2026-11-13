"""Existing-job Databricks matching adapter; the optional hook is loaded at execution."""

import hashlib
import json
import time

from overture_airflow_provider._airflow_compat import AirflowException, BaseOperator


class DatabricksMatchingOperator(BaseOperator):
    template_fields = ("parameters", "databricks_conn_id", "job_name")

    def __init__(
        self,
        *,
        parameters,
        databricks_conn_id: str,
        job_name: str,
        output_task_key_contains: str = "assign",
        timeout_seconds: int = 43200,
        poll_interval_seconds: int = 30,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.parameters = parameters
        self.databricks_conn_id = databricks_conn_id
        self.job_name = job_name
        self.output_task_key_contains = output_task_key_contains
        self.timeout_seconds = timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds

    def execute(self, context):
        from airflow.providers.databricks.hooks.databricks import DatabricksHook

        hook = DatabricksHook(self.databricks_conn_id)
        job_id = hook.find_job_id_by_name(self.job_name)
        if job_id is None:
            raise AirflowException(f"Databricks matching job {self.job_name!r} does not exist")
        ti = context["ti"]
        identity = json.dumps([ti.dag_id, ti.run_id, self.task_id, self.parameters], sort_keys=True)
        run_id = hook.run_now(
            json={
                "job_id": job_id,
                "job_parameters": self.parameters,
                "idempotency_token": hashlib.sha256(identity.encode()).hexdigest(),
            }
        )
        ti.xcom_push(key="databricks_run_id", value=run_id)
        self.log.info("Waiting for Databricks matching run %s", run_id)
        deadline = time.monotonic() + self.timeout_seconds
        while True:
            state = hook.get_run_state(run_id)
            if state.is_terminal:
                if not state.is_successful:
                    raise AirflowException(f"Databricks matching run {run_id} failed: {state}")
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AirflowException(f"Databricks matching run {run_id} timed out")
            time.sleep(min(self.poll_interval_seconds, remaining))

        tasks = [
            item
            for item in hook.get_run(run_id)["tasks"]
            if self.output_task_key_contains.lower() in item["task_key"].lower()
        ]
        if len(tasks) != 1:
            raise AirflowException("Expected exactly one matching Databricks output task")
        output = hook.get_run_output(tasks[0]["run_id"])["notebook_output"]["result"]
        if isinstance(output, str):
            output = json.loads(output)
        return output["resultDataPaths"]
