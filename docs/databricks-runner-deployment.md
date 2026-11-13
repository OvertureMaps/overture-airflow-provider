# Databricks runner deployment

A Databricks run needs two workspace assets: the runner notebook (`job_runner_databricks`) and the cluster init script (`agnostic_operator_cluster_init_databricks.sh`). Both ship with the provider.

The `setup_cluster` task uploads both on the worker at execute time, the way Glue and Wherobots runners get staged to S3. Nothing runs at DAG parse time. Each asset lands at a content-hash-keyed path under `DatabricksConfig.workspace_scripts_path_template` (after `{s3_assets_root}` substitution):

- `{scripts_path}/runners/{sha256[:12]}-job_runner_databricks`
- `{scripts_path}/runners/{sha256[:12]}-agnostic_operator_cluster_init_databricks.sh`

The run's `notebook_path` and `init_scripts` point at those paths. A `workspace get-status` check skips any path that already exists. A concurrent upload of the same hash counts as success. A new provider version writes new paths instead of overwriting files that in-flight clusters, or another environment sharing the workspace, still use.

The upload uses the same Airflow connection as the submit path (`DatabricksConfig.cluster_conf["databricks_conn_id"]`), through `DatabricksSdkHook`. All of its auth modes work: PAT, OAuth M2M, Azure service principal, and federated OIDC.

> [!IMPORTANT]
> The connection's identity needs write access to `{scripts_path}/runners`, since the task creates that folder and imports into it. The default `/Shared/{s3_assets_root}` path needs no extra grant: Databricks gives every workspace user `CAN MANAGE` on `/Shared`. A custom path outside `/Shared` needs an explicit `CAN_EDIT` or `CAN_MANAGE` grant for that identity.

## Using pre-deployed assets

Set `DatabricksConfig(stage_workspace_assets=False)` to keep deploying the assets yourself, for example when Airflow workers hold no workspace write credentials. The run then references the fixed paths `{scripts_path}/job_runner_databricks` and `{scripts_path}/{cluster_init_script_name}`, with no workspace call at setup time. Deploy them via CI/CD or the bundled helpers:

```python
from overture_airflow_provider.runner_assets import (
    upload_databricks_init_script_to_workspace,
    upload_databricks_runner_to_workspace,
)

host, token = "https://my-workspace.cloud.databricks.com", "dapi..."
upload_databricks_runner_to_workspace(host, token, "/Shared/<s3_assets_root>/job_runner_databricks")
upload_databricks_init_script_to_workspace(
    host, token, "/Shared/<s3_assets_root>/agnostic_operator_cluster_init_databricks.sh"
)
```

A non-default `DatabricksConfig.cluster_init_script_name` means you supply your own init script. The provider never stages it, even with staging on, and references it at `{scripts_path}/{cluster_init_script_name}`. The runner notebook is still staged.

A missing asset surfaces as a Databricks cluster-launch error at run time, naming the missing path.

> While a Databricks job is deferred, the Triggerer may log `aiohttp` "Unclosed client session / connector" ERROR lines. These originate in the upstream `apache-airflow-providers-databricks` `DatabricksExecutionTrigger` (its async client is not explicitly closed on the event loop), not in this provider. The task defers, polls, and resumes correctly regardless. This provider deliberately reuses the installed trigger and does not fork it to silence the message.
