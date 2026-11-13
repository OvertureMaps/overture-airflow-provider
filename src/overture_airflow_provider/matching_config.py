"""Caller-supplied Corpus transport settings for matching."""

from dataclasses import dataclass

from overture_airflow_provider.config import _require_non_empty


@dataclass(frozen=True)
class CorpusDagConfig:
    """Transport contract for caller-deployed Corpus DAGs.

    Child runs are waited on and failures propagate. Every trigger has a stable
    child run ID and no automatic retries. Clearing a trigger task clears and
    reruns that same child run, so a rerun replaces the earlier attempt instead
    of adding a second one. Clearing a load therefore replays that load.

    ``create_branch=True`` triggers the create-branch DAG for every non-default
    target branch, so that DAG must succeed when the branch already exists. A
    failed creation blocks the load. When left off, the target branch must exist.

    The exports of one input or baseline selection run one at a time. To keep
    exports from different selections or instances apart, use a pool whose slot
    is held for the whole child run: triggers block a worker by default, and a
    deferred trigger (``deferrable=True``) releases its slot unless the pool
    counts deferred tasks.
    """

    export_dag_id: str
    load_dag_id: str
    export_output_task_id: str
    export_output_key: str = "return_value"
    create_branch_dag_id: str | None = None
    create_branch: bool = False
    default_branch: str = "main"
    export_format: str = "parquet"
    combine_output: bool = True
    spark_impl_name: str | None = None
    export_pool: str = "default_pool"
    load_pool: str = "default_pool"
    deferrable: bool = False

    def __post_init__(self):
        _require_non_empty(
            type(self).__name__,
            export_dag_id=self.export_dag_id,
            load_dag_id=self.load_dag_id,
            export_output_task_id=self.export_output_task_id,
            default_branch=self.default_branch,
            export_format=self.export_format,
        )
        if self.create_branch and not self.create_branch_dag_id:
            raise ValueError("create_branch requires create_branch_dag_id")
