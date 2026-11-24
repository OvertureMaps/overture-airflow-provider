# Wherobots runtime selection

On Wherobots, `spark_cluster_desired_worker_cores` picks a general-purpose runtime (`small` through `2x-large`). To run a task group on a specific runtime instead, such as a memory-optimized one for a job whose executors run out of memory, set `spark_cluster_size_name`:

```python
spark_agnostic_task_group(
    group_id="my_spark_job",
    spark_impl_name="WHEROBOTS_v1_5_0",
    spark_cluster_desired_worker_cores="1000",  # still sizes Glue / Databricks runs
    spark_cluster_size_name="large-himem",  # Wherobots only
    # ... rest of config unchanged
)
```

`spark_cluster_size_name` takes a `ClusterSize` name (`"XS"` to `"XXL"`) or any runtime name the installed `wherobots` SDK defines, such as `"large-himem"`, `"2x-large-himem"`, or `"x-large-hicpu"`. Matching is case-insensitive, and the kwarg is templated. When set, it wins over `spark_cluster_desired_worker_cores` on Wherobots. Glue and Databricks ignore it, so the same DAG keeps running unchanged on those platforms. An unknown name fails `execute_spark_job` before anything is submitted, and the error lists the valid names. Your Wherobots organization needs access to the chosen runtime.
