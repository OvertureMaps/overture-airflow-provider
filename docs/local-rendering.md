# Local rendering

The `overture_airflow_provider.render` module produces the platform submission payload without importing or running any Airflow operators. Use it to drive real cloud resources from the CLI, or to snapshot-test payload shape in CI.

```bash
# Render the payload to stdout as JSON.
uv run python -m overture_airflow_provider.render \
    --spark-impl GLUE_v5 --module-name my_module --class-name MyJob

# Render to a directory and emit an executable cli.sh.
uv run python -m overture_airflow_provider.render \
    --spark-impl GLUE_v5 --module-name my_module --class-name MyJob \
    --out ./rendered/

bash ./rendered/cli.sh   # invokes aws glue create-job / start-job-run
```

You can also drive it from Python:

```python
from overture_airflow_provider import render_spark_job

result = render_spark_job(
    spark_impl_name="DATABRICKS_v15",
    module_name="my_module",
    class_name="MyJob",
    parameters={"date": "2024-01-01"},
)
print(result.submit_payload)  # equivalent to `databricks jobs submit --json`
print(result.operator_kwargs)  # what the Airflow operator would receive
result.write_to("./out/")  # dump JSON payloads + cli.sh
```

Pass `pre_resolved_package_info=` or `pre_resolved_jar_info=` with real S3 URIs from a previous `download_python_packages_*` or `download_jars_*` run to skip the `s3://.../REPLACE-ME.whl` placeholders.
