# Operator links

## Job console link

The `execute_spark_job` task automatically gets a "Spark Job" link in the Airflow UI that opens the platform's job-run console (Glue, Databricks, or Wherobots). No configuration is needed; the link is attached automatically when the provider is installed.

## Report Issue link

`ReportIssueConfig` adds a "Report Issue" button to the execute task that opens a pre-filled GitHub issue form when a job fails. The link is push-based: the config is written to XCom at task start, so the button renders even if the run fails mid-flight.

```python
from overture_airflow_provider import ReportIssueConfig, spark_agnostic_task_group

spark_agnostic_task_group(
    "my_glue_job",
    spark_impl_name="GLUE_v5",
    report_issue_config=ReportIssueConfig(
        enabled=True,
        target="my-org/my-repo",  # GitHub owner/repo
        labels=["spark-failure"],  # optional labels pre-applied to the issue
    ),
    # ... rest of config unchanged
)
```

The button is off by default (`enabled=False`). Only `"github"` ships built in; additional trackers are pluggable via `_report_issue.IssueTracker` and `register_tracker` without touching the link or operator code.
