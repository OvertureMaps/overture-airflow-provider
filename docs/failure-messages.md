# Failure messages

When a Spark job fails, the provider emits a classified error instead of a raw platform exception. Every failure includes a category, a reason, an optional root-cause tail, an actionable hint, and a direct console link where available:

```
Spark job FAILED on GLUE (downstream job error, not a provider/submit fault).
  run:     jr_abc1234567890      state: FAILED
  reason:  Job run failed with exit code 1
  cause:   java.lang.OutOfMemoryError: Java heap space
  hint:    OOM: increase worker size/cores or reduce partition size.
  console: https://us-east-1.console.aws.amazon.com/glue/home#/etl/jobs/run/details/jr_abc1234567890
```

Classifications: `downstream-job`, `submit/config`, `trigger/polling`, `platform/infra`, `platform/capacity`.

The hint layer scans the reason and root-cause text for known patterns (IAM denials, auth errors, OOM, throttling, missing resources, Glue capacity rejections) and appends an actionable message automatically.

## Retries

`execute_spark_job` defaults to `retries=0`: a job that fails costs real compute to re-run, so this provider doesn't retry by default.

Every failure classification above already carries a `run_launched` signal, whether the job reached the platform before it failed. Raising `retries` is a small, safe adjustment on top of that existing signal: the exception type just follows it.

### What's retryable

`submit/config` failures raise the retryable `AirflowException`. The run never reached the platform (a worker recycling mid-submit, a bad cluster policy), so nothing ran and nothing costs money to redo.

`platform/capacity` failures are retryable too, for the same reason. On Glue, a run submitted with `JobRunQueuingEnabled` waits in Glue's run queue for about 15 minutes; if the account's DPU ceiling never frees up, Glue fails the run with `ErrorMessage: "Exceeded maximum concurrent compute"` and `ExecutionTime: 0`. The run has an id, but no job code ran and nothing was written, so the provider raises `RetryableJobFailure` (an `AirflowException` subclass) instead of `AirflowFailException`:

```
Spark job FAILED on GLUE (platform capacity failure: the run never started, safe to retry).
  run:     jr_abc1234567890      state: FAILED
  reason:  Exceeded maximum concurrent compute
  hint:    Glue capacity: the account's DPU ceiling was hit. A run with ExecutionTime 0 never started and is safe to retry; raise retries on execute_spark_job or stagger concurrent submits.
  console: https://us-west-2.console.aws.amazon.com/glue/home#/etl/jobs/run/details/jr_abc1234567890
```

With `retries=1`, one queue timeout becomes an effective 30-minute wait for capacity, which is usually enough for a nightly fan-out peak to drain. The detection is Glue-only and requires both the capacity error message and `ExecutionTime` of 0 (or missing): a run that carries the same message but actually executed stays `downstream-job` and is never retried. The retry try's pre-submit stale-run scan only looks at active runs, so the FAILED capacity run is left alone and a fresh run is submitted.

### What isn't

`downstream-job` and `trigger/polling` failures raise `AirflowFailException`, which Airflow never retries regardless of the task's `retries=`. The run launched, then failed, or a Triggerer crash happened mid-poll after launch, so re-running it blindly burns another full job at cost instead of fixing anything.

This policy applies both to normal completion callbacks (`execute_complete`) and to terminal failures reported by a trigger (`__fail__`). Classified terminal job failures become `AirflowFailException`; explicit `RetryableJobFailure` capacity failures remain retryable.

Set `retries=1` (or higher) on `execute_spark_job` to recover automatically from transient submission-time infra faults and Glue capacity rejections without risking a silent, full-cost retry of a job that actually ran and failed.
