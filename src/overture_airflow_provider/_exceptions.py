"""Provider-defined exceptions layered on the Airflow compat shim.

Kept separate from ``_failures`` (which is deliberately stdlib-only) and from
``spark_platform_handlers`` (which ``render`` imports without Airflow present),
so both the Glue completion path and the operator can import the same type.
"""

from overture_airflow_provider._airflow_compat import AirflowException


class RetryableJobFailure(AirflowException):
    """A classified platform job failure whose run did no work.

    Raised instead of a plain ``AirflowException`` when ``_failures.is_retryable``
    says the run never started (e.g. Glue rejected a queued run with ``Exceeded
    maximum concurrent compute`` and ``ExecutionTime: 0``). The operator
    propagates it unchanged, so the task's configured ``retries`` apply, where a
    launched-and-failed run would be wrapped in the non-retryable
    ``AirflowFailException``.
    """
