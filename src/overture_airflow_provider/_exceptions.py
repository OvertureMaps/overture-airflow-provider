"""Provider exceptions built on the Airflow compat shim.

Separate from ``_failures`` (stdlib-only) and ``spark_platform_handlers``
(imported by ``render`` without Airflow).
"""

from overture_airflow_provider._airflow_compat import AirflowException


class RetryableJobFailure(AirflowException):
    """A classified job failure whose run did no work.

    The operator propagates it unchanged instead of wrapping it in
    ``AirflowFailException``, so the task's ``retries`` apply.
    """
