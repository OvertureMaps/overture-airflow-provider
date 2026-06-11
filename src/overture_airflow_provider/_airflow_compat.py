"""Compatibility shim for Airflow 2.x and 3.x.

Re-exports the small surface of Airflow we depend on, sourcing each symbol
from the module that exists on the installed Airflow version:

====================  =============================================  =============================================
Symbol                Airflow 2.x                                    Airflow 3.x
====================  =============================================  =============================================
``DAG``               ``airflow.DAG``                                ``airflow.sdk.DAG``
``task``              ``airflow.decorators.task``                    ``airflow.sdk.task``
``task_group``        ``airflow.decorators.task_group``              ``airflow.sdk.task_group``
``BaseHook``          ``airflow.hooks.base.BaseHook``                ``airflow.sdk.bases.hook.BaseHook``
``BaseOperatorLink``  ``airflow.models.baseoperatorlink``            ``airflow.sdk.bases.operator.BaseOperatorLink``
``XCom``              ``airflow.models.xcom.XCom``                   ``airflow.sdk.execution_time.xcom.XCom``
``AirflowException``  ``airflow.exceptions.AirflowException``        (both)
====================  =============================================  =============================================

Callers should always import from this module, never directly from
``airflow.*``. When the project drops Airflow 2 support, simplify this
module to the ``airflow.sdk`` branch (or just inline the imports).
"""

from airflow.exceptions import AirflowException

try:  # Airflow 3.x
    from airflow.sdk import DAG, BaseOperator, task, task_group
    from airflow.sdk.bases.hook import BaseHook
    from airflow.sdk.bases.operatorlink import BaseOperatorLink
    from airflow.sdk.exceptions import TaskDeferred
    from airflow.sdk.execution_time.xcom import XCom

    AIRFLOW_MAJOR = 3
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task, task_group
    from airflow.exceptions import TaskDeferred
    from airflow.hooks.base import BaseHook
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.baseoperatorlink import BaseOperatorLink
    from airflow.models.xcom import XCom

    AIRFLOW_MAJOR = 2

__all__ = [
    "AIRFLOW_MAJOR",
    "DAG",
    "AirflowException",
    "BaseHook",
    "BaseOperator",
    "BaseOperatorLink",
    "TaskDeferred",
    "XCom",
    "task",
    "task_group",
]
