"""Single import point for the Airflow 3 surface this provider depends on.

====================  =============================================
Symbol                Source
====================  =============================================
``DAG``               ``airflow.sdk.DAG``
``task``              ``airflow.sdk.task``
``task_group``        ``airflow.sdk.task_group``
``BaseOperator``      ``airflow.sdk.BaseOperator``
``BaseHook``          ``airflow.sdk.bases.hook.BaseHook``
``BaseOperatorLink``  ``airflow.sdk.bases.operatorlink.BaseOperatorLink``
``XCom``              ``airflow.sdk.execution_time.xcom.XCom``
``AirflowException``      ``airflow.exceptions.AirflowException``
``AirflowFailException``  ``airflow.sdk.exceptions.AirflowFailException``
``TaskDeferred``          ``airflow.sdk.exceptions.TaskDeferred``
====================  =============================================

Callers should import from this module rather than directly from
``airflow.*`` so any future relocation of these symbols is a one-file change.
"""

from airflow.exceptions import AirflowException
from airflow.sdk import DAG, BaseOperator, task, task_group
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.operatorlink import BaseOperatorLink
from airflow.sdk.exceptions import AirflowFailException, TaskDeferred
from airflow.sdk.execution_time.xcom import XCom

__all__ = [
    "DAG",
    "AirflowException",
    "AirflowFailException",
    "BaseHook",
    "BaseOperator",
    "BaseOperatorLink",
    "TaskDeferred",
    "XCom",
    "task",
    "task_group",
]
