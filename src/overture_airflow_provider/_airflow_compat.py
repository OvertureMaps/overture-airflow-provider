"""Single import point for the Airflow 3 surface this provider depends on.

====================  =============================================
Symbol                Source
====================  =============================================
``DAG``               ``airflow.sdk.DAG``
``task``              ``airflow.sdk.task``
``task_group``        ``airflow.sdk.task_group``
``TaskGroup``         ``airflow.sdk.TaskGroup``
``TriggerRule``       ``airflow.sdk.TriggerRule``
``XComArg``           ``airflow.sdk.XComArg``
``BaseOperator``      ``airflow.sdk.BaseOperator``
``BaseHook``          ``airflow.sdk.bases.hook.BaseHook``
``BaseOperatorLink``  ``airflow.sdk.bases.operatorlink.BaseOperatorLink``
``XCom``              ``airflow.sdk.execution_time.xcom.XCom``
``AirflowException``      ``airflow.exceptions.AirflowException``
``AirflowFailException``  ``airflow.sdk.exceptions.AirflowFailException``
``TaskDeferred``          ``airflow.sdk.exceptions.TaskDeferred``
``EmptyOperator``         ``airflow.providers.standard.operators.empty`` (lazy)
``TriggerDagRunOperator`` ``airflow.providers.standard.operators.trigger_dagrun`` (lazy)
====================  =============================================

Callers should import from this module rather than directly from
``airflow.*`` so any future relocation of these symbols is a one-file change.
The standard-provider operators load on first access because importing them
pulls in ``airflow.models``.
"""

from importlib import import_module

from airflow.exceptions import AirflowException
from airflow.sdk import DAG, BaseOperator, TaskGroup, TriggerRule, XComArg, task, task_group
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.bases.operatorlink import BaseOperatorLink
from airflow.sdk.exceptions import AirflowFailException, TaskDeferred
from airflow.sdk.execution_time.xcom import XCom

_LAZY_OPERATORS = {
    "EmptyOperator": "airflow.providers.standard.operators.empty",
    "TriggerDagRunOperator": "airflow.providers.standard.operators.trigger_dagrun",
}


def __getattr__(name: str):
    if name not in _LAZY_OPERATORS:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    return getattr(import_module(_LAZY_OPERATORS[name]), name)


__all__ = [
    "DAG",
    "AirflowException",
    "AirflowFailException",
    "BaseHook",
    "BaseOperator",
    "BaseOperatorLink",
    "TaskDeferred",
    "TaskGroup",
    "TriggerRule",
    "XCom",
    "XComArg",
    "task",
    "task_group",
    *_LAZY_OPERATORS,
]
