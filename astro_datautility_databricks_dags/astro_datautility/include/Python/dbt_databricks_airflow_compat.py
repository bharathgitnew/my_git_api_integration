"""Airflow 3 (Astro Runtime 3.x, per the repo Dockerfile) imports with Airflow 2 fallbacks."""
from __future__ import annotations

try:  # Airflow 3
    from airflow.sdk import DAG, Param, TaskGroup, get_current_context, task
except ImportError:  # Airflow 2.x
    from airflow import DAG
    from airflow.decorators import task
    from airflow.models.param import Param
    from airflow.operators.python import get_current_context
    from airflow.utils.task_group import TaskGroup

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from airflow.exceptions import AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule

__all__ = [
    "DAG", "Param", "TaskGroup", "task", "get_current_context",
    "EmptyOperator", "TriggerDagRunOperator", "TriggerRule",
    "AirflowFailException", "AirflowSkipException",
]
