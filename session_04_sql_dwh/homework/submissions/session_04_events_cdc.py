from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from dataops_common.notifications import notify_on_failure

default_args = {
    "owner": "dataops-class",
    "conn_id": "duckdb_default",
    "pool": "duckdb_pool",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
}

with DAG(
    dag_id="session_04_events_cdc",
    description="HW1 — apply CDC change stream into silver.events via MERGE",
    schedule="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/session_04_homework/sql/events_cdc",
    tags=["session-04", "homework", "cdc", "merge"],
) as dag:
    opts = {"split_statements": True, "autocommit": True}

    load_changes = SQLExecuteQueryOperator(task_id="load_changes", sql="load_changes.sql", **opts)
    apply_cdc = SQLExecuteQueryOperator(task_id="apply_cdc", sql="apply_cdc.sql", **opts)
    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile_cdc.sql")

    load_changes >> apply_cdc >> reconcile
