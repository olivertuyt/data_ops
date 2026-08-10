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
    dag_id="session_04_fact_customer_daily_starter",
    description="LAB 1 — idempotent daily rollup fact (fact_customer_daily)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/session_04_lab/sql/fact_customer_daily_starter",
    tags=["session-04", "lab", "exercise"],
) as dag:
    opts = {"split_statements": True, "autocommit": True}

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema", sql="create_fact_customer_daily.sql", **opts
    )
    build_fact = SQLExecuteQueryOperator(
        task_id="build_fact", sql="build_fact_customer_daily.sql", **opts
    )
    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile_fact_customer_daily.sql")

    create_schema >> build_fact >> reconcile
