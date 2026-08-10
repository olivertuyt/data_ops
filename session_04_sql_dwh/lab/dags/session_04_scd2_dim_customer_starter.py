from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
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
    dag_id="session_04_scd2_dim_customer_starter",
    description="LAB 2 — Slowly Changing Dimension Type 2 (dim_customer_scd2)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/session_04_lab/sql/scd2_dim_customer_starter",
    tags=["session-04", "lab", "exercise", "scd2"],
) as dag:
    opts = {"split_statements": True, "autocommit": True}

    create_schema = SQLExecuteQueryOperator(task_id="create_schema", sql="create_scd2.sql", **opts)
    load_snapshot = SQLExecuteQueryOperator(
        task_id="load_snapshot", sql="load_snapshot.sql", **opts
    )
    validate_snapshot = SQLExecuteQueryOperator(
        task_id="validate_snapshot", sql="validate_snapshot.sql", **opts
    )
    apply_scd2 = SQLExecuteQueryOperator(task_id="apply_scd2", sql="apply_scd2.sql", **opts)

    create_schema >> load_snapshot >> validate_snapshot >> apply_scd2
