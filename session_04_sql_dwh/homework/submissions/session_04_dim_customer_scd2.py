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
    dag_id="session_04_dim_customer_scd2",
    description="HW2 — SCD Type 2 dimension with point-in-time query",
    schedule="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    template_searchpath="/opt/airflow/dags/session_04_homework/sql/dim_customer_scd2",
    params={"silver_upto": 500},
    tags=["session-04", "homework", "scd2"],
) as dag:
    opts = {"split_statements": True, "autocommit": True}

    load_snapshot = SQLExecuteQueryOperator(
        task_id="load_snapshot", sql="load_snapshot.sql", **opts
    )
    apply_scd2 = SQLExecuteQueryOperator(task_id="apply_scd2", sql="apply_scd2.sql", **opts)
    point_in_time = SQLExecuteQueryOperator(
        task_id="point_in_time", sql="point_in_time.sql", **opts
    )
    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile_scd2.sql")

    load_snapshot >> apply_scd2 >> point_in_time >> reconcile
