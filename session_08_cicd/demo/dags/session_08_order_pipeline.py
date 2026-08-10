from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from dataops_common.duckdb_dag import LOAD_OPTS, duckdb_default_args
from dataops_common.notifications import notify_on_sla_miss

with DAG(
    dag_id="session_08_order_pipeline",
    description="Orders pipeline Demo",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=duckdb_default_args(),
    sla_miss_callback=notify_on_sla_miss,
    template_searchpath="/opt/airflow/dags/sql/order_pipeline",
    tags=["session-08", "cicd", "duckdb"],
) as dag:
    init_schema = SQLExecuteQueryOperator(
        task_id="init_schema", sql="create_schema.sql", **LOAD_OPTS
    )
    load_bronze = SQLExecuteQueryOperator(
        task_id="load_bronze", sql="raw2bronze__orders.sql", **LOAD_OPTS
    )
    silver_transform = SQLExecuteQueryOperator(
        task_id="silver_transform",
        sql="bronze2silver__orders.sql",
        sla=timedelta(hours=1),
        **LOAD_OPTS,
    )
    load_gold = SQLExecuteQueryOperator(
        task_id="load_gold", sql="silver2gold__orders.sql", **LOAD_OPTS
    )
    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile.sql")

    init_schema >> load_bronze >> silver_transform >> load_gold >> reconcile
