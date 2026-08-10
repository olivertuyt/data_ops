from __future__ import annotations

from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from dataops_common.duckdb_dag import LOAD_OPTS, duckdb_default_args

with DAG(
    dag_id="session_08_order_pipeline",
    description="Orders pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=True,  # TODO Bug1: backfills all history on first deploy — should be False
    max_active_runs=1,
    default_args=duckdb_default_args(),
    template_searchpath="/opt/airflow/dags/sql/order_pipeline",
    tags=["session-08", "cicd", "duckdb"],
) as dag:
    load_bronze = SQLExecuteQueryOperator(
        task_id="load_bronze", sql="raw2bronze__orders.sql", **LOAD_OPTS
    )
    silver_transform = SQLExecuteQueryOperator(
        task_id="silver_transform",
        sql="bronze2silver__orders.sql",
        **LOAD_OPTS,
    )
    load_gold = SQLExecuteQueryOperator(
        task_id="load_gold", sql="silver2gold__orders.sql", **LOAD_OPTS
    )
    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile.sql")

    (
        load_bronze >> silver_transform >> reconcile >> load_gold
    )  # TODO Bug2: reconcile runs before load_gold — gold is never loaded
