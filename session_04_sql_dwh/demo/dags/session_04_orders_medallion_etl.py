from __future__ import annotations

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from dataops_common.duckdb_dag import LOAD_OPTS, duckdb_default_args
from dataops_common.notifications import notify_on_sla_miss

with DAG(
    dag_id="session_04_orders_medallion_etl",
    description="Reference Medallion pipeline on DuckDB (operators + external .sql)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=duckdb_default_args(),
    sla_miss_callback=notify_on_sla_miss,
    template_searchpath="/opt/airflow/dags/session_04/sql/orders_medallion_etl",
    tags=["session-04", "sql", "duckdb", "medallion"],
) as dag:
    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema", sql="create_schema.sql", **LOAD_OPTS
    )
    load_bronze = SQLExecuteQueryOperator(task_id="load_bronze", sql="load_bronze.sql", **LOAD_OPTS)
    validate_to_silver = SQLExecuteQueryOperator(
        task_id="validate_to_silver",
        sql="validate_to_silver.sql",
        sla=timedelta(hours=1),
        **LOAD_OPTS,
    )
    build_dim_customer = SQLExecuteQueryOperator(
        task_id="build_dim_customer", sql="build_dim_customer.sql", **LOAD_OPTS
    )
    build_fact_sales = SQLExecuteQueryOperator(
        task_id="build_fact_sales", sql="build_fact_sales.sql", **LOAD_OPTS
    )
    # SQLCheckOperator fails the task if any column in reconcile.sql's row is falsy.
    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile.sql")

    (
        create_schema
        >> load_bronze
        >> validate_to_silver
        >> build_dim_customer
        >> build_fact_sales
        >> reconcile
    )
