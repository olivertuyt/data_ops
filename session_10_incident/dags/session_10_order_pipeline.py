from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from dataops_common.notifications import notify_on_failure

PY_FILES = "/opt/airflow/plugins/dataops_common/lakehouse_common.py"
SPARK_CONF = {
    "spark.driver.host": "airflow-worker",
    "spark.driver.bindAddress": "0.0.0.0",
}


@dag(
    dag_id="session_10_order_pipeline",
    description="Order pipeline: bronze CSV → silver clean → gold daily revenue",
    schedule="@daily",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 0,
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_on_failure,
    },
    tags=["session-10", "lakehouse", "spark", "delta", "orders"],
)
def session_10_order_pipeline():
    raw2bronze__marketplace_orders = SparkSubmitOperator(
        task_id="raw2bronze__marketplace_orders",
        application="/opt/airflow/jobs/raw2bronze__marketplace_orders.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ data_interval_end | ds }}"],
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
    )

    bronze2silver__orders = SparkSubmitOperator(
        task_id="bronze2silver__orders",
        application="/opt/airflow/jobs/bronze2silver__orders.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ data_interval_end | ds }}"],
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
    )

    silver2gold__fact_daily_revenue = SparkSubmitOperator(
        task_id="silver2gold__fact_daily_revenue",
        application="/opt/airflow/jobs/silver2gold__fact_daily_revenue.py",
        conn_id="spark_default",
        application_args=["--ds", "{{ data_interval_end | ds }}"],
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
    )

    reconcile__not_empty__orders = SQLCheckOperator(
        task_id="reconcile__not_empty__orders",
        conn_id="trino_default",
        sql="sql/session_10_order_pipeline/reconcile__not_empty__orders.sql",
    )

    reconcile__revenue__fact_daily_revenue = SQLCheckOperator(
        task_id="reconcile__revenue__fact_daily_revenue",
        conn_id="trino_default",
        sql="sql/session_10_order_pipeline/reconcile__revenue__fact_daily_revenue.sql",
    )

    reconcile__row_count__fact_daily_revenue = SQLCheckOperator(
        task_id="reconcile__row_count__fact_daily_revenue",
        conn_id="trino_default",
        sql="sql/session_10_order_pipeline/reconcile__row_count__fact_daily_revenue.sql",
    )

    (
        raw2bronze__marketplace_orders
        >> bronze2silver__orders
        >> silver2gold__fact_daily_revenue
        >> reconcile__not_empty__orders
        >> reconcile__revenue__fact_daily_revenue
        >> reconcile__row_count__fact_daily_revenue
    )


session_10_order_pipeline()
