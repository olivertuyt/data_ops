import logging
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLCheckOperator, SQLExecuteQueryOperator
from dataops_common.duckdb_dag import LOAD_OPTS, duckdb_default_args
from dataops_common.ge_checkpoint import run_ge_checkpoint

logger = logging.getLogger(__name__)

DEMO_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = DEMO_DIR / "data"
GE_DIR = DEMO_DIR / "great_expectations"
WAREHOUSE = Path("/opt/airflow/db/dwh.duckdb")
SQL_DIR = str(DEMO_DIR / "dags" / "sql" / "session_09_order_pipeline")


@dag(
    dag_id="session_09_order_pipeline",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=duckdb_default_args(),
    template_searchpath=[SQL_DIR],
    tags=["session-09", "monitoring", "data-quality"],
)
def order_pipeline():
    init_schema = SQLExecuteQueryOperator(
        task_id="init_schema", sql="create_schema.sql", **LOAD_OPTS
    )

    load_bronze = SQLExecuteQueryOperator(
        task_id="load_bronze", sql="raw2bronze__orders.sql", **LOAD_OPTS
    )

    silver_transform = SQLExecuteQueryOperator(
        task_id="silver_transform", sql="bronze2silver__orders.sql", **LOAD_OPTS
    )

    @task
    def ge_checkpoint_silver() -> None:
        run_ge_checkpoint(
            warehouse=WAREHOUSE,
            ge_dir=GE_DIR,
            suite_name="orders_silver_suite",
            sql="SELECT * FROM silver.orders",
        )

    load_gold = SQLExecuteQueryOperator(
        task_id="load_gold", sql="silver2gold__orders.sql", **LOAD_OPTS
    )

    reconcile = SQLCheckOperator(task_id="reconcile", sql="reconcile.sql")

    (
        init_schema
        >> load_bronze
        >> silver_transform
        >> ge_checkpoint_silver()
        >> load_gold
        >> reconcile
    )


order_pipeline()
