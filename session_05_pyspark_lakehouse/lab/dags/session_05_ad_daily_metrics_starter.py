from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dataops_common.notifications import notify_on_failure

log = logging.getLogger(__name__)

JARS = Variable.get("spark_jars")
PY_FILES = "/opt/airflow/jobs/common/lakehouse_common.py"
SPARK_CONF = {"spark.driver.host": "airflow-worker", "spark.driver.bindAddress": "0.0.0.0"}

ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))

SQL_DIR = Path(__file__).parent / "sql"
MAX_DAY_OVER_DAY_DRIFT = 0.5


def _reconcile_metrics(ds: str):
    import trino

    sql = (SQL_DIR / "reconcile.sql").read_text().format(ds=ds)
    conn = trino.dbapi.connect(host=TRINO_HOST, port=TRINO_PORT, user="airflow", catalog="delta")
    cur = conn.cursor()
    cur.execute(sql)
    return cur.fetchone()


def _spark_submit(task_id: str, application: str) -> SparkSubmitOperator:
    return SparkSubmitOperator(
        task_id=task_id,
        application=application,
        conn_id="spark_default",
        application_args=["--ds", "{{ ds }}"],
        jars=JARS,
        py_files=PY_FILES,
        conf=SPARK_CONF,
        verbose=False,
    )


@dag(
    dag_id="session_05_ad_daily_metrics_starter",
    description="LAB — complete the fact/aggregate jobs and the reconcile task, then run this",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
        "execution_timeout": timedelta(hours=2),
        "on_failure_callback": notify_on_failure,
    },
    tags=["session-05", "lab", "exercise", "ads"],
)
def session_05_ad_daily_metrics_starter():
    @task(task_id="bronze_2_silver__validate_raw_file")
    def validate_raw_file(ds=None):
        from minio import Minio
        from minio.error import S3Error

        client = Minio("minio:9000", access_key=ACCESS_KEY, secret_key=SECRET_KEY, secure=False)
        try:
            stat = client.stat_object("bronze", f"ad_events/{ds}.csv")
        except S3Error as err:
            raise FileNotFoundError(f"s3a://bronze/ad_events/{ds}.csv not found in MinIO") from err
        if stat.size == 0:
            raise ValueError(f"bronze/ad_events/{ds}.csv is empty")
        log.info("Validated bronze/ad_events/%s.csv (%d bytes)", ds, stat.size)

    # Lab 1: you write jobs/lab/build_fact_ad_events.py
    build_fact_ad_events = _spark_submit(
        "bronze_2_silver__build_fact_ad_events", "/opt/airflow/jobs/lab/build_fact_ad_events.py"
    )
    # Lab 2: you write jobs/lab/aggregate_ad_daily.py
    aggregate_ad_daily = _spark_submit(
        "silver_2_gold__aggregate_ad_daily", "/opt/airflow/jobs/lab/aggregate_ad_daily.py"
    )

    @task(task_id="reconcile__silver_gold")
    def reconcile(ds=None):
        fact_rows, fact_distinct, fact_impr, agg_impr, bad_rows, prev_rows = _reconcile_metrics(ds)

        if fact_rows == 0:
            raise ValueError(f"silver.fact_ad_events is empty for {ds}")
        if fact_rows != fact_distinct:
            raise ValueError(
                f"duplicate event_ids for {ds}: rows={fact_rows} distinct={fact_distinct}"
            )
        if fact_impr != agg_impr:
            raise ValueError(f"impressions mismatch for {ds}: fact={fact_impr} agg={agg_impr}")
        if bad_rows:
            raise ValueError(f"{bad_rows} gold rows break a business rule for {ds}")
        if prev_rows and abs(fact_rows - prev_rows) / prev_rows > MAX_DAY_OVER_DAY_DRIFT:
            raise ValueError(
                f"day-over-day volume drift for {ds}: today={fact_rows} prev={prev_rows}"
            )

        log.info(
            "Reconcile passed for %s: %d rows, %d impressions, 0 bad rows", ds, fact_rows, fact_impr
        )

    validate_raw_file() >> build_fact_ad_events >> aggregate_ad_daily >> reconcile()


session_05_ad_daily_metrics_starter()
