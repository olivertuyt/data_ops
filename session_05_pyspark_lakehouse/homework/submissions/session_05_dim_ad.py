from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from dataops_common.notifications import notify_on_failure

JARS = Variable.get("spark_jars")
PY_FILES = "/opt/airflow/jobs/common/lakehouse_common.py"
SPARK_CONF = {"spark.driver.host": "airflow-worker", "spark.driver.bindAddress": "0.0.0.0"}

ACCESS_KEY = os.environ["AWS_ACCESS_KEY_ID"]
SECRET_KEY = os.environ["AWS_SECRET_ACCESS_KEY"]
TRINO_HOST = os.environ.get("TRINO_HOST", "trino")
TRINO_PORT = int(os.environ.get("TRINO_PORT", "8080"))


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
    dag_id="session_05_dim_ad",
    description="HW1 — upsert gold.dim_ad via Delta MERGE + reconcile",
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
    tags=["session-05", "homework", "dim", "merge"],
)
def session_05_dim_ad():
    upsert_dim = _spark_submit(
        "upsert_dim_ad",
        "/opt/airflow/jobs/homework/upsert_dim_ad.py",
    )

    @task(task_id="reconcile_dim")
    def reconcile_dim(ds=None):
        import trino

        sql = f"""
        SELECT
            COUNT(*)                                                        AS total_rows,
            COUNT(DISTINCT ad_id)                                           AS distinct_ads,
            COUNT(CASE WHEN first_seen_date > last_active_date THEN 1 END) AS bad_dates,
            COUNT(CASE WHEN latest_ctr < 0 OR latest_ctr > 1 THEN 1 END)  AS bad_ctr,
            (
                SELECT COUNT(DISTINCT ad_id)
                FROM delta.gold.agg_ad_daily
                WHERE event_date <= DATE '{ds}'
            ) AS agg_distinct_ads
        FROM delta.gold.dim_ad
        """
        conn = trino.dbapi.connect(
            host=TRINO_HOST, port=TRINO_PORT, user="airflow", catalog="delta"
        )
        cur = conn.cursor()
        cur.execute(sql)
        total_rows, distinct_ads, bad_dates, bad_ctr, agg_distinct_ads = cur.fetchone()

        if total_rows != distinct_ads:
            raise ValueError(
                f"dim_ad has duplicate ad_id rows: total={total_rows} distinct={distinct_ads}"
            )
        if bad_dates:
            raise ValueError(f"{bad_dates} rows have first_seen_date > last_active_date")
        if total_rows != agg_distinct_ads:
            raise ValueError(f"dim_ad missing ads: dim={total_rows} agg={agg_distinct_ads}")
        if bad_ctr:
            raise ValueError(f"{bad_ctr} rows have latest_ctr outside [0, 1]")

    upsert_dim >> reconcile_dim()


session_05_dim_ad()
