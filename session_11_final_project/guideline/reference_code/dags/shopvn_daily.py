"""Daily ShopVN pipeline. Gold publication has a strict all-success dependency."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import TaskInstance
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email


LOGGER = logging.getLogger(__name__)
SPARK_SUBMIT = "/opt/spark/bin/spark-submit"
JARS = ",".join(
    [
        "/opt/airflow/jars/iceberg-spark-runtime.jar",
        "/opt/airflow/jars/iceberg-aws-bundle.jar",
        "/opt/airflow/jars/postgresql.jar",
    ]
)
JOB_DIR = "/opt/shopvn/jobs"
RUN_ENV = {
    "RUN_ID": "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') }}",
    "START_DATE": "{{ dag_run.conf.get('start_date', ds) }}",
    "END_DATE": "{{ dag_run.conf.get('end_date', ds) }}",
}


def notify_failure(context: dict) -> None:
    task_instance: TaskInstance = context["task_instance"]
    subject = f"[ShopVN] Pipeline failure: {task_instance.dag_id}.{task_instance.task_id}"
    body = (
        f"Run: {task_instance.run_id}<br>"
        f"Task: {task_instance.task_id}<br>"
        f"Log: {task_instance.log_url}<br>"
        f"Exception: {context.get('exception')}"
    )
    LOGGER.error(subject)
    alert_email = os.getenv("SHOPVN_ALERT_EMAIL")
    if alert_email:
        send_email(to=[alert_email], subject=subject, html_content=body)


def spark_task(task_id: str, script: str) -> BashOperator:
    command = (
        f'{SPARK_SUBMIT} --master "local[2]" --jars "{JARS}" '
        f'"{JOB_DIR}/{script}" --run-id "$RUN_ID" '
        '--start-date "$START_DATE" --end-date "$END_DATE"'
    )
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        env=RUN_ENV,
        append_env=True,
        execution_timeout=timedelta(hours=2),
    )


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "on_failure_callback": notify_failure,
    # The DAG starts at 02:00. A six-hour task SLA maps to the 08:00 business SLA.
    "sla": timedelta(hours=6),
}


with DAG(
    dag_id="shopvn_daily",
    description="Idempotent ShopVN medallion pipeline with pre-Gold DQ",
    start_date=datetime(2026, 6, 1),
    schedule="0 2 * * *",
    catchup=False,
    # Historical loads are explicit reviewed DAG runs with start_date/end_date conf.
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    dagrun_timeout=timedelta(hours=6),
    tags=["shopvn", "iceberg", "daily"],
) as dag:
    bronze_postgres = spark_task("bronze_postgres", "postgres_bronze.py")
    bronze_sftp = spark_task("bronze_sftp", "sftp_bronze.py")
    bronze_api = spark_task("bronze_api", "api_bronze.py")
    silver = spark_task("bronze_to_silver", "bronze_to_silver.py")
    candidates = spark_task("build_gold_candidates", "build_gold_candidates.py")
    blocking_dq = spark_task("validate_gold_candidates", "validate_gold_candidates.py")
    publish_gold = spark_task("publish_gold", "publish_gold.py")

    bronze_postgres >> bronze_api
    [bronze_postgres, bronze_api, bronze_sftp] >> silver
    silver >> candidates >> blocking_dq >> publish_gold
