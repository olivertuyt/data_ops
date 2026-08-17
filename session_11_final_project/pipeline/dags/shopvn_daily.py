"""Daily ShopVN DAG with isolated publication TaskGroups per business domain."""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.models import TaskInstance
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email
from airflow.utils.task_group import TaskGroup
from common.config import optional_env, required_env

LOGGER = logging.getLogger(__name__)
JOB_ROOT = "/opt/shopvn/spark/jobs"
JARS = ",".join(
    [
        "/opt/airflow/jars/iceberg-spark-runtime.jar",
        "/opt/airflow/jars/iceberg-aws-bundle.jar",
        "/opt/airflow/jars/postgresql.jar",
    ]
)
RUN_ENV = {
    "SHOPVN_RUN_ID": "{{ dag_run.run_id | replace(':', '_') | replace('+', '_') }}",
    "SHOPVN_START_DATE": "{{ dag_run.conf.get('start_date') or ds }}",
    "SHOPVN_END_DATE": "{{ dag_run.conf.get('end_date') or ds }}",
}

BACKFILL_PARAMS = {
    "start_date": Param(
        None,
        type=["null", "string"],
        format="date",
        title="Start date",
        description_md="Optional inclusive start date for a bounded manual backfill (YYYY-MM-DD).",
    ),
    "end_date": Param(
        None,
        type=["null", "string"],
        format="date",
        title="End date",
        description_md=(
            "Optional inclusive end date for a bounded manual backfill (YYYY-MM-DD). "
            "It must be on or after start date."
        ),
    ),
}


def notify_failure(context: dict) -> None:
    task_instance: TaskInstance = context["task_instance"]
    subject = (
        f"[ShopVN] Pipeline failure: {task_instance.dag_id}.{task_instance.task_id}"
    )
    body = (
        f"Run: {task_instance.run_id}<br>"
        f"Task: {task_instance.task_id}<br>"
        f"Log: {task_instance.log_url}<br>"
        f"Exception class: {type(context.get('exception')).__name__}"
    )
    LOGGER.error(subject)
    alert_email = optional_env("SHOPVN_ALERT_EMAIL")
    if alert_email:
        send_email(to=[alert_email], subject=subject, html_content=body)


def spark_task(
    *, task_id: str, script: str, extra_args: str = "", retries: int = 0
) -> BashOperator:
    command = (
        f'spark-submit --master "$SPARK_MASTER" --jars "{JARS}" '
        f'"{JOB_ROOT}/{script}" '
        '--run-id "$SHOPVN_RUN_ID" '
        '--start-date "$SHOPVN_START_DATE" '
        '--end-date "$SHOPVN_END_DATE" '
        f"{extra_args}"
    )
    return BashOperator(
        task_id=task_id,
        bash_command=command,
        env=RUN_ENV,
        append_env=True,
        pool="spark_pool",
        retries=retries,
        retry_delay=timedelta(minutes=10),
        retry_exponential_backoff=True,
        max_retry_delay=timedelta(minutes=30),
        execution_timeout=timedelta(hours=2),
    )


def domain_group(domain: str) -> tuple[TaskGroup, BashOperator]:
    with TaskGroup(group_id=domain) as group:
        candidate = spark_task(
            task_id="build_candidates",
            script="transformation/build_gold_candidates.py",
            extra_args=f"--domain {domain}",
        )
        validate = spark_task(
            task_id="validate_candidates",
            script="validation/validate_gold_candidates.py",
            extra_args=f"--domain {domain}",
        )
        publish = spark_task(
            task_id="publish_gold",
            script="publication/publish_gold.py",
            extra_args=f"--domain {domain}",
        )
        candidate >> validate >> publish
    return group, candidate


DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "on_failure_callback": notify_failure,
    # The 07:30 warning leaves 30 minutes before the 08:00 business deadline.
    "sla": timedelta(hours=5, minutes=30),
}


with DAG(
    dag_id="shopvn_daily",
    description="Domain-isolated ShopVN Lakehouse pipeline with pre-Gold DQ",
    start_date=pendulum.datetime(2026, 6, 1, 2, 0, tz=required_env("SHOPVN_TIMEZONE")),
    schedule="0 2 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    max_active_tasks=2,
    dagrun_timeout=timedelta(hours=6),
    params=BACKFILL_PARAMS,
    tags=["shopvn", "iceberg", "daily"],
) as dag:
    bronze_postgres = spark_task(
        task_id="bronze_postgres",
        script="ingestion/postgres_bronze.py",
        retries=2,
    )
    bronze_api = spark_task(
        task_id="bronze_api",
        script="ingestion/api_bronze.py",
        extra_args='--landing-dir "/opt/airflow/landing/api"',
    )
    bronze_sftp = spark_task(
        task_id="bronze_sftp",
        script="ingestion/sftp_bronze.py",
        extra_args='--landing-dir "/opt/airflow/landing/marketplace"',
    )

    silver_postgres = spark_task(
        task_id="silver_postgres",
        script="transformation/bronze_to_silver.py",
        extra_args="--model-group postgres",
    )
    silver_api = spark_task(
        task_id="silver_api",
        script="transformation/bronze_to_silver.py",
        extra_args="--model-group api",
    )
    silver_sftp = spark_task(
        task_id="silver_sftp",
        script="transformation/bronze_to_silver.py",
        extra_args="--model-group sftp",
    )

    bronze_postgres >> [bronze_api, silver_postgres]
    bronze_api >> silver_api
    bronze_sftp >> silver_sftp

    finance, finance_candidate = domain_group("finance")
    operations, operations_candidate = domain_group("operations")
    customer, customer_candidate = domain_group("customer")
    product, product_candidate = domain_group("product")

    [silver_postgres, silver_sftp] >> finance_candidate
    [silver_postgres, silver_api, silver_sftp] >> operations_candidate
    silver_postgres >> customer_candidate
    [silver_postgres, silver_sftp] >> product_candidate
