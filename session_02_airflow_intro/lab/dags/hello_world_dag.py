"""Hello World DAG — first look at tasks, operators, and dependencies."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

log = logging.getLogger(__name__)


def _print_context(**context):
    log.info("DAG run ID   : %s", context["run_id"])
    log.info("Logical date : %s", context["logical_date"])
    log.info("Executor     : %s", context["conf"].get("core", "executor"))


default_args = {
    "owner": "dataops-class",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


@dag(
    dag_id="hello_world",
    description="First demo DAG — explore the UI and task structure",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["session-02", "demo"],
)
def hello_world_dag():

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command='echo "Hello from Airflow $(airflow version)!"',
    )

    print_context = PythonOperator(
        task_id="print_context",
        python_callable=_print_context,
    )

    @task
    def compute_run_label(logical_date=None) -> str:
        label = f"run_{logical_date.strftime('%Y%m%d')}"
        log.info("Run label: %s", label)
        return label

    @task
    def finish(label: str):
        log.info("Pipeline complete. Label: %s", label)

    label = compute_run_label()
    say_hello >> print_context >> label >> finish(label)


hello_world_dag()
