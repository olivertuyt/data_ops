"""
Parallel workload — a fan-out of slow tasks, used to watch the Celery Executor
in Flower.

Each task sleeps long enough to observe it running: open Flower to see the tasks
distributed across workers, scale the workers to see them spread out, or stop a
worker mid-run to watch its tasks get reassigned.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

# How long each parallel task sleeps, and how many run in parallel.
# Change them here, or override live (no restart) via Airflow Variables of the
# same name under Admin → Variables.
SLEEP_SECONDS = int(Variable.get("parallel_sleep_seconds", default_var=120))  # 2 minutes
TASK_COUNT = int(Variable.get("parallel_task_count", default_var=8))


@dag(
    dag_id="parallel_workload",
    description="Fan-out of slow tasks — watch Celery distribute them in Flower",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={"owner": "dataops-class"},
    tags=["session-02", "celery", "demo"],
)
def parallel_workload():
    start = EmptyOperator(task_id="start")
    done = EmptyOperator(task_id="done")

    @task
    def work(n: int) -> None:
        log.info("work_%d sleeping for %ds", n, SLEEP_SECONDS)
        time.sleep(SLEEP_SECONDS)
        log.info("work_%d done", n)

    for i in range(1, TASK_COUNT + 1):
        start >> work.override(task_id=f"work_{i}")(i) >> done


parallel_workload()
