"""
Finance team's daily spend report — waits for campaign_daily_metrics to finish
that date via a cross-DAG ExternalTaskSensor, then flags over-budget campaigns.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from dataops_common.notifications import notify_on_failure
from dataops_common.storage import read_partition, write_partition

log = logging.getLogger(__name__)

default_args = {
    "owner": "finance",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
}


@dag(
    dag_id="session_03_campaign_spend_report",
    description="Daily spend report — runs after campaign_daily_metrics.reconcile",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["session-03", "ads", "cross-dag"],
)
def campaign_spend_report():

    # reschedule mode frees the worker slot between checks — preferred over poke
    # when the wait can be long.
    wait_for_metrics = ExternalTaskSensor(
        task_id="wait_for_metrics",
        external_dag_id="session_03_campaign_daily_metrics",
        external_task_id="reconcile",
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    @task
    def flag_over_budget(logical_date: datetime | None = None) -> dict:
        budget = float(Variable.get("daily_budget_usd", default_var=5.0))
        metrics = read_partition("published", "campaign_metrics", logical_date)
        over_budget = [m["campaign"] for m in metrics if m["spend_usd"] > budget]
        report = {
            "date": logical_date.strftime("%Y-%m-%d"),
            "budget_usd": budget,
            "total_spend_usd": round(sum(m["spend_usd"] for m in metrics), 2),
            "over_budget_campaigns": over_budget,
        }
        write_partition("reports", "campaign_spend", logical_date, [report])
        log.info("Spend report: %s", report)
        return report

    wait_for_metrics >> flag_over_budget()


campaign_spend_report()
