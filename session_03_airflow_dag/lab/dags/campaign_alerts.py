"""
Flags campaigns whose CTR is below a threshold, downstream of
campaign_daily_metrics. Mirrors campaign_spend_report — same sensor pattern,
different metric.
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
    "owner": "growth-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "on_failure_callback": notify_on_failure,
}


@dag(
    dag_id="session_03_campaign_alerts",
    description="Flag low-CTR campaigns — runs after campaign_daily_metrics.reconcile",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["session-03", "ads", "exercise"],
)
def campaign_alerts():

    wait_for_metrics = ExternalTaskSensor(
        task_id="wait_for_metrics",
        external_dag_id="session_03_campaign_daily_metrics",
        external_task_id="reconcile",
        mode="reschedule",
        poke_interval=30,
        timeout=3600,
    )

    @task
    def flag_low_ctr(logical_date: datetime | None = None) -> dict:
        min_ctr = float(Variable.get("min_ctr", default_var=0.15))
        metrics = read_partition("published", "campaign_metrics", logical_date)
        low_ctr = [m for m in metrics if m["ctr"] < min_ctr]
        wasted = round(sum(m["spend_usd"] for m in low_ctr), 2)
        report = {
            "date": logical_date.strftime("%Y-%m-%d"),
            "min_ctr": min_ctr,
            "low_ctr_campaigns": [m["campaign"] for m in low_ctr],
            "wasted_spend_usd": wasted,
        }
        write_partition("reports", "campaign_low_ctr", logical_date, [report])
        log.info("Low-CTR alert: %s", report)
        return report

    alert = flag_low_ctr()

    wait_for_metrics >> alert


campaign_alerts()
