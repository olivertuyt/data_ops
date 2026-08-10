"""Reference pipeline for session 3 — raw ad events rolled up into daily
per-campaign metrics, written idempotently per date."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from multiprocessing import pool

from airflow.decorators import dag, task
from airflow.models import Variable

from dataops_common.notifications import notify_on_failure, notify_on_sla_miss
from dataops_common.sample_data import aggregate_campaigns, generate_ad_events
from dataops_common.storage import read_partition, reconcile_counts, write_partition
from airflow.operators.bash import BashOperator

log = logging.getLogger(__name__)

default_args = {
    "owner": "growth-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
    "on_failure_callback": notify_on_failure,
}


@dag(
    dag_id="session_03_campaign_daily_metrics",
    description="Idempotent daily ad-campaign metrics — safe to rerun and backfill",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    sla_miss_callback=notify_on_sla_miss,
    tags=["session-03", "ads", "reference"],
)
def campaign_daily_metrics():

    @task(sla=timedelta(minutes=30))
    def ingest(logical_date: datetime | None = None) -> int:
        volume = int(Variable.get("daily_event_volume", default_var=30))
        events = generate_ad_events(logical_date, n_events=volume)
        write_partition("raw", "ad_events", logical_date, events)
        log.info("Ingested %d ad events for %s", len(events), logical_date.date())
        return len(events)

    @task(pool="reporting_pool")
    def rollup(_event_count: int, logical_date: datetime | None = None) -> int:
        events = read_partition("raw", "ad_events", logical_date)
        metrics = aggregate_campaigns(events)
        write_partition("curated", "campaign_metrics", logical_date, metrics)
        return len(metrics)

    @task(pool="campaign_db_pool", sla=timedelta(minutes=45))
    def publish(_campaign_count: int, logical_date: datetime | None = None) -> int:
        metrics = read_partition("curated", "campaign_metrics", logical_date)
        write_partition("published", "campaign_metrics", logical_date, metrics)
        log.info("Published %d campaign rows for %s", len(metrics), logical_date.date())
        return len(metrics)

    @task
    def reconcile(_published_count: int, logical_date: datetime | None = None) -> None:
        raw_events = len(read_partition("raw", "ad_events", logical_date))
        published = read_partition("published", "campaign_metrics", logical_date)
        total_impressions = sum(row["impressions"] for row in published)
        reconcile_counts(raw_events, total_impressions, label=f"ad_events/{logical_date.date()}")
        log.info("Reconciliation OK: %d events == %d impressions", raw_events, total_impressions)
        
    @task.bash
    def trigger_downstream_dag(logical_date: datetime):
        return "airflow dags trigger session_03_campaign_spend_report -e {{ ds }}"
    

    events = ingest()
    metrics = rollup(events)
    published = publish(metrics)
    recon = reconcile(published)
    
    # Just note in class. 
    # If you want to trigger an DAG, Highly recommend using below operator
    # https://airflow.apache.org/docs/apache-airflow/2.3.4/_api/airflow/operators/trigger_dagrun/index.html 
    # recon >> trigger_downstream_dag()


campaign_daily_metrics()
