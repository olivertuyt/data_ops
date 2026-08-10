"""
Daily genre stats pipeline — reads plays + tracks from Postgres, aggregates
per (day, genre), and writes into daily_genre_stats with full idempotency
and reconciliation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

from dataops_common.notifications import notify_on_failure

log = logging.getLogger(__name__)

default_args = {
    "owner": "music-data",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": notify_on_failure,
}


@dag(
    dag_id="session_03_genre_daily_stats",
    description="Daily genre aggregation from plays + tracks into daily_genre_stats",
    schedule="@daily",
    start_date=datetime(2024, 3, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["session-03", "music", "homework"],
)
def genre_daily_stats():

    @task
    def transform_load(ds: str | None = None) -> int:
        hook = PostgresHook(postgres_conn_id="homework_db")
        conn = hook.get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM daily_genre_stats WHERE dt = %s", (ds,))
                cur.execute(
                    """
                    INSERT INTO daily_genre_stats (dt, genre, play_count, unique_listeners, minutes_played)
                    SELECT
                        DATE %s                        AS dt,
                        t.genre,
                        COUNT(*)                       AS play_count,
                        COUNT(DISTINCT p.user_id)      AS unique_listeners,
                        ROUND(SUM(p.ms_played) / 60000.0, 2) AS minutes_played
                    FROM plays p
                    JOIN tracks t USING (track_id)
                    WHERE p.played_at >= %s::date
                      AND p.played_at <  %s::date + INTERVAL '1 day'
                    GROUP BY t.genre
                    """,
                    (ds, ds, ds),
                )
                row_count = cur.rowcount
            conn.commit()
            log.info("Loaded %d genre rows for %s", row_count, ds)
            return row_count
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @task
    def reconcile(ds: str | None = None) -> None:
        hook = PostgresHook(postgres_conn_id="homework_db")
        source_count = hook.get_first(
            "SELECT COUNT(*) FROM plays WHERE played_at >= %s::date AND played_at < %s::date + INTERVAL '1 day'",
            parameters=(ds, ds),
        )[0]
        target_sum = hook.get_first(
            "SELECT COALESCE(SUM(play_count), 0) FROM daily_genre_stats WHERE dt = %s",
            parameters=(ds,),
        )[0]
        if source_count != target_sum:
            raise ValueError(
                f"Reconcile failed for {ds}: plays={source_count} != play_count sum={target_sum}"
            )
        log.info("Reconcile OK for %s: %d plays == %d play_count", ds, source_count, target_sum)

    transform_load() >> reconcile()


genre_daily_stats()
