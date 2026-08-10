# Session 3 — Writing DAGs & Advanced Concepts

**Master Class DataOps for Modern Data Platforms · Session 3/11**

Session 2 got Airflow running. This session is about writing DAGs that are *operable*: safe to rerun, safe to backfill, and able to fail without falling over. We build one small **ad-campaign analytics** pipeline — raw ad events rolled up into daily per-campaign metrics — and use it to practise every idea, rather than scattering unrelated demos.

The pipeline runs on the **same Airflow stack you built in session 2** — session 3 adds only DAG code, no new infrastructure.

## What you will learn

- **Idempotency** — a task rewrites its date's partition instead of appending, so a rerun produces the same result as a single run.
- **Atomicity** — a partition write lands via a temp file + atomic rename (`write_partition`), so a crash mid-write never leaves a half-written partition behind.
- **Determinism** — events are keyed off the run's logical date, never `datetime.now()`, so a backfill for an old date reproduces exactly what that date should hold.
- **Scheduling** — `@daily`, a fixed `start_date`, `catchup=False`, and `max_active_runs=1`, and how backfill interacts with them.
- **Fault tolerance** — `retries`, `retry_delay`, and per-task `execution_timeout`; clearing one task reruns only that task.
- **SLA & alerting** — a per-task `sla` with an `sla_miss_callback`, plus an `on_failure_callback` to Slack.
- **Sensors** — waiting for an upstream dependency with `ExternalTaskSensor` in `reschedule` mode (frees the worker slot while waiting).
- **Runtime config** — reading a `Variable`, using a `Connection` by `conn_id` (no hardcoded credentials), and a `Pool` to cap concurrency on a shared resource.

## DAGs

| DAG | Location | Role |
|---|---|---|
| `session_03_campaign_daily_metrics` | `demo/` | The reference pipeline: `ingest → rollup → publish → reconcile`, carrying most of the operational concepts above. Mentor-run — see [demo/README.md](demo/README.md). |
| `session_03_campaign_spend_report` | `demo/` | A downstream report owned by finance; waits for the metrics pipeline that date, then flags over-budget campaigns. |
| `session_03_campaign_alerts` | `lab/` | A **starter DAG you complete** — flags low-CTR campaigns. See the Exercise in [lab/README.md](lab/README.md). |

Data flows through partition layers written to `data/`:

```
raw/ad_events/dt=YYYY-MM-DD                 # one row per impression
curated|published/campaign_metrics/dt=…     # per-campaign rollup (impressions, clicks, spend, ctr)
reports/campaign_spend/dt=…                 # daily spend + over-budget flags
```

Shared helpers (`storage`, `sample_data`, `notifications`) are **not** copied into this session — they live in the repo-level `plugins/dataops_common/` package and are imported by DAGs across every session.

## Directory structure

```
session_03_airflow_dag/
├── README.md            # this file — session overview
├── demo/                # mentor-run material
│   ├── README.md        # demo run-book (corrupt → reconcile catches → alert)
│   └── dags/            # campaign_daily_metrics, campaign_spend_report
├── lab/
│   ├── README.md        # setup guide + the exercise you complete
│   └── dags/            # campaign_alerts (starter)
├── data/                # partition output (raw / curated / published / reports) — gitignored
└── homework/            # take-home: DB→DB pipeline, submitted via Pull Request
    ├── README.md        # assignment: requirements + setup + submission
    ├── seed/            # source DB schema + deterministic seed data
    └── submissions/     # students add their DAG here (PR is review-only, not merged)

plugins/                 # repo-level, shared across sessions
└── dataops_common/      # storage.py, sample_data.py, notifications.py
```

## Where to start

Follow [lab/README.md](lab/README.md): it reuses the session 2 stack, adds a one-time Pool/Variable setup, and walks through running the reference pipeline and completing the exercise. The mentor demo has its own run-book in [demo/README.md](demo/README.md).

## Homework

[homework/README.md](homework/README.md) — a take-home pipeline that reads from a **relational database** (a music-streaming schema in Postgres), joins + aggregates, and writes back to a table, applying every principle above. Submitted by opening a Pull Request.

## Position in the course

| | |
|---|---|
| Previous | Session 2 — Airflow architecture & deployment |
| This session | Writing operable DAGs: idempotency, backfill, retries, sensors, cross-DAG deps |
| Next | Session 4 — SQL in DataOps: the Data Warehouse stack |
