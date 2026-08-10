# Master Class — DataOps for Modern Data Platforms

Hands-on course repository. Each session has its own directory with everything you need to practice: a runnable stack, sample code, and setup guides.

The course is built around one idea: a data pipeline doesn't just need to *run* — it needs to be **operable**. Everything we build comes back to the six properties of a safe data pipeline:

| Property | In one line |
|---|---|
| **Idempotency** | Rerun N times → same result as running once |
| **Atomicity** | All or nothing — no half-written state |
| **Determinism** | Same input + same logic → same output, whenever it runs |
| **Fault Tolerance** | Partial failure doesn't take everything down — retry only what failed |
| **Reconciliation** | Source vs target checks that data is correct and complete |
| **Reproducibility** | Any result can be rebuilt from scratch with the same input & config |

## Sessions

| # | Session | Main stack |
|---|---|---|
| 1 | DataOps mindset & operating principles | — (theory) |
| 2 | [Introduction to Airflow: architecture & deployment](session_02_airflow_intro/) | Airflow · Docker Compose · Celery |
| 3 | Writing DAGs & advanced concepts (idempotency, backfill, retry, XCom) | Airflow · Python TaskFlow API |
| 4 | SQL in DataOps — the Data Warehouse stack | DuckDB · SQL · Airflow |
| 5 | PySpark in the modern Lakehouse | PySpark · Delta Lake / Iceberg |
| 6 | Performance & cost optimization | Spark UI · query plans · cost tools |
| 7 | Security & sensitive data | Secrets management · PII masking |
| 8 | CI/CD for data pipelines | Git · GitHub Actions · pytest |
| 9 | Monitoring, alerting & data quality | Prometheus · Grafana · Great Expectations |
| 10 | Incidents, RCA & recovery | Lineage · runbooks · LLM agents |
| 11 | Final project & demo | Everything above |

Session directories are added as the course progresses.

## How a session directory is organized

```
session_XX_topic/
├── README.md    # session overview — read this first
├── lab/         # your hands-on part: runnable stack + setup guide
└── demo/        # mentor-driven demos (observe only, no setup required)
```


## Prerequisites

- **Docker Desktop** with at least **4GB RAM** allocated (Settings → Resources) — most sessions run their stack in containers.
- **Python 3.10+** for reading and editing pipeline code.
- Basic comfort with a terminal and Git.

## Getting started

```bash
git clone <this repo>
cd master-class-dataops/session_02_airflow_intro
```

Open the session's `README.md` and follow it. Build the session 2 stack carefully — it is reused throughout the rest of the course.
