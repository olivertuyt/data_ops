# Session 09 — Monitoring, Alerting & Data Quality

Instrument an Airflow pipeline with Prometheus metrics, visualise them in Grafana, and gate data quality with Great Expectations before each load.

## Stack

| Tool | Role |
|---|---|
| Prometheus + StatsD Exporter | Scrape and store Airflow metrics |
| Grafana 10 | Dashboards + provisioned alert rules |
| Great Expectations 0.18 | In-pipeline data quality checkpoint |
| Slack Webhook | Alert delivery |
| DuckDB | Lightweight warehouse (bronze / silver / gold) |

## Structure

```
session_09_monitoring/
├── demo/                         # order pipeline DAG + GE suites + sample data
│   ├── dags/session_09_order_pipeline.py
│   ├── dags/sql/session_09_order_pipeline/
│   ├── great_expectations/       # expectation suites
│   └── data/                     # orders_clean.parquet, orders_broken.parquet
├── grafana/
│   ├── dashboards/               # Airflow cluster + DAG dashboards
│   └── provisioning/             # datasources, dashboards, alerting (5 alert rules)
├── labs/                         # Lab 1: Grafana alert, Lab 2: GE checkpoint
├── prometheus/prometheus.yml
├── statsd/statsd.yaml            # StatsD → Prometheus metric mappings
├── docker-compose.yml            # prometheus + statsd-exporter + grafana
└── requirements.txt              # host deps for data generation and lab runner
```

## Quick start

```bash
# 1. Start the monitoring stack (Prometheus + StatsD Exporter + Grafana)
cd session_09_monitoring
docker compose up -d
docker compose ps

# 2. Start the Airflow stack (session_07 — hosts the pipeline DAG)
cd session_07_security
docker compose up -d --build   # --build picks up duckdb + great-expectations

# 3. Verify endpoints
#    Prometheus:     http://localhost:9090/targets   (airflow job must be UP)
#    Grafana:        http://localhost:3000            (admin / admin)
#    StatsD metrics: http://localhost:9102/metrics

# 4. Generate sample data (run once; files are committed)
pip install -r session_09_monitoring/requirements.txt
cd session_09_monitoring/demo/data
python generate_data.py
```

See [`demo/README.md`](demo/README.md) for the full demo walkthrough and
[`labs/README.md`](labs/README.md) for lab instructions.

## Common errors

| Error | Cause | Fix |
|---|---|---|
| Grafana alert rules missing | Old anonymous volume from prior run | `docker compose down -v && docker compose up -d` |
| `airflow` target DOWN in Prometheus | StatsD env vars not set in Airflow | Add `AIRFLOW__METRICS__STATSD_ON=True` and `STATSD_HOST=statsd-exporter` to session_07 env |
| `session_09_order_pipeline` DAG not visible | session_07 image not rebuilt | `docker compose up -d --build` in session_07_security |
| `duckdb.connect` permission error | Another process holds the write lock | Stop conflicting task, or open with `read_only=True` |
