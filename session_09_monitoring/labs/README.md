# Session 09 — Labs

Two hands-on labs on the monitoring stack (`session_09_monitoring/docker-compose.yml` must be running and `session_09_order_pipeline` must have completed at least once).

| Lab | Topic | Time |
|---|---|---|
| Lab 1 | Grafana Alert + Slack Webhook | 20 min |
| Lab 2 | GE Data Quality Checkpoint | 20 min |

## Rules

1. Each finding requires: **(a)** exact location, **(b)** specific risk or business impact, **(c)** working fix, **(d)** evidence of retest.
2. Do not modify demo infrastructure unless the lab explicitly requires it.

## Running lab code

```bash
pip install -r session_09_monitoring/requirements.txt

# Lab 1 needs BOTH stacks: session_07 runs Airflow (and the pipeline DAG), session_09 runs monitoring.
cd session_07_security   && docker compose up -d --build
cd session_09_monitoring && docker compose up -d
docker compose ps          # session_09 services must be "Up"
```

- Lab 1 needs both stacks running and the `session_09_order_pipeline` DAG (deployed into the
  session_07 Airflow) to have run at least once, so `af_agg_*` metrics exist in Prometheus.
- Lab 2 runs entirely on your laptop — no Docker needed. Generate the parquet files first with `cd session_09_monitoring/demo/data && python generate_data.py`.

---

## Lab 1 — Grafana Alert + Slack Webhook

The Ops team needs to be notified on Slack when the `session_09_order_pipeline` DAG run duration
exceeds SLA. Set up a Grafana alert rule that fires into a Slack channel.

To create a Slack Incoming Webhook, go to [https://api.slack.com/apps?new_app=1](https://api.slack.com/apps?new_app=1).

Demo section 4 in [`../demo/README.md`](../demo/README.md) shows the click-path for wiring a Slack
contact point. Follow that flow, then create an alert rule with these values:

| Field | Value |
|---|---|
| Alert name | `DAG Duration High — orders` |
| Metric | `af_agg_dagrun_duration_success{dag_id="session_09_order_pipeline", quantile="0.9"}` |
| Condition | last() > 300 |
| FOR | 2m |
| Severity label | `critical` |
| Annotation `runbook` | Any URL |
| Contact point | Your Slack webhook |

**Acceptance criteria:**

- [ ] `docker compose ps` shows every service `Up`, and Prometheus target `airflow` is **UP** at
      `http://localhost:9090/targets`.
- [ ] The metric appears in Prometheus: query `af_agg_dagrun_duration_success{dag_id="session_09_order_pipeline"}` at `http://localhost:9090/graph` returns results.
- [ ] The Slack **Test** from the contact point arrived in the channel.
- [ ] The alert rule exists in Grafana with the correct metric, condition `last() > 300`, and `FOR = 2m`.

---

## Lab 2 — Great Expectations Data Quality

**Ticket (DQ-118):** Finance reported that last month's revenue dashboard showed negative order
amounts and orders with no customer ID. The data came from `gold.fact_orders`. Investigate the
incoming batch file, identify what should have been rejected before loading, and build an expectation
suite that would catch it automatically next time.

You are given:
- `labs/jobs/orders_lab_suite.json` — starter suite with 2 placeholder expectations (both pass on any data)
- `labs/jobs/run_checkpoint.py` — validates a parquet file against the suite and prints per-expectation results

**Step 1 — Explore the data**

Generate the sample files if not already done:

```bash
cd session_09_monitoring/demo/data
python generate_data.py
cd ../../../
```

Inspect the incoming batch to understand what you're working with:

```bash
cd session_09_monitoring
python3 -c "
import duckdb
con = duckdb.connect()
print(con.execute('DESCRIBE SELECT * FROM read_parquet(\'demo/data/orders_broken.parquet\')').fetchall())
print(con.execute('SELECT COUNT(*) FROM read_parquet(\'demo/data/orders_broken.parquet\')').fetchone())
"
```

Run the starter suite — notice it passes even on the broken file:

```bash
cd session_09_monitoring
python3 labs/jobs/run_checkpoint.py demo/data/orders_broken.parquet
```

The starter suite has only 2 placeholder expectations: the table has at least 1 row, and the
`order_id` column exists. Both are true even for a broken batch — they say nothing about data
quality. Your job is to add expectations that actually catch what Finance reported.

**Step 2 — Build the suite**

Edit `labs/jobs/orders_lab_suite.json`. Add expectations that:
- catch the data issues Finance reported
- pass on a clean batch

Only edit the `expectations` array. Do not change `expectation_suite_name` or `meta`.

**Hint:** Finance reported two specific issues — start with those:

| Issue reported | Expectation to add | Column | kwargs |
|---|---|---|---|
| Orders with no customer ID | `expect_column_values_to_not_be_null` | `customer_id` | — |
| Orders with negative amounts | `expect_column_values_to_be_between` | `amount` | `min_value: 0.01` |

GE expectation gallery: [https://greatexpectations.io/expectations](https://greatexpectations.io/expectations)

You need at least 4 more expectations in total. Explore the data to find the rest.

**Step 3 — Verify**

```bash
cd session_09_monitoring

# Must exit non-zero, at least 3 expectations FAIL
python3 labs/jobs/run_checkpoint.py demo/data/orders_broken.parquet
echo "exit: $?"

# Must exit 0, all expectations PASS
python3 labs/jobs/run_checkpoint.py demo/data/orders_clean.parquet
echo "exit: $?"
```

**Acceptance criteria:**

- [ ] `labs/jobs/orders_lab_suite.json` has at least 6 expectations
- [ ] Running against `orders_broken.parquet` exits non-zero and at least 4 expectations show `FAIL`
- [ ] Running against `orders_clean.parquet` exits 0 and all expectations show `PASS`
- [ ] Each added expectation has a clear business reason (add a `"comment"` field in `meta` to explain)
