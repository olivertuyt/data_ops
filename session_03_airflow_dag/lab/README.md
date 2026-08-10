# Session 3 Lab — Ad-Campaign Metrics

**Master Class DataOps for Modern Data Platforms · Session 3/11**

You work on the same Airflow stack from session 2 — nothing new to start.

What you do in this lab:

- **Labs 1–2** — drive a ready-made pipeline (`session_03_campaign_daily_metrics` and its downstream report) to *see* the ideas in action: idempotency, backfill, XCom, and a cross-DAG sensor. You don't build a DAG here — you run and observe.
- **Lab 3** — the one you build: finish the starter DAG `session_03_campaign_alerts`.

> The mentor's failure-and-alert demo is separate — it has its own run-book in [`../demo/README.md`](../demo/README.md).

**Where commands run:** every `docker compose ...` command runs from `session_02_airflow_intro/lab`. Every `cat session_03_airflow_dag/...` command runs from the **repo root**.

---

## 1. Setup (once)

**Prerequisites:** the session 2 stack is healthy (`docker compose ps`), and ports `8080` (Airflow UI) and `5555` (Flower) are reachable.

**Start the stack** (this also applies the session 3 mounts):

```bash
cd session_02_airflow_intro/lab
docker compose --profile flower up -d

# confirm the DAGs loaded
docker compose exec airflow-scheduler ls /opt/airflow/dags/session_03 /opt/airflow/dags/session_03_lab
# session_03:     campaign_daily_metrics.py  campaign_spend_report.py
# session_03_lab: campaign_alerts.py
```

Editing any DAG under `session_03_airflow_dag/**/dags/` is picked up automatically (~30s rescan) — no restart needed.

**Create the Pool and Variables** once, in the UI (`http://localhost:8080`, `airflow` / `airflow`) or via the CLI below:

| Type | Name | Value | Required? |
|---|---|---|---|
| Pool | `campaign_db_pool` | 2 slots | **Yes** — `publish` won't run without it |
| Variable | `daily_event_volume` | `30` | No (defaults to 30) |
| Variable | `daily_budget_usd` | `5.0` | No (used by `campaign_spend_report`) |
| Variable | `min_ctr` | `0.15` | No (used by your Lab 3 DAG) |
| Variable | `slack_webhook_url` | your Slack webhook URL | No (alerts just log if missing) |

```bash
docker compose exec airflow-scheduler airflow pools set campaign_db_pool 2 "Session 3 campaign publish"
docker compose exec airflow-scheduler airflow variables set daily_event_volume 30
```

> The `homework_db` Connection (a real one, with credentials) shows up in the homework — that's where you actually connect to a database by `conn_id`.

On the optional Slack alert:

- **`slack_webhook_url`** turns the log-only alerts into real Slack messages. To get a URL: Slack → [api.slack.com/apps](https://api.slack.com/apps) → your app → **Incoming Webhooks** → **Add New Webhook to Workspace** → pick a channel. Then `airflow variables set slack_webhook_url 'https://hooks.slack.com/services/...'`. Keep the URL in the Variable only — never in code.

---

## 2. Run the reference pipeline first

Labs 1–3 all read the data this pipeline produces, so run it once for a date.

In the UI, unpause `session_03_campaign_daily_metrics` and trigger it for **2024-02-10** (or via CLI):

```bash
docker compose exec airflow-scheduler airflow dags trigger session_03_campaign_daily_metrics -e 2024-02-10
```

Watch `ingest → rollup → publish → reconcile` go green. Then check the output (from the repo root):

```bash
cat session_03_airflow_dag/data/published/campaign_metrics/dt=2024-02-10/data.json
# → 3 campaigns, each with impressions / clicks / spend_usd / ctr
```

---

## 3. Lab 1 — Idempotency, backfill & XCom

**Goal:** see why the pipeline is safe to rerun and backfill.

1. You already ran `2024-02-10` above. Note the `reconcile` log line: `30 events == 30 impressions`.
2. **Rerun the same day.** You can't re-`trigger` a date that already ran (Airflow refuses it: `DagRunAlreadyExists`), so rerun it the real way — in the Grid view, select the `2024-02-10` run and click **Clear**, or:
   ```bash
   docker compose exec airflow-scheduler airflow tasks clear session_03_campaign_daily_metrics -s 2024-02-10 -e 2024-02-10 -y
   ```
   It reruns and the output is still **3 campaigns / 30 impressions**, not doubled. That's idempotency: the load overwrites the day's partition instead of appending. (A rerun in production is a Clear, not a fresh trigger — same muscle memory.)
3. **Backfill three days:**
   ```bash
   docker compose exec airflow-scheduler airflow dags backfill session_03_campaign_daily_metrics -s 2024-02-01 -e 2024-02-03 --reset-dagruns
   ```
   Check that each day got its own partition, with its own data:
   ```bash
   cat session_03_airflow_dag/data/published/campaign_metrics/dt=2024-02-01/data.json
   cat session_03_airflow_dag/data/published/campaign_metrics/dt=2024-02-02/data.json
   cat session_03_airflow_dag/data/published/campaign_metrics/dt=2024-02-03/data.json
   ```
4. **XCom:** in the Grid view, click the `ingest` task → **XCom** tab. It pushed a single number (the event count) — not the whole list of events.

**Think about:**
- Why is writing to a `dt=YYYY-MM-DD` partition what makes backfill safe?
- XCom is stored in the metadata DB. Why push only a count/path through it, never the full list of events?

---

## 4. Lab 2 — Cross-DAG dependency & sensor

**Goal:** see one DAG wait for another. `session_03_campaign_spend_report` waits for `session_03_campaign_daily_metrics` to finish the same day before it runs.

1. Enable both DAGs.
2. Trigger `session_03_campaign_spend_report` for **2024-02-11** — a day the metrics pipeline hasn't run yet:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger session_03_campaign_spend_report -e 2024-02-11
   ```
3. Its `wait_for_metrics` task sits in **`up_for_reschedule`** — waiting, without holding a worker slot, without failing.
4. Now trigger the metrics pipeline for the **same day** and let `reconcile` finish:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger session_03_campaign_daily_metrics -e 2024-02-11
   ```
5. `wait_for_metrics` passes on its own, then `flag_over_budget` runs and writes `reports/campaign_spend/dt=2024-02-11`.

**Think about:**
- Why point the sensor at the **last** task (`reconcile`) instead of the first?
- Why is `mode="reschedule"` better than the default `poke` when the wait can be long?

---

## 5. Lab 3 — Build the alert DAG

**This is the one you write.** `session_03_campaign_alerts` is a starter DAG — the wiring (sensor, task, dependency) is done; the logic in `flag_low_ctr` is left as 7 `# TODO`s. Finish it so it flags campaigns whose CTR is below a threshold.

Read [`../demo/dags/campaign_spend_report.py`](../demo/dags/campaign_spend_report.py) first — it's the same shape with a different metric.

The 7 TODOs, in order:

1. Read the Variable `min_ctr` (default `0.15`) as a float.
2. Read the published partition: `read_partition("published", "campaign_metrics", logical_date)`.
3. Keep the campaigns with `ctr < min_ctr`, and sum their `spend_usd` (the "wasted" spend).
4. Build the report dict: `date`, `min_ctr`, `low_ctr_campaigns` (list of names), `wasted_spend_usd`.
5. Write it: `write_partition("reports", "campaign_low_ctr", logical_date, [report])` — one row, overwrites on rerun (idempotent).
6. `log.info(...)` the report and return it.
7. Wire the dependency: `wait_for_metrics >> alert`.

**Test it:**

```bash
docker compose exec airflow-scheduler airflow dags trigger session_03_campaign_alerts -e 2024-02-10
cat session_03_airflow_dag/data/reports/campaign_low_ctr/dt=2024-02-10/data.json
```

**Done when:**
- The DAG runs green (no `NotImplementedError`).
- `flag_low_ctr` runs only after `wait_for_metrics` succeeds.
- With the default data, only `brand_awareness` (CTR 0.10) is flagged — `prospecting` (0.30) and `retargeting` (0.50) are not.
- Re-running the same day (Clear the run — a second `trigger` for the same date is refused) leaves exactly one row in the partition (idempotent).

---

## 6. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `session_03_*` DAGs missing in UI | Stack not restarted after mounts were added | `docker compose --profile flower up -d` in `session_02_airflow_intro/lab` |
| `ModuleNotFoundError: dataops_common` | Plugins mount not applied | Confirm `../../plugins` is mounted; restart the stack |
| `publish` stuck, never runs | Pool `campaign_db_pool` doesn't exist | Create it (section 1) |
| Import error on a DAG | Edited while the scheduler was rescanning | Wait ~30s; check the import-error banner in the UI |
| No data under `data/` | Data volume not mounted | Confirm `session_03_airflow_dag/data` maps to `/opt/airflow/data` |
