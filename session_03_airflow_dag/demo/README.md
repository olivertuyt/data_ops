# Session 3 Demo — Mentor Run-Book

Two DAGs the mentor runs and walks through — the reference pipeline and its
downstream report. Neither is a student exercise (that lives in [`../lab/`](../lab/README.md)).
Every command below has been run end to end.

All `docker compose` commands run from `session_02_airflow_intro/lab`.

## The two demo DAGs

| DAG | Role |
|---|---|
| `session_03_campaign_daily_metrics` | Reference pipeline: `ingest → rollup → publish → reconcile`, carrying every operational concept — idempotent partition writes, deterministic data, retries + timeouts, SLA + callbacks, a Pool, and a Variable. |
| `session_03_campaign_spend_report` | Downstream finance report — waits for the metrics pipeline that date via a cross-DAG `ExternalTaskSensor`, then flags over-budget campaigns. |

Run the reference pipeline for a date first; the report and both labs read the
`published` partition it produces.

## Before class

Bring the session 2 stack up so the session 3 mounts are live, and confirm the
DAGs loaded:

```bash
cd session_02_airflow_intro/lab
docker compose --profile flower up -d
docker compose exec airflow-scheduler ls /opt/airflow/dags/session_03
# → campaign_daily_metrics.py  campaign_spend_report.py
```

Do the one-time Pool/Variable setup from
[`../lab/README.md` §1](../lab/README.md) (the `campaign_db_pool` pool is required —
`publish` won't schedule without it). For a **real** Slack alert in the demo below,
set the `slack_webhook_url` Variable; without it the alert just logs.

---

## Demo — a real alert from a real failure

Fixed, single-path script, run against the real reference pipeline — not a
throwaway DAG. It doubles as the answer to "why does `reconcile` even exist?": you
corrupt the exact thing it guards, then watch it catch the corruption, retry, fail,
and alert Slack — for real.

> **Use `airflow dags trigger`, NOT `airflow dags backfill`.** Clearing a task
> inside a *backfill* run is **not** picked up by the scheduler — the task sits
> stuck with no status and the demo dies. A triggered (manual) run is
> scheduler-managed, so a cleared task reruns normally. This is the one thing that
> will break the demo if you get it wrong.

The demo uses date `2025-06-01` — one no exercise touches. Have the Airflow UI open
at the Grid view of `session_03_campaign_daily_metrics`, Slack channel beside it.

### 1. Run the pipeline clean

```bash
docker compose exec airflow-scheduler airflow dags unpause session_03_campaign_daily_metrics
docker compose exec airflow-scheduler airflow dags trigger session_03_campaign_daily_metrics -e 2025-06-01
```

In Grid view the run goes all green (`ingest → rollup → publish → reconcile`) in ~40s.

### 2. Corrupt the published partition

Simulating a downstream process dropping one impression *after* `publish` ran.
From the **repo root**:

```bash
python3 - <<'PY'
import json, pathlib
p = pathlib.Path("session_03_airflow_dag/data/published/campaign_metrics/dt=2025-06-01/data.json")
rows = json.loads(p.read_text())
rows[0]["impressions"] -= 1
p.write_text(json.dumps(rows, indent=2, sort_keys=True))
print("corrupted:", rows[0]["campaign"], "→ impressions now", rows[0]["impressions"])
PY
```

Only `published` changes; `raw` and `curated` stay correct — `published` is the
layer `reconcile` checks.

### 3. Clear only `reconcile`

In the UI: Grid view → the `2025-06-01` run → click the `reconcile` box → **Clear** →
confirm. (This is also the live "clear one task, only that task reruns" example.)
CLI equivalent, from `session_02_airflow_intro/lab`:

```bash
docker compose exec airflow-scheduler airflow tasks clear session_03_campaign_daily_metrics -t reconcile -s 2025-06-01 -e 2025-06-01 -y
```

### 4. Watch it catch the corruption

`reconcile` reruns, recomputes from the corrupted partition, the counts don't
match → it raises, and the task log shows the real error:

```
ValueError: Reconciliation failed for ad_events/2025-06-01: source=30, target=29
```

### 5. Narrate the retry window — don't wait in silence

`reconcile` inherits `retries=2, retry_delay=1min`: it fails (`up_for_retry`), waits
~1 min, retries, fails, retries once more, fails — **~2 minutes total** — then gives
up (`failed`). Point at the attempt history in the UI; this is the retry behavior
every task in the DAG has.

### 6. The Slack alert

On the **final** failure `on_failure_callback` fires and posts to Slack:

```
🔴 *session_03_campaign_daily_metrics* › `reconcile` failed
Run: `manual__2025-06-01T00:00:00+00:00` | Date: `2025-06-01T00:00:00+00:00`
```

Confirm it fired in the `reconcile` task log — look for a `{notifications.py}` line
(a successful post logs nothing; a failed post logs `Slack post failed (non-fatal): ...`).
If no message arrives, read the `reconcile` task log:
- `slack_webhook_url not set — skipping` → Variable missing/empty; set it (§1), redo step 3.
- `Slack post failed (non-fatal): ...` → webhook URL is wrong or revoked; recreate it (§1), redo step 3.

### 7. Fix the data

Clear `publish` (it reruns `reconcile` downstream too):

```bash
docker compose exec airflow-scheduler airflow tasks clear session_03_campaign_daily_metrics -t "publish|reconcile" -s 2025-06-01 -e 2025-06-01 -y
```

`publish` rewrites the partition from `curated` (idempotent overwrite), `reconcile`
passes, the run is green again. Say it out loud: recovering from a bad state here is
just "rerun it" — that is the payoff of idempotency, not luck.

> **SLA alerts (`sla_miss_callback`) are a separate mechanism and not part of this
> demo**: Airflow only evaluates SLA misses on *scheduled* runs, never on manual
> triggers or backfills — there's no way to force one on demand. Walk through the
> `sla=timedelta(...)` config and `notify_on_sla_miss` code instead of trying to
> trigger it live.

---

## Optional — show the cross-DAG dependency (`campaign_spend_report`)

`session_03_campaign_spend_report` waits for the metrics pipeline via
`ExternalTaskSensor`. The sensor waits for the **same logical date** in
`session_03_campaign_daily_metrics`, so trigger the report for the **same date** you
ran metrics — otherwise it waits on a date that has no run:

```bash
docker compose exec airflow-scheduler airflow dags unpause session_03_campaign_spend_report
docker compose exec airflow-scheduler airflow dags trigger session_03_campaign_spend_report -e 2025-06-01
```

`wait_for_metrics` sits in `up_for_reschedule` (reschedule mode frees the worker
slot between checks) until `reconcile` of that date succeeds, then `flag_over_budget`
runs and writes `reports/campaign_spend/dt=2025-06-01/`.
