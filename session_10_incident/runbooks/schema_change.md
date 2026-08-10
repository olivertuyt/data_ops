# Runbook: Upstream Schema Change — Column Renamed in Source CSV

## Symptom

- `reconcile__revenue__fact_daily_revenue` task fails with check returned `False` (revenue = 0)
- All upstream Spark tasks (`raw2bronze__marketplace_orders`, `bronze2silver__orders`, `silver2gold__fact_daily_revenue`) are **green**
- Row counts in silver and gold are normal

## Trace in Marquez

Marquez registers datasets in two separate namespaces: `s3a://` (Airflow provider) and `s3://` (Spark listener). The schema change signal lives in the Spark-registered datasets under the `s3://` namespaces.

1. Open `http://localhost:3000` → click **Datasets** icon (left sidebar)
2. Select namespace **s3://gold** → click **fact_daily_revenue** row → **VIEW**
   - Graph shows: `/orders` → JOB `silver2gold___fac...` → **`fact_daily_revenue`** (focused)
   - `/orders` node field list shows `amount` — the column exists but carried null values from silver
3. Switch namespace to **s3://silver** → click **orders** → **VIEW**
   - Graph shows the silver `orders` dataset and what produced it
4. Switch namespace to **s3://bronze** → click **marketplace_orders** → **VIEW**
   - **The field list on the node shows `revenue` instead of `amount`** — this is the signal
   - OpenLineage (Spark listener) recorded the bronze job's output schema at the incident run: `amount` was replaced by `revenue` in the source CSV
   - **Root cause confirmed**: upstream renamed `amount` → `revenue`. Bronze accepted the renamed column, silver still selected `amount` (now null), gold summed nulls to 0.

## Fix

```bash
# 1. Restore the correct column name in the source file
python scripts/inject_incident.py --restore

# 2. Rerun all tasks for the incident date
docker exec session_10_incident-airflow-worker-1 \
  airflow tasks clear session_10_order_pipeline -s 2026-06-15 -e 2026-06-16 -y
```

## Verify

- All tasks turn green in Airflow UI
- Marquez bronze schema: `revenue` column no longer appears in new run
- `reconcile__revenue__fact_daily_revenue` passes (Trino query returns `True`)
- `total_revenue > 0` for the incident date

## Escalate If

- Source file on partner side still has the wrong column name → contact partner's data team
- Pipeline still fails after restore → check `bronze2silver__orders` logs for other schema issues
- Recurring pattern on the same partner → add a schema contract check in `raw2bronze__marketplace_orders`
