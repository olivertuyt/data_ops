# Session 4 Demo — Run-Book

Two demos, in order — both real Airflow DAGs (operators + external `.sql`,
triggered like any other pipeline), not standalone scripts. Every command and
every expected output below has been run end to end.

All `docker compose` commands run from `session_02_airflow_intro/lab`.

> The `python -c "import duckdb..."` snippets below are copy-paste-from-terminal
> inspection, nothing more — the warehouse file is a plain file on your **host**
> (`db/dwh.duckdb`), so DBeaver or any DuckDB client works just as well, without
> `docker exec` at all. See [../README.md](../README.md#querying-the-warehouse-from-dbeaver-or-any-duckdb-client-on-your-host).

## Why these two demos, and in this order

Session 4 covers 5 of the 6 pipeline properties from session 1 — Idempotency,
Atomicity, Determinism, Fault Tolerance, Reconciliation (only Reproducibility is
still missing; that needs snapshots/time travel, saved for session 6). The
through-line to repeat across both demos: **a pipeline finishing without error is
not proof the data is correct.**

1. **Demo 1** isolates Idempotency and Atomicity on one small, realistic load task
   — one DAG, one task, one committed `.sql` file. Nothing here is a toy: the same
   task, the same trigger/clear commands, the same "did the count change" check is
   exactly how you'd verify these properties on a real pipeline.
2. **Demo 2 (the reference pipeline)** is where all 5 properties converge in one
   real, multi-task DAG, and it resolves what demo 1 sets up: `reconcile` is the
   thing that catches what "the task succeeded" alone can't.

## Before class

Build the image (base Airflow + DuckDB) and bring the stack up so the session 4
mounts are live:

```bash
cd session_02_airflow_intro/lab
docker compose build      # one-time; skips instantly if already built
docker compose up -d
docker compose exec airflow-scheduler python -c "import duckdb; print(duckdb.__version__)"
```

Have the Airflow UI open (`http://localhost:8080`, `airflow` / `airflow`) at the Grid
view of `session_04_orders_medallion_etl`. For the Slack alert in Demo 2, set the
`slack_webhook_url` Variable (see `../lab/README.md` wording, same as session 3) —
without it the alert just logs instead of posting.

---

## Demo 1 — Hardening a load: idempotency, then atomicity
**Unpause Airflow DAG:**

By CLI:
```bash
docker compose exec airflow-scheduler airflow dags unpause session_04_demo_write_patterns
```

```bash
# from the repo root
SQL=session_04_sql_dwh/demo/dags/sql/demo_write_patterns/load_day.sql
```

Every `docker compose exec` / `airflow dags|tasks` command still runs from
`session_02_airflow_intro/lab` as stated above — just switch directories between
the two kinds of commands.

### Step 1 — baseline

```bash
docker compose exec airflow-scheduler airflow dags trigger session_04_demo_write_patterns -e 2024-07-01
```

Check the count:

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True)
print(c.execute(\"SELECT COUNT(*) FROM demo.write_patterns WHERE order_date='2024-07-01'\").fetchone()[0])
"
# → 3
```

### Step 2 — Idempotency: naive INSERT duplicates on rerun

Overwrite the file with the anti-pattern:

```bash
cat > "$SQL" <<'SQL'
CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.write_patterns (
    order_id INTEGER, amount DOUBLE, order_date DATE
);
INSERT INTO demo.write_patterns VALUES
    (1, 10.0, '{{ ds }}'),
    (2, 20.0, '{{ ds }}'),
    (3, 30.0, '{{ ds }}');
SQL
```

**Rerun the same date the way you actually would in Airflow** — clear the task,
don't re-trigger (`dags trigger` refuses a second run for a logical date that
already has one; `tasks clear` is the real mechanism for "run this day again"):

```bash
docker compose exec airflow-scheduler airflow tasks clear session_04_demo_write_patterns -t load_day -s 2024-07-01 -e 2024-07-01 -y
```

Then check the count again.

### Step 3 — Atomicity: the same failure, with and without a transaction

Overwrite with a version that adds a new statement for "today's source has one bad
row", **without** wrapping it:

```bash
cat > "$SQL" <<'SQL'
CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.write_patterns (
    order_id INTEGER, amount DOUBLE, order_date DATE
);
DELETE FROM demo.write_patterns WHERE order_date = '{{ ds }}';
INSERT INTO demo.write_patterns VALUES
    (1, 10.0, '{{ ds }}'),
    (2, 20.0, '{{ ds }}'),
    (3, 30.0, '{{ ds }}'),
    (4, CAST('not-a-number' AS DOUBLE), '{{ ds }}')
;
SQL

docker compose exec airflow-scheduler airflow tasks clear session_04_demo_write_patterns -t load_day -s 2024-07-01 -e 2024-07-01 -y
```

The task goes **red** (`ConversionException` in the log) — expected. Check the
count → **0**. 🔴 **Symptom:** the `DELETE` already committed before the bad
`INSERT` threw — the whole day is gone, and the task failing doesn't tell you that
by itself.

Restore the committed file and reset the baseline:

```bash
git checkout -- "$SQL"
docker compose exec airflow-scheduler airflow tasks clear session_04_demo_write_patterns -t load_day -s 2024-07-01 -e 2024-07-01 -y
# count → 3 again
```

Now repeat the exact same bad row, but on top of the **real, committed**
(transaction-wrapped) file — add one line to the version already in the repo:

```bash
cat > "$SQL" <<'SQL'
CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.write_patterns (
    order_id INTEGER, amount DOUBLE, order_date DATE
);
BEGIN;
DELETE FROM demo.write_patterns WHERE order_date = '{{ ds }}';
INSERT INTO demo.write_patterns VALUES
    (1, 10.0, '{{ ds }}'),
    (2, 20.0, '{{ ds }}'),
    (3, 30.0, '{{ ds }}'),
    (4, CAST('not-a-number' AS DOUBLE), '{{ ds }}')
;
COMMIT;
SQL

docker compose exec airflow-scheduler airflow tasks clear session_04_demo_write_patterns -t load_day -s 2024-07-01 -e 2024-07-01 -y
```

The task **still goes red** — the bad row is still bad, atomicity doesn't fix
data quality — but check the count → **still 3**. ✅ **Fix confirmed:** the failed
`INSERT` never committed, so `COMMIT` never ran, so DuckDB discarded the whole
block including the `DELETE`. The table is exactly as it was.

Restore and move on:

```bash
git checkout -- "$SQL"
docker compose exec airflow-scheduler airflow tasks clear session_04_demo_write_patterns -t load_day -s 2024-07-01 -e 2024-07-01 -y
```

**Narrate:** a bad row reaching the load is exactly what session 4's Bronze→Silver
validation step exists to prevent (see demo 2). Atomicity doesn't stop bad data
from arriving — it stops a failure from destroying what was already there.

**MERGE** (the third idempotent pattern, upsert-by-key) isn't re-demoed here —
`session_04_orders_medallion_etl`'s `build_dim_customer` task in demo 2 already is
a real one, on real data.

---

## Demo 2 — The reference pipeline, and what `reconcile` is.

> This is my example, a business rule that I defined. In reality, it depends on your specific context and logic.😁

The full Medallion pipeline in Airflow. We run it clean, show it's idempotent, then
corrupt the exact thing `reconcile` guards and watch it catch the corruption, retry,
fail, and alert. Uses date `2024-06-01` (no lab touches it).

> **Use `airflow dags trigger`, not `backfill`.** A task cleared inside a *backfill*
> run is not rescheduled — it hangs with no status and the demo dies. A triggered
> (manual) run is scheduler-managed, so a cleared task reruns normally.

### 2a. Run it clean

```bash
docker compose exec airflow-scheduler airflow dags unpause session_04_orders_medallion_etl
docker compose exec airflow-scheduler airflow dags trigger session_04_orders_medallion_etl -e 2024-06-01
```

In Grid view the run goes all green in ~25s:
`create_schema → load_bronze → validate_to_silver → build_dim_customer → build_fact_sales → reconcile`.

Show the layers — 400,002 raw rows in, 400,000 survive validation (two dirty rows
dropped), 400,000 land in the fact (across 5,000 customers in `dim_customer`):

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True)
d='2024-06-01'
print('bronze raw :', c.execute('SELECT COUNT(*) FROM bronze.orders_raw WHERE order_date=?',[d]).fetchone()[0])
print('silver     :', c.execute('SELECT COUNT(*) FROM silver.orders WHERE order_date=?',[d]).fetchone()[0])
print('fact_sales :', c.execute('SELECT COUNT(*) FROM gold.fact_sales WHERE order_date=?',[d]).fetchone()[0])
"
```

### 2b. Show it's idempotent

Clear the whole run for that date and re-check — the fact still has 400,000 rows, not
800,000. `validate_to_silver` still drops exactly the same 2 dirty rows (400,002 raw →
400,000 kept).

```bash
docker compose exec airflow-scheduler airflow tasks clear session_04_orders_medallion_etl -s 2024-06-01 -e 2024-06-01 -y
```

Rerunning a date is safe — that is the whole point of
`DELETE`-by-date + `MERGE` (`MERGE` statement can be used if we want to compare data with keys before merging the new data.).

### 2c. Break the data, watch `reconcile` catch it

Simulate a bad write landing in the fact *after* it was built — one order's amount
inflated by 100, so the fact no longer ties back to silver:

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb')
c.execute(\"UPDATE gold.fact_sales SET amount = amount + 100 WHERE order_date='2024-06-01' AND order_id = (SELECT MIN(order_id) FROM gold.fact_sales WHERE order_date='2024-06-01')\")
print('corrupted one fact row (+100)')
"
```

Now **clear only `reconcile`** for that date (this is also the live "clear one task,
only that task reruns" example):

```bash
docker compose exec airflow-scheduler airflow tasks clear session_04_orders_medallion_etl -t reconcile -s 2024-06-01 -e 2024-06-01 -y
```

`reconcile` (a `SQLCheckOperator`) reruns `reconcile.sql`, the `sum_match` column
comes back false, and the check fails. The task log shows the failing row:

```
airflow.exceptions.AirflowException: Test failed.
Query: SELECT ... AS count_match, ... AS sum_match, ... AS rules_ok, ... AS dod_ok
Results: (True, False, True, True)
```

(`sum_match = False` — the fact's summed amount for that date is now $100 higher than
silver's, so the two no longer tie. The other three checks — row-count match, business
rules, and the day-over-day volume guard — still hold, so only `sum_match` trips.)

**Narrate the retry window — don't wait in silence.** `reconcile` inherits
`retries=2, retry_delay=1min`: it fails, waits ~1 min, retries, fails, retries once
more, fails — **~2 minutes total** — then gives up (`failed`). On the final failure
`on_failure_callback` fires and posts to Slack:

```
🔴 *session_04_orders_medallion_etl* › `reconcile` failed
Run: `manual__2024-06-01T00:00:00+00:00` | Date: `2024-06-01T00:00:00+00:00`
```

(If no Slack message arrives, read the `reconcile` task log: `slack_webhook_url not
set — skipping` means the Variable is missing; set it and clear `reconcile` again.)

### 2d. Recover

Rebuild the fact from silver (the idempotent way) and re-check — clearing
`build_fact_sales` reruns `reconcile` downstream too:

```bash
docker compose exec airflow-scheduler airflow tasks clear session_04_orders_medallion_etl -t "build_fact_sales|reconcile" -s 2024-06-01 -e 2024-06-01 -y
```

`build_fact_sales` overwrites the date from silver, `reconcile` passes, the run is
green again. The payoff: recovering from bad state here is just "rerun it".

> **SLA alerts (`sla_miss_callback`) are a separate mechanism, not part of this
> demo.** Airflow **ONLY** evaluates SLA misses on *scheduled* runs, never on manual
> triggers — there's no way to force one on demand. Walk through the `sla=` config
> and `notify_on_sla_miss` in the code instead of trying to trigger it live.

---

## Cleanup (optional)

Demo 1 always leaves `load_day.sql` matching git (the last step of the walkthrough
is `git checkout --`), so there's nothing to restore if you followed the steps in
order. If you want the scratch `demo` schema gone entirely between classes:

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb')
c.execute('DROP SCHEMA IF EXISTS demo CASCADE')
print('demo schema dropped')
"
```

For demo 2, remove its rows so a later run starts clean. The lab data (`2024-03-01`)
and the warehouse schema are left intact:

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
c = duckdb.connect('/opt/airflow/db/dwh.duckdb')
for t in ['bronze.orders_raw','silver.orders','gold.fact_sales']:
    c.execute(f\"DELETE FROM {t} WHERE order_date='2024-06-01'\")
print('demo rows for 2024-06-01 removed')
"
```
