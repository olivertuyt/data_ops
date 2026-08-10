# Session 4 Lab — Setup & Exercises

**Master Class DataOps for Modern Data Platforms · Session 4/11**

Session 4 reuses the Airflow stack from session 2 — there is no separate stack to
start here. This guide adds the DuckDB package and the session 4 mounts, runs the
reference pipeline, then walks through the two exercises.

> Every `python -c "import duckdb..."` snippet in this guide is copy-paste
> terminal inspection — nothing requires `docker exec`. The warehouse is a plain
> file on your **host** (`db/dwh.duckdb`); open it directly in DBeaver or any
> DuckDB client instead if you prefer. See
> [../README.md](../README.md#querying-the-warehouse-from-dbeaver-or-any-duckdb-client-on-your-host).

## 1. Prerequisites

- The session 2 stack works (`docker compose ps` all healthy). See
  `session_02_airflow_intro/lab/README.md`.
- Port `8080` (Airflow) reachable.

## 2. Enable session 4 on the stack

The session 2 `docker-compose.yaml` already carries the session 4 changes: it builds
a custom image (base Airflow + DuckDB, via the `Dockerfile` next to the compose),
sets `DATAOPS_DWH_PATH`, and mounts:

| Host path | Container path | Purpose |
|---|---|---|
| `plugins/` (repo root) | `/opt/airflow/plugins` | shared `dataops_common` (notifications) |
| `session_04_sql_dwh/demo/dags` | `/opt/airflow/dags/session_04` | both demo DAGs (`session_04_demo_write_patterns`, `session_04_orders_medallion_etl`) + their `sql/` |
| `session_04_sql_dwh/lab/dags` | `/opt/airflow/dags/session_04_lab` | the two lab DAGs + their `sql/` |
| `session_04_sql_dwh/db` | `/opt/airflow/db` | the DuckDB warehouse file |

It also builds a custom image (base Airflow + `duckdb` + `airflow-provider-duckdb`),
bakes the `duckdb_default` connection (an env var), and creates the 1-slot
`duckdb_pool` (in `airflow-init`) — so there is **no manual connection or pool
setup**. Build the image once, then bring the stack up:

```bash
cd session_02_airflow_intro/lab
docker compose build      # one-time: bakes DuckDB into the image
docker compose up -d

# confirm DuckDB is available and the DAGs are mounted
docker compose exec airflow-scheduler python -c "import duckdb; print(duckdb.__version__)"
docker compose exec airflow-scheduler ls /opt/airflow/dags/session_04 /opt/airflow/dags/session_04_lab
```

`docker compose build` runs only once (rerun it only if the `Dockerfile` changes);
DuckDB is baked in, so container starts stay normal speed. Editing a DAG under
`session_04_sql_dwh/**/dags/` afterwards is picked up automatically (the scheduler
rescans every ~30s) — no rebuild or restart needed.

## 3. Run the reference pipeline first

The labs read `silver.orders`, which the reference pipeline produces. Run it for a
date before starting the exercises. All `docker compose` commands run from
`session_02_airflow_intro/lab`.

1. In the UI (`http://localhost:8080`, `airflow` / `airflow`), unpause
   `session_04_orders_medallion_etl` and trigger a run for **2024-03-01**, or from the CLI:
   ```bash
   docker compose exec airflow-scheduler airflow dags trigger session_04_orders_medallion_etl -e 2024-03-01
   ```
2. Watch `create_schema → load_bronze → validate_to_silver → build_dim_customer →
   build_fact_sales → reconcile` go green.
3. Inspect the warehouse:
   ```bash
   docker compose exec -T airflow-scheduler python -c "
   import duckdb
   c = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True)
   print('silver rows :', c.execute(\"SELECT COUNT(*) FROM silver.orders WHERE order_date='2024-03-01'\").fetchone()[0])
   print('fact rows   :', c.execute(\"SELECT COUNT(*) FROM gold.fact_sales WHERE order_date='2024-03-01'\").fetchone()[0])
   "
   ```
   Both are **400,000** (of 400,002 raw rows, two dirty ones were dropped at Silver).

   > Inspect the warehouse with plain `duckdb` and the file path, as above. The
   > `dataops_common` helper only imports **inside** an Airflow task, not from a
   > bare `python -c`.

> DuckDB is single-writer, but you don't have to babysit it: every session-4 task
> runs on the 1-slot `duckdb_pool`, so Airflow serializes warehouse access even if
> two DAGs are triggered at once.

## 4. Table schemas — what you read, what you produce

**Lab 1** rolls `silver.orders` up into `gold.fact_customer_daily`.

**Input — `silver.orders`** (produced by the reference pipeline; one row per order):

| Column | Type | Notes |
|---|---|---|
| `order_id` | bigint | primary key |
| `customer_id` | integer | ids 101–5100 |
| `customer_name` | varchar | |
| `amount` | double | order value |
| `order_date` | date | the run date `ds` |

**Lab 1 output — `gold.fact_customer_daily`** (target table given; one row per customer per day):

| Column | Type | How you build it |
|---|---|---|
| `customer_id` | integer | group key |
| `order_date` | date | group key — `DATE '{{ ds }}'` |
| `order_count` | integer | `COUNT(*)` per customer per day |
| `total_amount` | double | `SUM(amount)` per customer per day |

Primary key `(customer_id, order_date)`.

---

**Lab 2** builds an SCD Type 2 dimension from the validated snapshot.

**Input — `silver.customer_snapshots`** (validated from `bronze.customer_snapshots` by the given
`validate_snapshot.sql`: nulls dropped, tier domain enforced, deduped — one row per customer per day):

| Column | Type | Notes |
|---|---|---|
| `customer_id` | integer | business key |
| `tier` | varchar | `gold` / `silver` / `platinum` |
| `snapshot_date` | date | the run date `ds` |

> `bronze.customer_snapshots` has the same three columns but carries the noise (~100 duplicate +
> ~50 null-tier rows/day). You read **Silver**, never Bronze.

**Lab 2 output — `gold.dim_customer_scd2`** (tables given; one row per customer **version**):

| Column | Type | Notes |
|---|---|---|
| `customer_key` | bigint | **surrogate** PK — one per version, filled by `gold.seq_customer_key` (omit it on INSERT) |
| `customer_id` | integer | business key — repeats across a customer's versions |
| `tier` | varchar | the tier for this version |
| `effective_from` | date | version start — `DATE '{{ ds }}'` |
| `effective_to` | date | version end; **NULL while current** |
| `is_current` | boolean | `true` for the live version |

## 5. Lab 1 — an idempotent daily rollup fact

DAG: `lab/dags/session_04_fact_customer_daily_starter.py`. Build a Gold table with
one row per customer per day — `order_count` and `total_amount` — aggregated from
`silver.orders`. **You complete the SQL, not the Python**: the DAG already wires
three operators to three `.sql` files in `lab/dags/sql/fact_customer_daily_starter/`.
The reference pipeline's `build_fact_sales.sql` + `reconcile.sql` are the same
shape — read them first.

1. **DAG, TODO #1** — fill `default_args`: `retries=2`, `retry_delay=timedelta(minutes=1)`, `on_failure_callback=notify_on_failure`.
2. **`sql/fact_customer_daily_starter/build_fact_customer_daily.sql`** — the idempotent + atomic rollup: `BEGIN;` `DELETE` the date; `INSERT … SELECT customer_id, order_date, COUNT(*), SUM(amount) … GROUP BY customer_id, order_date;` `COMMIT;`. Use `DATE '{{ ds }}'` for the run date.
3. **`sql/fact_customer_daily_starter/reconcile_fact_customer_daily.sql`** — return one row of boolean columns (`SQLCheckOperator` fails on any falsy): `SUM(silver.amount) == SUM(fact.total_amount)` and `COUNT(silver rows) == SUM(fact.order_count)`.

> **Exact vs tolerance.** Here both sides derive from the same `silver.orders`
> rows with no lossy step, so an exact `ROUND(…, 2)` equality is the right check.
> Real-world reconciliation usually allows a small tolerance (e.g. abs diff
> `< 0.01%`) because floats, currency conversion, or a lossy source make an exact
> match too brittle — a check that fails on a one-cent rounding drift just trains
> people to ignore it.

`sql/fact_customer_daily_starter/create_fact_customer_daily.sql` (the target table) is given.

Test it (date `2024-03-01`, after the reference pipeline ran that date):

```bash
docker compose exec airflow-scheduler airflow dags trigger session_04_fact_customer_daily_starter -e 2024-03-01
```

Acceptance criteria:
- Before you start, `reconcile` fails (its placeholder returns `false`) — that's the "not done yet" signal.
- Once finished, the DAG runs green; `gold.fact_customer_daily` has 5,000 rows for the date (one per customer, ids 101–5100), and `SUM(total_amount)` equals `SUM(amount)` in `silver.orders`.
- Triggering the same date twice leaves exactly 5,000 rows for that date (idempotent).
- If you drop the `DELETE` (plain `INSERT`), a second run **errors at `build_fact`** — DuckDB
  enforces the `(customer_id, order_date)` primary key, so re-inserting the day's rows is a
  duplicate-key violation (the DB itself guards the grain). Put the `DELETE` back so the run is
  idempotent.

## 6. Lab 2 — a Slowly Changing Dimension (Type 2)

DAG: `lab/dags/session_04_scd2_dim_customer_starter.py`. The reference `dim_customer`
is SCD Type 1 — it overwrites, so history is lost. Here you keep every version with a
validity window (`effective_from`, `effective_to`, `is_current`) plus a **surrogate
key** `customer_key` (one per version — Kimball), so you can ask "what tier was this
customer on that day?". This DAG seeds its own source; you write one `.sql` file.

The source flows through the **Medallion layers**, same as the demo: the raw daily
snapshot lands in `bronze.customer_snapshots`, a Silver step validates it into
`silver.customer_snapshots`, and the SCD2 dimension is built from **Silver** — never
straight from Bronze. The DAG runs `create_schema → load_snapshot → validate_snapshot
→ apply_scd2`.

`create_scd2.sql` (tables + sequence), `load_snapshot.sql` (the deterministic daily
source — 5,000 customers, and a noisy feed: ~100 duplicate rows + ~50 null-tier rows),
and `validate_snapshot.sql` (Bronze → Silver: drop null/unknown tiers, dedup to one row
per customer) are given.

1. **DAG, TODO #1** — fill `default_args` (same as Lab 1).
2. **`sql/scd2_dim_customer_starter/apply_scd2.sql`** — read from `silver.customer_snapshots`, inside one transaction (`BEGIN … COMMIT`):
   - **Close** current rows whose `tier` differs from today's snapshot: set `effective_to = DATE '{{ ds }}'`, `is_current = false`.
   - **Open** a new current row (`effective_from = DATE '{{ ds }}'`, `effective_to = NULL`, `is_current = true`) for every snapshot customer with no current row of that tier — new customers and the ones you just closed. Omit `customer_key` so the sequence fills it.

Reference: DuckDB's [MERGE for SCD Type 2](https://duckdb.org/docs/current/guides/sql_features/merge) guide.

Test the history is built correctly by running three days **strictly in order** — SCD2 is
sequential, so 2024-01-01 must finish before 2024-01-02 starts. `--wait-for-completion` blocks
until each run is done, so the next date can't overtake it (a fixed `sleep` can't guarantee that):

```bash
for d in 2024-01-01 2024-01-02 2024-01-03; do
  docker compose exec airflow-scheduler \
    airflow dags trigger session_04_scd2_dim_customer_starter -e $d --wait-for-completion
done
```

Then verify — don't dump all 5,500 rows; check the totals and spot-check two customers:

```bash
docker compose exec -T airflow-scheduler python -c "
import duckdb
q = duckdb.connect('/opt/airflow/db/dwh.duckdb', read_only=True).execute
print('total versions  :', q('SELECT COUNT(*) FROM gold.dim_customer_scd2').fetchone()[0], '(expect 5500)')
print('current rows    :', q('SELECT COUNT(*) FROM gold.dim_customer_scd2 WHERE is_current').fetchone()[0], '(expect 5000)')
print('>1 current/cust :', q('SELECT COUNT(*) FROM (SELECT customer_id FROM gold.dim_customer_scd2 WHERE is_current GROUP BY 1 HAVING COUNT(*)<>1)').fetchone()[0], '(expect 0)')
print('cust 201 (slice):', q(\"SELECT tier,effective_from,effective_to,is_current FROM gold.dim_customer_scd2 WHERE customer_id=201 ORDER BY effective_from\").fetchall())
print('cust 800 (outside):', q(\"SELECT tier,effective_from,effective_to,is_current FROM gold.dim_customer_scd2 WHERE customer_id=800 ORDER BY effective_from\").fetchall())
"
```

Or query the file directly from a **DuckDB client on your host** (DBeaver, or the `duckdb` CLI) — no `docker exec`. Open `session_04_sql_dwh/db/dwh.duckdb` read-only and run:

```sql
-- totals + the "exactly one current per customer" invariant
SELECT
    COUNT(*)                                          AS total_versions,   -- 5500
    COUNT(*) FILTER (WHERE is_current)                AS current_rows,     -- 5000
    (SELECT COUNT(*) FROM (
        SELECT customer_id FROM gold.dim_customer_scd2
        WHERE is_current GROUP BY customer_id HAVING COUNT(*) <> 1))
                                                      AS customers_bad     -- 0
FROM gold.dim_customer_scd2;

-- spot-check a changed customer (2 versions) vs an unchanged one (1 version)
SELECT customer_key, customer_id, tier, effective_from, effective_to, is_current
FROM gold.dim_customer_scd2
WHERE customer_id IN (201, 800)
ORDER BY customer_id, effective_from;
```

> Match the DBeaver DuckDB driver to this file's version (`1.5.4`), and disconnect
> before triggering the DAG — DuckDB allows many readers *or* one writer, not both.

Acceptance criteria:
- **Silver drops the noise:** `bronze.customer_snapshots` = 5,150 rows/day →
  `silver.customer_snapshots` = **5,000** (dedup + null-tier removed).
- Customer **201** (in the changing slice 201–700) has **two** versions with
  **different `customer_key`s**: `gold` valid `[2024-01-01, 2024-01-03)` with
  `is_current=false`, and `platinum` valid from `2024-01-03` with `is_current=true`.
- A customer **outside** the slice (e.g. 800) has **one** current version, unchanged.
- After the three days: `dim_customer_scd2` = **5,500** rows total, **5,000** current
  (the 500-customer slice gained a v2 on 2024-01-03).
- Re-running any of those dates does **not** add a duplicate version (idempotent) —
  the check is that a same-tier snapshot opens no new row.

## 7. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| `ModuleNotFoundError: duckdb` | Image not built with DuckDB | `docker compose build` then `docker compose up -d` in `session_02_airflow_intro/lab` |
| `ModuleNotFoundError: dataops_common` | Plugins mount not applied | Confirm `../../plugins` is mounted; restart the stack |
| `session_04*` DAGs missing in UI | Mounts not applied, or an import error | Restart the stack; check the DAG's import-error banner in the UI |
| `IO Error: Could not set lock on file` | A task ran off the pool (concurrent writer) | Confirm every session-4 task has `pool="duckdb_pool"` and the pool exists (`airflow pools list`) |
| Task stuck `queued`, never runs | `duckdb_pool` missing (airflow-init didn't create it) | `docker compose up -d` to re-run init, or `airflow pools set duckdb_pool 1 x` |
| Lab DAG fails at `silver.orders` does not exist | Reference pipeline hasn't run for the date | Trigger `session_04_orders_medallion_etl` for the date first (section 3) |
