# Session 4 Homework — CDC with MERGE, and Slowly-Changing History

Two take-home tasks on the same DuckDB warehouse from the lab. Both are the warehouse
patterns interviewers reach for and the session's headline skill — **`MERGE INTO`** — applied
to the two jobs it exists for:

- **Homework 1** — apply a **change-data-capture (CDC) stream** into a current-state table with
  a single `MERGE` (inserts, updates, and deletes/tombstones), the way real ingestion lands.
- **Homework 2** — keep **slowly-changing history** with SCD Type 2, then *use* it: answer
  "what did this customer look like on date X" with a **point-in-time query**.

The lab only ever `overwrote` a slice (`DELETE`-by-date + `INSERT`). These push into the writes
that *accumulate*: a `MERGE` that reconciles a batch of changes against what is already there.

You run them on the same Airflow stack from session 2. Put your DAGs (and their `.sql` files) in
`session_04_sql_dwh/homework/submissions/` — that folder is mounted into Airflow at
`/opt/airflow/dags/session_04_homework`, so the scheduler picks them up within ~30s.

> **Inspecting results.** The warehouse is a plain file on your **host**
> (`session_04_sql_dwh/db/dwh.duckdb`) — open it in **DBeaver** or any DuckDB client (read-only)
> and query directly; see
> [../README.md](../README.md#querying-the-warehouse-from-dbeaver-or-any-duckdb-client-on-your-host).
> Disconnect before triggering a DAG — DuckDB is single-writer.

Build your DAGs the same way as the lab: **operators + external `.sql` files**, not raw Python
connections. Reach the warehouse through the baked-in `duckdb_default` connection, keep every DAG
on the `duckdb_pool` pool (DuckDB is single-writer), and put the SQL in a sibling `sql/` folder
pointed to by `template_searchpath`:

```python
from airflow.providers.common.sql.operators.sql import (
    SQLExecuteQueryOperator, SQLCheckOperator,
)

default_args = {"conn_id": "duckdb_default", "pool": "duckdb_pool", ...}
# with DAG(..., template_searchpath="/opt/airflow/dags/session_04_homework/sql"):
#   SQLExecuteQueryOperator(task_id="...", sql="my_step.sql",
#                           split_statements=True, autocommit=True)
```

Wrap any multi-statement write in `BEGIN … COMMIT` inside the `.sql` file for atomicity, and
template the run date with `{{ ds }}`.

---

## Homework 1 — Apply a change stream with `MERGE` (required)

A source system emits one row per **change** — an insert, an update, or a delete — each tagged
with an operation flag `op ∈ {I, U, D}`. Those events arrive **duplicated** (retries) and
**out of order** (late updates). Your job is to keep `silver.events` as the **current state**:
one row per `event_id` (its newest version), and **no row for a key whose latest event is a
delete** (a *tombstone*). Real ingestion does this incrementally with one `MERGE` per batch —
not by rebuilding the table — so that is what you will write.

### The source (given — seed it deterministically in your DAG)

`load_changes.sql` seeds one day's change batch. Generated from `{{ ds }}` (and `range()`), so
the batch — its duplicates, late update, and deletes — is reproducible and at real scale
(~135k change rows/day):

```sql
BEGIN;
DELETE FROM bronze.events_raw WHERE event_date = DATE '{{ ds }}';

INSERT INTO bronze.events_raw
-- 100k inserts
SELECT i, 1000 + (i % 5000), ROUND(10 + (i % 500) * 1.5, 2),
       'I', TIMESTAMP '{{ ds }} 09:00:00', DATE '{{ ds }}'
FROM range(1, 100001) AS t(i)
UNION ALL  -- 20k late UPDATEs (amount +100, a later timestamp) to keys 1..20000
SELECT i, 1000 + (i % 5000), ROUND(10 + (i % 500) * 1.5 + 100, 2),
       'U', TIMESTAMP '{{ ds }} 15:00:00', DATE '{{ ds }}'
FROM range(1, 20001) AS t(i)
UNION ALL  -- 10k DELETEs (tombstones) of keys 90001..100000
SELECT i, 1000 + (i % 5000), NULL,
       'D', TIMESTAMP '{{ ds }} 18:00:00', DATE '{{ ds }}'
FROM range(90001, 100001) AS t(i)
UNION ALL  -- 5k duplicate insert retries of keys 1..5000 (an at-least-once source)
SELECT i, 1000 + (i % 5000), ROUND(10 + (i % 500) * 1.5, 2),
       'I', TIMESTAMP '{{ ds }} 09:00:00', DATE '{{ ds }}'
FROM range(1, 5001) AS t(i);
COMMIT;
```

Schema: `bronze.events_raw(event_id BIGINT, user_id INTEGER, amount DOUBLE, op VARCHAR, updated_at TIMESTAMP, event_date DATE)`
and the target `silver.events(event_id BIGINT PRIMARY KEY, user_id INTEGER, amount DOUBLE, updated_at TIMESTAMP)`.

### Your task

Write a DAG `session_04_events_cdc` with steps `load_changes → apply_cdc → reconcile` that, per
run date, seeds the batch and **applies it with a single `MERGE`**.

The catch that makes CDC different from the demo's load: **a `MERGE` can match at most one source
row per target key**, but the batch has several rows for the same `event_id` (the retry, the late
update). So you must **collapse the batch to the latest event per key first**, then merge that:

```sql
-- apply_cdc.sql
BEGIN;

-- 1) reduce the batch to ONE row per key (the newest event) BEFORE merging.

-- 2) apply inserts, updates, and tombstones in one statement.
COMMIT;
```

| Principle | What your pipeline must do |
|---|---|
| **Determinism** | Everything derives from `{{ ds }}`; never `now()` / `random()`. |
| **Batch dedup first** | Collapse the batch to the latest event per key (`QUALIFY row_number() … ORDER BY updated_at DESC`) **before** the `MERGE` — you cannot merge two source rows onto one target row. |
| **Tombstones** | `WHEN MATCHED AND op = 'D' THEN DELETE`; a delete for a key not present is a no-op (`WHEN NOT MATCHED AND op <> 'D'` skips it). |
| **Idempotency** | Re-applying the same date's batch leaves `silver.events` unchanged. |
| **Atomicity** | The dedup + `MERGE` run inside one `BEGIN … COMMIT` in the `.sql`. |
| **Reconciliation** | A `SQLCheckOperator` on `reconcile.sql` returns TRUE only when `silver.events` has **no duplicate `event_id`** and **no key whose latest event across the batch was a delete**. |
| **Fault tolerance** | `retries`, `retry_delay`, `on_failure_callback=notify_on_failure` in `default_args`. |

### Test it yourself

1. **Runs clean:** trigger `2024-03-01`. `silver.events` has **90,000 rows** (100k inserted −
   10k tombstoned). `event_id = 1`'s amount is the **updated** value `111.5` (base `11.5` + 100),
   `event_id = 90001` is **absent** (tombstoned), and there are **no duplicate `event_id`s**.
2. **Idempotent:** trigger `2024-03-01` again — still **90,000** rows, unchanged.
3. **Accumulates (why `MERGE`, not rebuild):** point `load_changes.sql` at a second day whose
   batch inserts new keys, updates some existing ones, and tombstones others; trigger
   `2024-03-02`. `silver.events` reflects the **merged** state across both days — old keys still
   there, deleted keys gone, new keys added — which a per-day rebuild could not give you.
4. **The dedup matters:** temporarily `MERGE` straight from `bronze.events_raw` (skip `_latest`).
   The `MERGE` fails with a **`PRIMARY KEY … duplicate key` constraint error** — the un-deduped
   batch carries the same `event_id` several times (a retry + a late update), so the
   `WHEN NOT MATCHED … INSERT` branch tries to insert that key more than once. Put the dedup back.
   (SQL-standard engines like Postgres/Snowflake instead reject this earlier with *"multiple
   source rows matched a target row"*; DuckDB doesn't enforce that cardinality check, so here the
   duplicate surfaces on the insert — either way the lesson is the same: collapse to one row per
   key *before* the `MERGE`.)

---

## Homework 2 — SCD Type 2 history + a point-in-time query (required)

The lab *built* a Slowly-Changing-Dimension Type 2 table. Its whole reason to exist is the
**temporal query** it unlocks: reconstructing what a dimension looked like at any past date.
Here you keep two days of customer history, then answer "as of date X".

### The dimension

`gold.dim_customer_scd2(customer_id INT, name VARCHAR, tier VARCHAR, effective_date DATE, expiration_date DATE, is_current BOOLEAN, version INT)`.

> **Windowing convention — deliberately different from the lab.** The lab left the current
> version open with `effective_to = NULL` and used a **half-open** window (`effective_to` = the
> *next* version's start date). Here the open version instead carries a **high-date sentinel**
> `expiration_date = DATE '9999-12-31'`, and an expired version's `expiration_date = ds - 1 day` —
> a **closed, inclusive** window (`[effective_date, expiration_date]`), which is why the
> point-in-time query can use `BETWEEN`. Both the NULL/half-open and sentinel/closed conventions
> are standard in the wild; using each once is intentional so you've seen both.

Seed a daily **snapshot** of the source (given — deterministic, 1,000 customers):

```sql
-- load_snapshot.sql : today's full snapshot of the source dimension
DELETE FROM stage.customer_snapshot WHERE snapshot_date = DATE '{{ ds }}';
INSERT INTO stage.customer_snapshot
SELECT i, 'Customer ' || i,
       CASE WHEN i <= {{ params.silver_upto }} THEN 'SILVER' ELSE 'BRONZE' END,
       DATE '{{ ds }}'
FROM range(1, 1001) AS t(i);
```

Run it for two dates so a slice of customers **change tier** between them:
`2024-03-01` with `silver_upto = 0` (everyone BRONZE), then `2024-03-02` with `silver_upto = 200`
(customers 1–200 upgrade to SILVER).

### Your task

Write a DAG `session_04_dim_customer_scd2` with `load_snapshot → apply_scd2 → point_in_time → reconcile`.

1. **`apply_scd2.sql`** (the lab pattern — reuse it): for each run date, compare the snapshot to
   the current rows; for a customer whose attributes changed, **expire** the current version
   (`expiration_date = ds - 1 day`, `is_current = false`) and **insert** a new version
   (`effective_date = ds`, `expiration_date = DATE '9999-12-31'`, `is_current = true`,
   `version + 1`); insert brand-new customers as version 1. Unchanged customers are untouched.

2. **`point_in_time.sql`** (the new skill): a query returning each customer's attributes **as of**
   `{{ ds }}` — the version whose validity window covers that date:

   ```sql
   SELECT customer_id, name, tier, version
   FROM gold.dim_customer_scd2
   WHERE DATE '{{ ds }}' BETWEEN effective_date AND expiration_date
   ORDER BY customer_id;
   ```

3. **`reconcile.sql`** (`SQLCheckOperator`): TRUE only when **exactly one** row per `customer_id`
   is `is_current`, and **no** customer has overlapping validity windows (history is a clean,
   gap-free timeline).

### Test it yourself

1. Run `2024-03-01` then `2024-03-02`. `gold.dim_customer_scd2` holds **1,200 rows**
   (1,000 v1 + 200 v2), of which **1,000** are `is_current`.
2. **Point-in-time as of `2024-03-01`:** 1,000 rows, **all BRONZE** — the SILVER upgrade hadn't
   happened yet. **As of `2024-03-02`:** 1,000 rows, of which **200 are SILVER**. Same table, two
   different truths, selected by date — that is what SCD 2 buys you.
3. `reconcile` stays green (one current row per customer, no overlaps). Break it on purpose:
   `apply_scd2` **without** expiring the old version (insert the new one only), rerun, and watch
   the "exactly one current" check fail. Put the expiry back.

---

## Short answer (include in your PR description)

DuckDB gave you `MERGE`, transactions, and a single-file warehouse. In session 5 we move to a
lakehouse (Spark + Delta). In 2–3 sentences: **what does the single-file warehouse stop you from
doing** that a lakehouse table format is built to handle? (Hint: the single-writer limit — many
jobs and engines reading and writing the same tables at once.)

---

## Submit — open a Pull Request

1. Create a branch: `git checkout -b homework/session-04/<your-name>`.
2. Add your DAGs + `.sql` under `session_04_sql_dwh/homework/submissions/`.
3. Commit and push, then **open a Pull Request** against `main`.
4. In the PR describe: what you did; the `silver.events` count after a rerun and after Day 2
   (HW1); the point-in-time result as of each date (HW2); which check you broke to prove each
   reconcile works; and your short answer above.

The PR is for review only — it will not be merged, so don't worry about conflicts with other
submissions.

## Acceptance criteria

- [ ] HW1: `silver.events` kept as current state with a **single `MERGE`** (insert/update/delete);
  the batch is deduped to one row per key **before** the merge; tombstones remove keys.
- [ ] HW1: the load is idempotent and atomic (`BEGIN … COMMIT`); `reconcile` fails on a duplicate
  `event_id` or a surviving tombstone.
- [ ] HW2: `apply_scd2` expires + inserts versions; the **point-in-time** query returns the
  version valid on `{{ ds }}`; `reconcile` fails if a customer has ≠ 1 current row or overlapping
  windows.
- [ ] Both use operators + external `.sql` (no raw Python connections), reach the warehouse via
  `conn_id="duckdb_default"`, run on `duckdb_pool`, and wire `on_failure_callback`.
