# Session 4 — SQL in DataOps: the Data Warehouse Stack

**Master Class DataOps for Modern Data Platforms · Session 4/11**

Session 3 made a DAG *operable*. This session is about the SQL underneath it: how
to write warehouse transformations that are **safe to rerun, safe to backfill, and
impossible to leave half-written** — then reconcile them so bad data can't slip
through. We build one small **daily orders warehouse** and use it to practise every
idea, rather than scattering unrelated demos.

The warehouse is **DuckDB** — an in-process analytics database (think "SQLite for
analytics"): no server to run, the whole warehouse is one file. It runs on the
**same Airflow stack from session 2**; session 4 adds only DAG code plus the DuckDB
package, no new infrastructure.

## What you will learn

- **Medallion architecture** — data flows Bronze (raw) → Silver (validated) → Gold
  (dimensions + facts), each layer one clear responsibility, so a wrong number is
  traceable to one step.
- **Idempotency** — a load replaces its slice (`DELETE`-by-date + `INSERT`, or
  `MERGE`) instead of appending, so a rerun equals a single run. Three patterns
  compared side by side.
- **Atomicity** — wrapping a load in `BEGIN … COMMIT` so a mid-way failure rolls
  back instead of leaving the table in a state nobody planned for.
- **Determinism** — rows derive from the run's date (`ds`), never `now()` /
  `random()`, so a backfill reproduces exactly what a date should hold.
- **Upserts** — `MERGE INTO` for dimensions (SCD Type 1 in the demo, SCD Type 2 in
  the lab), the workhorse of warehouse loading.
- **Reconciliation** — checking a target against its source on row count, an
  aggregate (`SUM`), and business rules, and failing the pipeline when they differ.
- **Fault tolerance** — `retries` and an `on_failure_callback` to Slack, reused
  from the shared `dataops_common` package.

## The warehouse

One DuckDB file (`db/dwh.duckdb`), organised in schemas by Medallion layer:

```
bronze.orders_raw     # raw orders as received, one day's partition at a time
silver.orders         # validated: rows with a bad customer/amount dropped
gold.dim_customer     # customer dimension, MERGE-upserted (SCD Type 1)
gold.fact_sales       # one row per order, loaded idempotently + atomically
```

The reference pipeline fills all four; the labs read `silver.orders` and build new
Gold marts beside it.

## Access

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | `http://localhost:8080` | `airflow` / `airflow` |
| DuckDB warehouse | `session_04_sql_dwh/db/dwh.duckdb` (a file — open in DBeaver, see below) | — |

> **DuckDB is single-writer.** Only one connection may write the file at a time, so
> the pipelines are linear and you run one writing DAG at a time. That limit is
> exactly what a lakehouse table format (session 5) is built to remove.

## Querying the warehouse from DBeaver (or any DuckDB client) on your host

DuckDB is embedded, not a server — there is no port to connect to. The warehouse
is a plain file, bind-mounted from the container into this repo, so you query it
**directly from your host machine**, no `docker exec` needed:

```
session_04_sql_dwh/db/dwh.duckdb
```

In DBeaver: **New Database Connection → DuckDB**, set **Path** to the absolute path
of that file, and add `duckdb.read_only=true` under driver properties (or append
`?access_mode=read_only` to the connection URL). Use the DBeaver connection any
time you want to poke around — you don't need this repo's `docker compose exec ...
python -c "..."` one-liners at all; those exist only because they're copy-paste
runnable from a terminal without any client installed.

> **Mind the single-writer lock.** Don't leave a DBeaver connection open while
> triggering a DAG (or vice versa) — DuckDB allows many readers *or* one writer,
> not both at once. Disconnect before triggering, reconnect once the run finishes.
> Match the DBeaver DuckDB driver version to `1.5.4` (this file's version) if you
> hit an "open file" error.

## Contents

| Path | What it is |
|---|---|
| `demo/` | Mentor-run material: two DAGs — `session_04_demo_write_patterns` (idempotency + atomicity, one task) and the fully-built reference pipeline `session_04_orders_medallion_etl`. See [demo/README.md](demo/README.md). |
| `lab/` | Two exercises you complete — an idempotent daily rollup fact, and an SCD Type 2 dimension. See [lab/README.md](lab/README.md). |
| `homework/` | Take-home, submitted by Pull Request: apply a CDC change stream with a single `MERGE`, and keep SCD Type 2 history with a point-in-time query. See [homework/README.md](homework/README.md). |

The DAGs are **operators + external SQL**: no raw Python database code. Each step is
a `SQLExecuteQueryOperator` running a `.sql` file (templated with `{{ ds }}`), and
reconciliation is a `SQLCheckOperator`. They reach DuckDB through the baked-in
`duckdb_default` connection (the `airflow-provider-duckdb` hook) and all run on a
1-slot `duckdb_pool` so the single-writer file is never written concurrently. The
shared `notifications` helper still lives in the repo-level `plugins/dataops_common/`.

## Directory structure

```
session_04_sql_dwh/
├── README.md                      # this file — session overview
├── demo/
│   ├── README.md                              # mentor run-book (step by step)
│   └── dags/
│       ├── session_04_demo_write_patterns.py  # 1 task: idempotency + atomicity
│       ├── session_04_orders_medallion_etl.py # the reference pipeline
│       └── sql/                               # one subfolder per DAG above,
│                                               # each .sql templated with {{ ds }}
├── lab/
│   ├── README.md                  # setup + the two exercises
│   └── dags/
│       ├── session_04_fact_customer_daily_starter.py  # Lab 1 (fill in its .sql)
│       ├── session_04_scd2_dim_customer_starter.py    # Lab 2 (fill in its .sql)
│       └── sql/                             # one subfolder per DAG above,
│                                             # given + TODO .sql files
├── homework/
│   ├── README.md                  # CDC-apply via MERGE + SCD2 point-in-time
│   └── submissions/               # you add your DAGs + sql here (PR is review-only)
└── db/                            # the DuckDB warehouse file lives here (mounted)

plugins/                           # repo-level, shared across sessions
└── dataops_common/                # notifications.py, ...
```

## Where to start

Follow [lab/README.md](lab/README.md): it reuses the session 2 stack (one image
build adds DuckDB + the provider; the connection and pool are baked in), and walks
through running the reference pipeline and then the two exercises.

## Homework

[homework/README.md](homework/README.md) — apply a noisy CDC change stream (inserts,
updates, deletes/tombstones) into a current-state table with a single `MERGE`, and keep
SCD Type 2 history you can query at any past date (point-in-time). Submitted by opening a
Pull Request.

## Where these patterns come from

The exercises are built on standard, tool-neutral warehouse practice, not any one
framework:

- **Dimensional modeling & SCD Types** — Kimball Group,
  [Type 2 Slowly Changing Dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
  (effective/expiration dates, current-row flag, a surrogate key per version).
- **`MERGE INTO` and SCD2 in SQL** — DuckDB docs:
  [MERGE INTO](https://duckdb.org/docs/lts/sql/statements/merge_into) and the
  [MERGE for SCD Type 2](https://duckdb.org/docs/current/guides/sql_features/merge) guide.
- **Idempotent loads & CDC (upsert, tombstones, dedup, reconciliation)** —
  [SQL MERGE for Data Engineers: Upserts, CDC, and Idempotent Pipelines](https://dataengineeracademy.com/blog/sql-merge-for-data-engineers-upserts-cdc-and-idempotent-pipelines/).

## Best-practice checklist

Every practice below is demonstrated by real code in this session — not advice in the
abstract. Use it as a review checklist for your own warehouse pipelines.

| Area | Best practice | How this session applies it | Why it matters |
|---|---|---|---|
| **Modeling** | Medallion layering | `bronze` → `silver` → `gold` schemas, one responsibility each | A wrong number is traceable to one layer, not a tangle |
| | Dimensional modeling | `MERGE` dimension (SCD 1 in the demo, SCD 2 in the lab) | Facts stay narrow; slowly-changing attributes live in dims |
| **Correctness** | Idempotent loads | `DELETE`-by-date + `INSERT`, or `MERGE` — never blind append | A rerun or backfill equals a single run; no doubling |
| | Atomicity | `BEGIN … COMMIT` around every `DELETE`+`INSERT` | A mid-load failure rolls back, never leaves a half-written table |
| | Determinism | Rows derive from `{{ ds }}`, never `now()` / `random()` | A backfill reproduces exactly what a date should hold |
| | Reconciliation | `SQLCheckOperator` on `reconcile.sql`: count, sum, business rules | "The task succeeded" is not proof the data is correct |
| | Day-over-day guard | `reconcile` flags a > 50% row swing vs the prior day | Catches a half-loaded / doubled day that same-day checks pass |
| | `NOT EXISTS`, not `NOT IN` | The referential-integrity check in `reconcile.sql` | A single `NULL` dim key makes `NOT IN` silently pass every orphan |
| **Reliability** | Fault tolerance + alerting | `retries`, `retry_delay`, `on_failure_callback` → Slack | Transient failures self-heal; real failures page someone |
| | Single-writer safety | All DAGs share the 1-slot `duckdb_pool` | Serializes writes to the single-file warehouse — no lock fights |
| **Engineering** | Operators + external `.sql` | `SQLExecuteQueryOperator` / `SQLCheckOperator` + `.sql` files | Reviewable, testable SQL; no raw `get_conn()` Python glue |
| | DRY config | `plugins/dataops_common/duckdb_dag.py` (`default_args`, load opts) | One home for the conn/pool/retry policy — no copy-paste drift |
| | Lint + format in CI | `ruff` + `sqlfluff` (DuckDB dialect) via pre-commit | Consistent style, and it catches broken SQL before commit |
| | Host warehouse access | `db/dwh.duckdb` is a plain file — DBeaver read-only, no `docker exec` | Inspect results without babysitting the container |

## Position in the course

| | |
|---|---|
| Previous | Session 3 — Writing operable DAGs: idempotency, backfill, retries, sensors |
| This session | SQL in the warehouse: Medallion layers, idempotent/atomic loads, MERGE, reconciliation on DuckDB |
| Next | Session 5 — Lakehouse: Spark + Delta, and the concurrency the single-file warehouse can't give |
