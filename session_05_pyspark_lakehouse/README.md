# Session 5 — The Lakehouse: PySpark + Delta Lake

**Master Class DataOps for Modern Data Platforms · Session 5/11**

Session 4 built a warehouse in a single DuckDB file: one writer, no history. This session
moves to a **lakehouse** — Delta Lake tables on object storage — and uses a realistic
workload to hit the problems that actually make Spark pipelines hard.

The workload is **e-commerce ad analytics**. Ad servers drop one CSV row per ad event
(impression or click) into MinIO every day — **~40,000–54,000 rows/day** (volume varies by day,
lower on weekends). We build the number the
ads team looks at each morning: **daily views (impressions) per ad**, plus clicks, reach,
and CTR. Along the way the data does what real data does — it **arrives duplicated**, and
its **schema changes from one day to the next**.

## What you will learn

- **Deduplication** — ad servers retry, so the same `event_id` lands twice; counting both
  inflates the view numbers. Dedup before aggregating.
- **Schema evolution** — the source team adds a column on Day 2 and changes a type on Day 3.
  How Delta handles additive changes (`mergeSchema`), what breaks, and how to stay resilient.
- **Delta table design** — partitioning by date, and the table properties that keep a
  production table healthy (optimized writes, auto-compaction, data-skipping, retention).
- **Table maintenance** — `OPTIMIZE`, `ZORDER`, and `VACUUM`, and when each matters.
- **Idempotent partition overwrite** (`replaceWhere`) and **backfill** across multiple days.
- **Reproducibility / time travel** — Delta keeps every version; `VERSION AS OF` reconstructs
  an earlier state.
- **Reconciliation** in the engine — checks run in Trino, never scanning raw into the client:
  dedup holds, silver ties to gold, business rules pass, and a day-over-day volume guard.
- **Orchestrating Spark from Airflow** — `SparkSubmitOperator` to a standalone Spark cluster.

> **Data skew** (a few hot ads dominating a shuffle) is a real lakehouse problem too, but it
> gets its own treatment in **Session 6**. Here the data is only mildly skewed and we don't
> tune for it.

## Data model

The lakehouse is organised in **medallion layers** — a MinIO bucket *and* a metastore schema
per layer:

| Layer | Location | Table | Grain |
|---|---|---|---|
| Bronze | `s3a://bronze/ad_events/{ds}.csv` | — (raw CSV landing) | one row per raw event |
| Silver | `s3a://silver/fact_ad_events/` | `silver.fact_ad_events` | one row per **deduped** event |
| Gold | `s3a://gold/agg_ad_daily/` | `gold.agg_ad_daily` | one row per **ad per day** |

Both Delta tables are partitioned by `event_date`. Fact columns: `event_id, ad_id, campaign_id,
user_id, event_type, event_ts, event_date` (+ `device_type`, `cost_micros` as they arrive).
Aggregate columns: `event_date, ad_id, campaign_id, impressions, clicks, unique_users, ctr`.

Pipeline: `bronze CSV → build_fact_ad_events (dedup) → aggregate_ad_daily → reconcile`.

## The 3-day dump: a schema that evolves

`scripts/generate_ad_events.py` is deterministic per date and reproduces a real source-team
rollout — generate three consecutive days and the schema changes under you:

| Day | File | Schema change |
|---|---|---|
| 2026-06-26 | `ad_events/2026-06-26.csv` | base: `event_id, ad_id, campaign_id(int), user_id, event_type, event_ts` |
| 2026-06-27 | `ad_events/2026-06-27.csv` | **+ `device_type`** (mobile/desktop/tablet) — additive |
| 2026-06-28 | `ad_events/2026-06-28.csv` | **+ `cost_micros`**, and `campaign_id` now a **string** (`"cmp_07"`) — a breaking type change |

Each day's volume varies (weekend dip + daily noise) — e.g. 53,600 / 41,800 / 40,500 raw rows
across 06-26/27/28 — with ~2% duplicate `event_id`s (52,528 / 40,964 / 39,690 distinct) and ~3% clicks.

## Schema evolution — the patterns and the traps

The core tension in every ingestion pipeline: **schema enforcement (safety) vs schema
evolution (flexibility)**. By default Delta **enforces** — a write whose columns don't match
the table is rejected. That's what stops a malformed feed from silently corrupting a table;
it's also what pages you at 2 a.m. when the source adds a harmless column.

| Change | What happens by default | How to handle it |
|---|---|---|
| **Add a column** (Day 2 `device_type`) | Write fails: `AnalysisException: A schema mismatch detected` | Opt in with `.option("mergeSchema", "true")`. The column is added table-wide; **old partitions read NULL**. Safe. |
| **Change a type** (Day 3 `campaign_id` int→string) | Fails even with `mergeSchema`: `[DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'campaign_id'` | **Normalize at ingestion** — cast ids to `string` so the fact's type is stable no matter what the source sends. (Or an explicit migration; `overwriteSchema=true` rewrites the whole schema and can drop columns — dangerous.) |
| **Rename a column** | Delta sees a **drop + add**: old values are orphaned under the old name, the new column is NULL for old rows | Column-mapping mode, or map the rename explicitly in the ETL. A silent-data-loss classic. |
| **Drop a source column** | Write still succeeds (column stays, new rows NULL) | Downstream reads must tolerate NULL; don't assume a column is always present. |

Our loader applies both guards: it **normalizes id columns to string** (surviving Day 3) and
writes with **`mergeSchema`** (absorbing Day 2). Demo 2 shows each failure and its fix live.

## Delta table design & optimization

The tables aren't bare — `jobs/init_schema.py` creates them **partitioned by `event_date`**
and with production table properties baked in:

```
delta.autoOptimize.optimizeWrite = true   -- write fewer, larger files (not ~200 tiny ones)
delta.autoOptimize.autoCompact   = true   -- compact small files right after a write
delta.dataSkippingNumIndexedCols = 8      -- keep min/max stats so queries skip files
delta.logRetentionDuration          = 'interval 30 days'   -- how far back time travel works
delta.deletedFileRetentionDuration  = 'interval 7 days'    -- how long VACUUM keeps old files
```

- **Partition by `event_date`** — matches how data arrives and how it's queried (per day), and
  it's exactly the slice `replaceWhere` overwrites for idempotency. **Don't** partition by a
  high-cardinality column like `ad_id` — that makes millions of tiny partitions (the
  small-files problem, inverted).
- **`optimizeWrite` / `autoCompact`** — a dedup does a shuffle (default 200 partitions), which
  would otherwise write ~200 small files per day. These properties coalesce each day's load to
  ~1 file per partition — after loading all 3 days, `fact_ad_events` has just **3 files**.
- **`OPTIMIZE … ZORDER BY (ad_id)`** — bin-packs files and co-locates rows by `ad_id`, so a
  `WHERE ad_id = …` query reads fewer files (data skipping). Run it as periodic maintenance.
- **`VACUUM`** — deletes files no longer referenced. Note the trade-off: VACUUM with a short
  retention **breaks older `VERSION AS OF`** — retention is why the two `…RetentionDuration`
  properties exist.
- **Liquid clustering** (`CLUSTER BY`, Delta 3.1+) is the newer alternative to
  partitioning + ZORDER, avoiding skewed partitions — worth knowing for what comes next.

## Contents

| Path | What it is |
|---|---|
| `demo/` | Mentor-run: **Demo 1** — the pipeline at volume + table design/OPTIMIZE; **Demo 2** — schema evolution across the 3-day dump. See [demo/README.md](demo/README.md). |
| `lab/` | Three exercises — build the deduped fact, the daily aggregate, and (when the schema changes) the `mergeSchema` fix + reconcile. See [lab/README.md](lab/README.md). |
| `homework/` | PR-submitted: a campaign rollup + **data-quality gate**, and a **schema-drift guard**. See [homework/README.md](homework/README.md). |

## Stack

| Component | Tool | Version | Port |
|---|---|---|---|
| Object Storage | MinIO | latest | 9000 · 9001 |
| Table Format | Delta Lake | 3.2.0 | — |
| Processing | PySpark | 3.5.x | — |
| Catalog | Hive Metastore | 3.0.0 | 9083 |
| Query Engine | Trino | 445 | 8090 |
| Orchestration | Airflow | 2.9.0 (Celery) | 8888 |
| Spark | Spark standalone | 3.5.0 | 8080 |

> The Airflow image is built from the `Dockerfile` here (Airflow + Java 11 + PySpark 3.5.0 +
> the Spark provider); its Python 3.8 / Java 11 match the Spark image so the client-mode driver
> runs in the Airflow worker without version clashes. Hive Metastore's backend is MariaDB.

## Access

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8888 | `airflow` / `airflow` |
| MinIO console | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Trino | `localhost:8090` (DBeaver: catalog `delta`) | no auth — any user name |
| Spark master UI | http://localhost:8080 | — |

## Bring up the stack

All commands run from `session_05_pyspark_lakehouse/`.

**1. Download the four Spark jars** (≈275 MB, not committed — see `.gitignore`):

```bash
wget -P jars/ \
  https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/3.2.0/delta-spark_2.12-3.2.0.jar \
  https://repo1.maven.org/maven2/io/delta/delta-storage/3.2.0/delta-storage-3.2.0.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
```

**2. Build the Airflow image and start everything:**

```bash
docker compose build      # one-time: bakes Java + PySpark + the Spark provider in
docker compose up -d
docker compose ps         # wait until services are healthy
```

`docker compose build` runs only once (rerun it only if the `Dockerfile` or
`requirements_local.txt` changes). Editing a DAG under `demo/` or `lab/` afterwards is picked
up automatically (~30s rescan); the Spark jobs are re-read on each run.

> The jars and Delta/S3A config live in spark-master's `conf/spark-defaults.conf`, so
> `docker exec spark-master spark-submit|spark-sql` commands need no `--jars` or `--conf`.

> **Local dev (optional):** `python3 -m venv .venv && source .venv/bin/activate && pip install
> -r requirements_local.txt` for IDE support / running a Spark job outside Docker.

## Generate the data and create the tables

```bash
# Generate the 3-day dump (deterministic per date; the schema evolves 26 → 27 → 28)
for d in 2026-06-26 2026-06-27 2026-06-28; do
  python3 scripts/generate_ad_events.py --ds $d --output-dir data/bronze/ad_events
done

# Upload to MinIO bronze/ad_events/
NET=$(docker inspect session_05_pyspark_lakehouse-minio-1 -f '{{range $k,$v := .NetworkSettings.Networks}}{{$k}}{{end}}')
docker run --rm --network "$NET" -v "$PWD/data/bronze/ad_events:/data" --entrypoint sh minio/mc -c "
  mc alias set local http://minio:9000 minioadmin minioadmin &&
  mc cp /data/2026-06-26.csv /data/2026-06-27.csv /data/2026-06-28.csv local/bronze/ad_events/ &&
  mc ls local/bronze/ad_events/"

# Create the silver/gold schemas + tables (partitioned, with the properties above) — once
docker exec spark-master /opt/spark/bin/spark-submit --py-files /jobs/lakehouse_common.py /jobs/init_schema.py
```

Now run the reference pipeline (see [demo/README.md](demo/README.md)) or the lab
(see [lab/README.md](lab/README.md)).

## Querying from Trino (or DBeaver) on your host

Trino is the interactive query engine over the Delta tables. Query it from a one-off CLI, or
connect **DBeaver** on your host to `localhost:8090` (catalog `delta`, schemas `silver` / `gold`).

> **Read-after-write.** The pipeline's `reconcile` reads through Trino immediately after Spark
> overwrites a partition. Trino caches Delta metadata by default and would serve a stale file
> list, so `conf/trino/catalog/delta.properties` sets `delta.metadata.cache-ttl=0s` — a
> lakehouse table shared by two engines needs the reader to see the writer's latest commit.

```bash
# Views per ad, top 5 for a day
docker exec session_05_pyspark_lakehouse-trino-1 trino --server localhost:8080 --execute \
  "SELECT ad_id, impressions, clicks, unique_users, ctr FROM delta.gold.agg_ad_daily
   WHERE event_date = DATE '2026-06-26' ORDER BY impressions DESC LIMIT 5"

# Delta version history
docker exec session_05_pyspark_lakehouse-trino-1 trino --server localhost:8080 --execute \
  "SELECT version, operation FROM delta.silver.\"fact_ad_events\$history\" ORDER BY version"
```

**Time travel** (`VERSION AS OF`) runs through **Spark** — this Trino build's Delta connector
does not expose versioned tables:

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) FROM silver.fact_ad_events VERSION AS OF 0"   # 0 — empty, before the first load
```

## Directory structure

```
session_05_pyspark_lakehouse/
├── README.md · docker-compose.yml · Dockerfile · requirements_local.txt
├── conf/                          # hive-site.xml, trino/, spark-defaults.conf
├── jars/                          # the four Spark jars (downloaded, gitignored)
├── scripts/generate_ad_events.py  # deterministic 3-day ad-event generator
├── jobs/                          # init_schema.py + lakehouse_common.py (shared Spark helpers)
├── demo/
│   ├── README.md                  # mentor run-book (Demo 1 + Demo 2)
│   ├── jobs/                       # build_fact_ad_events.py, aggregate_ad_daily.py
│   └── dags/session_05_ad_daily_metrics.py + sql/reconcile.sql
├── lab/
│   ├── README.md                  # the three exercises
│   ├── jobs/                       # build_fact_ad_events.py, aggregate_ad_daily.py (TODO starters)
│   └── dags/session_05_ad_daily_metrics_starter.py + sql/reconcile.sql
└── homework/
    ├── README.md
    └── submissions/               # you add code here (PR is review-only)

plugins/dataops_common/            # repo-level, shared (notifications.py, storage.py)
```

## Position in the course

| | |
|---|---|
| Previous | Session 4 — SQL warehouse on DuckDB: Medallion, idempotent/atomic loads, MERGE, reconciliation |
| This session | Lakehouse: PySpark + Delta at volume — dedup, schema evolution, table design/OPTIMIZE, idempotency, time travel |
| Next | Session 6 — Spark performance: data skew, partitioning/shuffle tuning, joins |

## Where these patterns come from

- **Delta Lake** — [batch reads/writes, `replaceWhere`, schema evolution, time travel](https://docs.delta.io/latest/delta-batch.html); [OPTIMIZE & Z-ORDER](https://docs.delta.io/latest/optimizations-oss.html).
- **Schema evolution & enforcement** — [Delta: schema enforcement vs evolution](https://docs.delta.io/latest/delta-batch.html#schema-validation).
- **Trino over Delta** — [Trino Delta Lake connector](https://trino.io/docs/current/connector/delta-lake.html).
- **Spark from Airflow** — [`SparkSubmitOperator`](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/operators.html).

## Best-practice checklist

Every practice below is demonstrated by real code in this session — the lakehouse
counterpart to session 4's SQL checklist. Use it as a review checklist for your own
Spark + Delta pipelines.

| Area | Best practice | How this session applies it | Why it matters |
|---|---|---|---|
| **Modeling** | Medallion layering | `bronze` CSV → `silver.fact_ad_events` → `gold.agg_ad_daily`, buckets + metastore schemas | A wrong metric is traceable to one layer |
| | Grain separation | Silver = one row per event; Gold = one row per ad per day | Facts stay raw-grained; aggregates are derived, never the source of truth |
| **Correctness** | Idempotent writes | `mode("overwrite").option("replaceWhere", "event_date = …")` | A rerun replaces the date's partition — no doubling |
| | Deduplication | `row_number()` window on `event_id`, keep earliest | Ad-server retries don't inflate impression counts |
| | Determinism | Everything keyed off `{{ ds }}` / the run date | A backfill reproduces exactly what a date should hold |
| | In-engine reconcile | `reconcile` runs SQL **in Trino**, never pulls raw into the worker | Scales to billions of rows; the check can't OOM the driver |
| | Day-over-day guard | `reconcile` fails on a > 50% row swing vs the prior day | Catches a half-loaded / doubled day that same-day checks pass |
| **Schema** | Select known columns | `df.select(KNOWN_COLUMNS …)` before write | An unexpected source column is dropped, not silently merged in |
| | Upsert with `MERGE` | Homework `dim_ad` uses `MERGE INTO` (vs `overwrite`) | Accumulating dimensions maintained in place, idempotently |
| **Delta design** | Partition + compaction | Partition by `event_date`; `optimizeWrite` + `autoCompact` | Few large files per day, not ~200 tiny shuffle files |
| | Data skipping | `dataSkippingNumIndexedCols`; `OPTIMIZE … ZORDER BY (ad_id)` | `WHERE ad_id = …` skips files instead of scanning all |
| | Retention vs history | `VACUUM` + `deletedFileRetentionDuration` (7d) vs time travel | The dial between reclaiming space and keeping old versions |
| **Reliability** | Fault tolerance + alerting | `retries`, `retry_delay`, `execution_timeout`, `on_failure_callback` → Slack | Transient failures self-heal; real failures page someone |
| | Read-after-write | Trino `delta.metadata.cache-ttl=0s` | The reconcile reader sees the writer's latest commit immediately |
| **Engineering** | Shared Spark module | `jobs/lakehouse_common.py` (`build_spark`, `get_logger`) via `--py-files` | Spark config + logging live in one place, not copied per job |
| | No hardcoded jars/secrets | Jars from `Variable.get("spark_jars")`; creds from `AWS_*` env vars | Config and secrets never baked into job or DAG code |
| | Lint + format in CI | `ruff` + `sqlfluff` (Trino dialect) via pre-commit | Consistent style; catches broken SQL before commit |

### Scenario — how the platform acts on a table's schema drift (Schema Evolution)

Schema is where a data platform earns its keep. A feed's shape *will* change; the point is
that the platform doesn't react ad-hoc — it **classifies the drift and takes a defined action
per table**, and Delta's default schema enforcement guarantees nothing slips through silently.
This session walks that policy across the 3-day dump:

| What changed on the source | Decision | Action the platform takes | This session's example |
|---|---|---|---|
| A **new additive** column appears | **Accept** | Opt in with `mergeSchema=true` — Delta adds the column table-wide; old rows read `NULL`. | Day 2 adds `device_type` |
| A column's **type changed** | **Absorb** | Normalize the column to a stable type at ingestion — e.g. cast id columns to `string`, or store money as `Decimal(38,6)` rather than a float. | Day 3 ships `campaign_id` as `"cmp_07"` (int → string) — no `DELTA_FAILED_TO_MERGE_FIELDS` |
| An **unknown/unexpected** column appears | **Ignore/Accept** | `select(KNOWN_COLUMNS …)` before the write — drop it rather than alter the table / Add new field/unknown fields into a `json` field at target table | an out-of-band field the source adds without notice |
| A **required** column disappears | **Reject & page** | Fail the load loudly *before* it corrupts the table | homework schema-guard: missing `user_id` |

The rule of thumb: **additive is safe to accept, type drift you absorb by normalizing, unknown
you drop, and a missing contract column stops the line.** Enforcement is the default so *you*
decide which changes to opt into — never the feed.
