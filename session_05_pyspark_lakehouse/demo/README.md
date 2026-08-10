# Session 5 Demo — Run Book

**Two demos.**
- Demo 1 runs the ad pipeline at volume and looks at how the Delta table is
physically built.
- Demo 2 walks the 3-day dump through a **schema change** and shows what
breaks and how it's handled.

*Every command and number below has been run end to end.*

All commands run from `session_05_pyspark_lakehouse/`. Bring the stack up, generate + upload
the 3-day dump, and run `init_schema.py` first — see
[../README.md](../README.md#generate-the-data-and-create-the-tables). The jars and Delta/S3A
config are baked into spark-master's `spark-defaults.conf`, so `spark-submit`/`spark-sql`
below need no `--jars` or `--conf`.

## Why these two demos

1. **Demo 1** proves the pipeline on real volume (50k events/day) and shows the two things
   that make a Delta table production-grade: **deduplication** (so views aren't inflated) and
   the **table design** (partitioning + properties that keep files healthy).
2. **Demo 2** is what actually happens in production: the **source schema changes**. We watch
   Delta reject a change, then accept it with `mergeSchema`, then hit a change `mergeSchema`
   can't fix — and see why the loader normalizes types.

---

## Demo 1 — The pipeline at volume, and the table underneath

### 1a. Run Day 1

```bash
docker compose exec airflow-scheduler airflow dags unpause session_05_ad_daily_metrics
docker compose exec airflow-scheduler airflow dags trigger session_05_ad_daily_metrics -e 2026-06-26
```

In the Grid view (`http://localhost:8888`, `airflow`/`airflow`) all four tasks go green:
`bronze_2_silver__validate_raw_file → bronze_2_silver__build_fact_ad_events →
silver_2_gold__aggregate_ad_daily → reconcile__silver_gold` (task ids carry the medallion
transition they run).

### 1b. Deduplication — retries don't inflate the views

Day 1's raw file has 53,600 rows but only 52,528 distinct `event_id`s (an ad server retried ~2%
of events). The fact keeps one per event:

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) AS fact_rows FROM silver.fact_ad_events WHERE event_date = DATE '2026-06-26'"
# 52528   — deduped from 53600 raw; counting the 1072 duplicates would over-report views
```

### 1c. The daily metric — views per ad

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT ad_id, impressions, clicks, unique_users, ctr FROM gold.agg_ad_daily
      WHERE event_date = DATE '2026-06-26' ORDER BY impressions DESC LIMIT 5"
# AD-0083  304  10  306  0.0329
# AD-0114  293  6   293  0.0205
# AD-0023  290  6   290  0.0207
# ...   (200 ads; ~50,939 total views, ~1,589 clicks, CTR ≈ 3.1% — a realistic display CTR)
```

`reconcile` already proved these tie out — and it does it **entirely in Trino**, never pulling
raw data into the Airflow worker: dedup held (`COUNT(*) == COUNT(DISTINCT event_id)`), fact
impressions == summed gold impressions, no gold row breaks a rule (clicks ≤ impressions,
ctr ∈ [0,1], reach ≤ events), and the day's volume is within range of the previous day.

### 1d. Idempotency — rerun the day, nothing doubles

```bash
docker compose exec airflow-scheduler airflow tasks clear session_05_ad_daily_metrics -s 2026-06-26 -e 2026-06-26 -y
# after it finishes, fact_ad_events for 2026-06-26 is still 52528, not 105056
```

Both writes use `replaceWhere` on `event_date`, so a rerun replaces the date's partition.

### 1e. How the table is built — partitioning + properties

The table isn't bare. `init_schema.py` created it partitioned by `event_date` with production
properties:

```bash
docker exec spark-master /opt/spark/bin/spark-sql -e "SHOW TBLPROPERTIES silver.fact_ad_events"
# delta.autoOptimize.optimizeWrite   true
# delta.autoOptimize.autoCompact     true
# delta.dataSkippingNumIndexedCols   8
# delta.logRetentionDuration         interval 30 days
# delta.deletedFileRetentionDuration interval 7 days
```

The dedup does a shuffle (default 200 partitions), which would otherwise write ~200 tiny files
per day. `optimizeWrite`/`autoCompact` coalesce each load — after all 3 days the fact is just
**3 files** (one per partition):

```bash
docker exec spark-master /opt/spark/bin/spark-sql -e "DESCRIBE DETAIL silver.fact_ad_events"
# look at the numFiles column -> 3  (one file per day partition, not ~600)
```

**Maintenance** — `OPTIMIZE … ZORDER BY (ad_id)` co-locates rows by ad so a `WHERE ad_id = …`
query skips files (data skipping); `VACUUM` reclaims old files:

```bash
docker exec spark-master /opt/spark/bin/spark-sql -e "OPTIMIZE silver.fact_ad_events ZORDER BY (ad_id)"
# already ~1 file/partition here (auto-compact), so file count stays 3 — the win is skipping, not packing
```

> **VACUUM vs time travel.** `VACUUM` deletes files older than `deletedFileRetentionDuration`
> (7 days here). Run it with a shorter retention and older `VERSION AS OF` queries stop
> working — retention is the dial between reclaiming space and keeping history.

**Time travel Syntax:**
```sql
-- check the version of table first
SELECT version, timestamp, operation
FROM "fact_ad_events$history"
ORDER BY version DESC
;

-- E.g i want to time travel this table at version 7
SELECT COUNT(*) AS fact_rows
FROM delta.silver.fact_ad_events FOR VERSION AS OF 7
WHERE event_date = DATE '2026-06-26';
```

---

## Demo 2 — Schema evolution across the 3-day dump

The source team changes the feed. Load the next two days and watch it.

### 2a. Day 2 adds a column (`device_type`) — additive

```bash
docker compose exec airflow-scheduler airflow dags trigger session_05_ad_daily_metrics -e 2026-06-27
```

The loader writes with `.option("mergeSchema", "true")`, so Delta **adds** `device_type` to the
table. Old rows (Day 1) simply read NULL for it:

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT event_date, COUNT(*) rows, COUNT(device_type) has_device
      FROM silver.fact_ad_events GROUP BY event_date ORDER BY event_date"
# 2026-06-26  52528  0       <- column didn't exist yet -> NULL
# 2026-06-27  40964  40964
# 2026-06-28  39690  39690
```

> **Without `mergeSchema`, this write fails** — Delta enforces the schema by default:
> `AnalysisException: A schema mismatch detected when writing to the Delta table`. That
> enforcement is a feature (it stops a malformed feed); `mergeSchema` is how you opt into a
> change you actually want.

### 2b. Day 3 changes a type (`campaign_id` int → string) — breaking

```bash
docker compose exec airflow-scheduler airflow dags trigger session_05_ad_daily_metrics -e 2026-06-28
```

Day 3 also ships `campaign_id` as a string code (`"cmp_07"`) instead of an integer. A type
change is **not** something `mergeSchema` can absorb — if the loader wrote the raw inferred
types it would fail:

```
AnalysisException: [DELTA_FAILED_TO_MERGE_FIELDS] Failed to merge fields 'campaign_id' and 'campaign_id'
```

The loader avoids it by **normalizing id columns to `string` at ingestion** — so the fact's
`campaign_id` type never changes, whatever the source sends. Day 3 lands fine:

```bash
docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT DISTINCT campaign_id FROM silver.fact_ad_events WHERE event_date = DATE '2026-06-28' ORDER BY campaign_id LIMIT 3"
# cmp_01
# cmp_02
# cmp_03    (and Day 1/2's integer campaign ids are stored as '1','2',… — one stable string type)
```

### 2c. See both failures for yourself (optional, on a scratch table)

```bash
docker exec spark-master /opt/spark/bin/spark-sql -e "
CREATE TABLE silver.schema_demo (ad_id STRING, campaign_id INT, event_date DATE)
  USING delta PARTITIONED BY (event_date) LOCATION 's3a://silver/schema_demo';
INSERT INTO silver.schema_demo VALUES ('AD-1', 5, DATE '2026-06-26');
-- add a column without mergeSchema -> 'A schema mismatch detected'
INSERT INTO silver.schema_demo (ad_id, campaign_id, device_type, event_date)
  VALUES ('AD-1', 6, 'mobile', DATE '2026-06-27');
"
docker exec spark-master /opt/spark/bin/spark-sql -e "DROP TABLE silver.schema_demo"
```

### 2d. Time travel — the schema before the change is still reachable

```bash
docker exec spark-master /opt/spark/bin/spark-sql -e "DESCRIBE HISTORY silver.fact_ad_events"
# version 0 CREATE TABLE, then a WRITE per day (2026-06-26/27/28), then OPTIMIZE.

docker exec spark-master /opt/spark/bin/spark-sql \
  -e "SELECT COUNT(*) FROM silver.fact_ad_events VERSION AS OF 1"
# 52528 — the state right after Day 1's load, before device_type existed, still queryable
```

---

## Setup gotchas (seen while preparing this lab)

| Symptom | Cause | Fix |
|---|---|---|
| DAG triggered but tasks stay blank / never run | New DAGs are created **paused** | `docker compose exec airflow-scheduler airflow dags unpause session_05_ad_daily_metrics` |
| `spark-submit: can't open '/opt/jobs/demo/…': No such file` | `spark-master` missing the jobs mount (added after first `up`) | `docker compose up -d spark-master` to apply the mount |
| `reconcile` fails `HIVE_CANNOT_OPEN_SPLIT … NoSuchKey`, or Trino returns a stale/doubled count right after a write | Trino cached an old Delta snapshot (read-after-write) | `delta.metadata.cache-ttl=0s` is already set; to clear a cache from before that, run `CALL delta.system.flush_metadata_cache()` in Trino |
| Re-running `init_schema.py` after a manual reset fails with a schema conflict | `DROP TABLE` leaves the Delta files at the S3 location | Also remove the location: `mc rm --recursive --force local/silver/fact_ad_events/` then re-init |

---

## Cleanup resources (optional)

Remove the demo rows so a later class starts fresh; the tables and their properties stay:

```bash
docker exec spark-master /opt/spark/bin/spark-sql -e "
DELETE FROM silver.fact_ad_events WHERE event_date BETWEEN DATE '2026-06-26' AND DATE '2026-06-28';
DELETE FROM gold.agg_ad_daily  WHERE event_date BETWEEN DATE '2026-06-26' AND DATE '2026-06-28';"
```
