# Session 5 Lab — Setup & Exercises

**Master Class DataOps for Modern Data Platforms · Session 5/11**

You build the ad-analytics pipeline the demo runs: a **deduped** Delta fact of ad events
(silver), a **daily per-ad aggregate** (gold — views, clicks, reach, CTR), and a **reconcile**
that verifies them in the query engine. Along the way the source schema changes — and you handle it.

The stack lives at the **session root** — bring it up, generate + upload the 3-day dump, and
run `init_schema.py` first, following [../README.md](../README.md#bring-up-the-stack).

## 1. Prerequisites

- Stack healthy (`docker compose ps`), the four jars in `jars/`.
- The 3-day dump (`2026-06-26/27/28`) generated and uploaded to `raw/ad_events/`, and
  `init_schema.py` run once — see [../README.md](../README.md#generate-the-data-and-create-the-tables).
- Airflow UI at `http://localhost:8888` (`airflow`/`airflow`).

## 2. What you complete

The DAG `lab/dags/session_05_ad_daily_metrics_starter.py` wires four tasks (their ids carry the
medallion transition): `bronze_2_silver__validate_raw_file →
bronze_2_silver__build_fact_ad_events → silver_2_gold__aggregate_ad_daily →
reconcile__silver_gold`. You fill in two
Spark jobs, the reconcile task, and one DAG line. The reference `demo/` versions are the same
shape — read them if you get stuck, but write your own first.

| # | File | What to do |
|---|---|---|
| Lab 1 | `lab/jobs/build_fact_ad_events.py` | Clean + **dedup** events, idempotent `replaceWhere` load. |
| Lab 2 | `lab/jobs/aggregate_ad_daily.py` | `agg_ad_daily` — views/ad/day, clicks, reach, CTR. |
| Lab 3 | `build_fact_ad_events.py` + the `reconcile` task | Survive Day 2's new column (`mergeSchema`) and write the reconcile checks. |
| DAG | the DAG's `default_args` | Add `on_failure_callback=notify_on_failure`. |

Edits under `lab/` are picked up automatically (scheduler rescans ~30s; Spark jobs re-read per run).

## 3. Table schemas — what goes in, what you produce

**Input — `s3a://bronze/ad_events/{ds}.csv`** (one row per ad event; the schema grows over the 3 days):

| Column | Type | Notes |
|---|---|---|
| `event_id` | string | unique per event, but **~2% are duplicated** (retries) — you dedup on this |
| `ad_id` | string | e.g. `AD-0042` |
| `campaign_id` | int → **string** | integer on Day 1–2, string code `"cmp_07"` from **Day 3** |
| `user_id` | string | e.g. `U-01234` |
| `event_type` | string | `impression` or `click` |
| `event_ts` | timestamp | event time (within the day) |
| `device_type` | string | **added Day 2** — `mobile` / `desktop` / `tablet` |
| `cost_micros` | int | **added Day 3** — spend in micros |

**Lab 1 output — `silver.fact_ad_events`** (deduped events, partitioned by `event_date`):

| Column | Type | How you build it |
|---|---|---|
| `event_id` | string | cast to string |
| `ad_id` | string | cast to string |
| `campaign_id` | string | cast to string → **stable type across the Day-3 change** |
| `user_id` | string | cast to string |
| `event_type` | string | keep only `impression` / `click` |
| `event_ts` | timestamp | from source |
| `device_type` | string | evolves in on Day 2 (NULL for Day-1 rows) |
| `cost_micros` | int | evolves in on Day 3 (NULL for Day-1/2 rows) |
| `event_date` | date | `to_date(event_ts)` — the **partition column** |

> `event_id … event_ts` + `event_date` exist from Day 1 (see `jobs/init_schema.py`); `device_type`
> and `cost_micros` are **added by your write** via `mergeSchema` when they first appear (Lab 3).

**Lab 2 output — `gold.agg_ad_daily`** (one row per ad per day, partitioned by `event_date`):

| Column | Type | How you build it |
|---|---|---|
| `event_date` | date | group key (partition column) |
| `ad_id` | string | group key |
| `campaign_id` | string | group key |
| `impressions` | bigint | `count` of rows where `event_type = 'impression'` |
| `clicks` | bigint | `count` of rows where `event_type = 'click'` |
| `unique_users` | bigint | `countDistinct(user_id)` |
| `ctr` | double | `round(clicks / impressions, 4)`, `0.0` when no impressions |

## 4. Lab 1 — the deduped fact (Day 1)

`lab/jobs/build_fact_ad_events.py`. The Spark session and the raw CSV read are given.

- **TODO 1 — clean + dedup:** cast the id columns to `string`; keep `event_type IN
  ('impression','click')`; add `event_date = to_date(event_ts)`; **dedup by `event_id`**,
  keeping the earliest `event_ts` (a retry sends the same event twice — counting both inflates
  views). A window does it:
  `Window.partitionBy("event_id").orderBy(col("event_ts").asc())` → `row_number() == 1`.
- **TODO 2 — idempotent load:** write Delta `mode("overwrite").option("replaceWhere",
  f"event_date = '{ds}'").save("s3a://silver/fact_ad_events")`.

Trigger Day 1 and confirm (all `docker compose` commands from the session root):

```bash
docker compose exec airflow-scheduler airflow dags trigger session_05_ad_daily_metrics_starter -e 2026-06-26
```

`build_fact_ad_events` should load **52,528** rows (deduped from 53,600 raw).

## 5. Lab 2 — the daily aggregate

`lab/jobs/aggregate_ad_daily.py`. This date's fact rows are read for you.

- **TODO 1:** group by `event_date, ad_id, campaign_id` and compute `impressions` (count of
  `event_type == 'impression'`), `clicks`, `unique_users = countDistinct("user_id")`, and
  `ctr = round(clicks/impressions, 4)` (0.0 when there are no impressions).
- **TODO 2:** idempotent `replaceWhere` load into `agg_ad_daily`.

After a run, `agg_ad_daily` has **200 rows** for the date (one per ad), CTR ≈ 3%.

## 6. Lab 3 — survive a schema change, then reconcile

Now trigger **Day 2** — the source added a `device_type` column:

```bash
docker compose exec airflow-scheduler airflow dags trigger session_05_ad_daily_metrics_starter -e 2026-06-27
```

`build_fact_ad_events` **fails**: `AnalysisException: A schema mismatch detected`. Delta
enforces the schema by default and rejects the new column.

- **Fix (Lab 3a):** add `.option("mergeSchema", "true")` to your fact write in Lab 1. Rerun
  Day 2 — it succeeds, and Day 1's rows read `device_type` as NULL (the column didn't exist
  then). Day 3 (`campaign_id` becomes a string code) also just works, because you already cast
  the id columns to `string` in Lab 1 — that's what keeps the type stable.
- **Lab 3b — the reconcile task:** `sql/reconcile.sql` (given, runs **entirely in Trino** — no
  raw scan) returns `(fact_rows, fact_distinct, fact_impr, agg_impr, bad_rows, prev_rows)`. Write
  the checks, each raising on failure:
  1. `fact_rows == fact_distinct` — dedup held (no duplicate event_ids).
  2. `fact_impr == agg_impr` — aggregate ties to fact.
  3. `bad_rows == 0` — business rules hold.
  4. `prev_rows` and `abs(fact_rows - prev_rows) / prev_rows > MAX_DAY_OVER_DAY_DRIFT` — day-over-day volume guard.
- **DAG TODO:** add `on_failure_callback=notify_on_failure` to `default_args`.

## 7. Acceptance criteria

- Before you finish, the Spark tasks fail (`NotImplementedError`) and `reconcile` fails — the
  "not done yet" signals.
- Day 1 green: `fact_ad_events` = **52,528** rows, `agg_ad_daily` = **200** ads, reconcile passes.
- **Idempotency:** clear and rerun a day — counts unchanged, not doubled.
- **Schema change:** without `mergeSchema`, Day 2 fails with `A schema mismatch detected`; with
  it, Day 2/3 load and Day 1's `device_type`/`cost_micros` read NULL.
- If you drop the dedup, `reconcile` fails (fact rows = 53,600 ≠ 52,528 distinct) — put it back.

## 8. Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A Spark task fails, `NotImplementedError` | TODO not done | Complete the relevant `lab/jobs/*.py`. |
| `A schema mismatch detected` on Day 2 | `mergeSchema` missing | Add `.option("mergeSchema","true")` to the fact write (Lab 3a). |
| `DELTA_FAILED_TO_MERGE_FIELDS ... campaign_id` | ids not normalized | Cast id columns to `string` in Lab 1 TODO 1. |
| `reconcile` fails on fact rows | Dedup missing/wrong | Keep exactly one row per `event_id`. |
| `Py4JJavaError ... NoClassDefFoundError: DeltaLog` | Jars missing | Confirm all four jars in `jars/`. |
