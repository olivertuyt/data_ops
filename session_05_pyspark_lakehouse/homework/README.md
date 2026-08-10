# Session 5 Homework — An Ad Dimension Upserted with MERGE, and a Schema-Drift Guard

Two take-home tasks on the ad lakehouse you built in the lab. Both push past what the demo and
lab did: the pipeline only ever **overwrote** partitions (`replaceWhere`), so you build the
lakehouse write it never showed — a **`MERGE` upsert** that accumulates a dimension in place —
and a guard that **detects schema drift** before it corrupts a table. Run the reference pipeline
(`session_05_ad_daily_metrics`) for `2026-06-26/27/28` first so `agg_ad_daily` exists.

Put your code under `homework/submissions/` — that folder is mounted into Airflow at
`/opt/airflow/dags/session_05_homework` and into Spark at `/opt/jobs/homework`. Build the same
way as the lab: a Spark job submitted by `SparkSubmitOperator`, credentials from the
`AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` env vars (no hardcoding), writes idempotent with
`replaceWhere`, `on_failure_callback=notify_on_failure`. Inspect results from Trino
(`localhost:8090`) or DBeaver — see [../README.md](../README.md#querying-from-trino-or-dbeaver-on-your-host).

---

## Homework 1 — An ad dimension, upserted with `MERGE` (required)

The demo's `agg_ad_daily` is a **fact** — each day recomputed and overwritten. A **dimension**
is different: it **accumulates**, one row per entity, updated in place as new days arrive. That
is the workhorse lakehouse write the demo and lab never used — every write so far was an
`overwrite`/`replaceWhere`. Here you build it with Delta's **`MERGE INTO`** (the same upsert
session 4 did in SQL, now in Spark).

### 1a. Build `gold.dim_ad`

Maintain one row per ad. Target table `gold.dim_ad`, partitioned **not** needed (small,
one row per ad), columns:
`ad_id STRING, campaign_id STRING, first_seen_date DATE, last_active_date DATE, latest_ctr DOUBLE`.

Write a job `submissions/upsert_dim_ad.py` and a DAG `session_05_dim_ad` that, per run date:
1. reads `gold.agg_ad_daily` for `event_date = ds` (today's per-ad snapshot),
2. **`MERGE INTO gold.dim_ad`** on `ad_id`:
   - **WHEN MATCHED** → `campaign_id = s.campaign_id` (an ad's campaign code can change — day 3
     ships `"cmp_07"`; SCD Type 1 overwrites), `first_seen_date = least(t.first_seen_date, ds)`,
     `last_active_date = greatest(t.last_active_date, ds)`, `latest_ctr = s.ctr`,
   - **WHEN NOT MATCHED** → insert with `first_seen_date = last_active_date = ds`.

> **Why `least`/`greatest` and not just `= ds`?** So the merge is **idempotent and
> order-independent** — re-running an *old* date must not move `last_active_date` backwards or
> reset `first_seen_date`. A plain `= ds` would corrupt the dates on any backfill or rerun. This
> is the subtlety `MERGE` adds over `replaceWhere`: you own the update rule.

### 1b. Reconcile the dimension

Add a `reconcile_dim` task (Trino query, like the pipeline's `reconcile`) that **fails the DAG**
if any of these hold after the merge:

| Check | Rule |
|---|---|
| One row per ad | `COUNT(*)` == `COUNT(DISTINCT ad_id)` (the merge key held) |
| Dates sane | no row with `first_seen_date > last_active_date` |
| Complete | every `ad_id` present in `agg_ad_daily` up to `ds` exists in `dim_ad` |
| CTR valid | every `latest_ctr` is `BETWEEN 0 AND 1` |

### Test it yourself
1. Run `2026-06-26` then `27` then `28`: `dim_ad` holds **200 rows** (one per ad), each with
   `first_seen_date = 2026-06-26` and `last_active_date = 2026-06-28`; `campaign_id` for every ad
   is now a `"cmp_NN"` string (day 3's SCD-1 overwrite won).
2. **Idempotency + order-independence:** re-run `2026-06-26` *after* the 28th — still 200 rows,
   and `last_active_date` stays `2026-06-28` (not reset to the 26th). If it moves, your merge
   used `= ds` instead of `greatest/least` — fix it.
3. Break it on purpose: change the merge to `WHEN MATCHED THEN UPDATE SET last_active_date = ds`
   (no `greatest`), rerun an old date, and watch `reconcile_dim`'s date-sanity check — then put
   it back.

---

## Homework 2 — A schema-drift guard (required)

The lab's loader survives schema change with `mergeSchema` + normalization. But a pipeline
should also **notice** drift and decide what's acceptable, rather than absorb anything
silently. Build a guard that runs **before** the load.

### Your task

Write `submissions/schema_guard.py` with a function that compares the incoming file's schema to
an **expected** schema and classifies the drift:

```python
EXPECTED = {           # column -> expected Spark type name
    "event_id": "string", "ad_id": "string", "campaign_id": "string",
    "user_id": "string", "event_type": "string", "event_ts": "timestamp",
}
REQUIRED = set(EXPECTED)          # these must be present

def check_schema(df) -> dict:
    incoming = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    return {
        "added":   sorted(set(incoming) - set(EXPECTED)),          # new columns
        "missing": sorted(REQUIRED - set(incoming)),               # required columns gone
        "retyped": sorted(c for c in EXPECTED if c in incoming      # type changed
                          and incoming[c] != EXPECTED[c]),
    }
```

Wire it as a task (or at the top of the fact job) that, for the run date:
- **allows and logs** additive columns (`added`) — that's normal evolution;
- **allows** `retyped` **id** columns because the loader normalizes them to string, but **logs
  a warning** so the drift is visible;
- **fails loudly** if any `REQUIRED` column is `missing`, with a message naming it.

### Test it yourself
1. **Day 3** (`2026-06-28`): the guard reports `added = ['cost_micros']` (device_type arrived on
   Day 2), `retyped = ['campaign_id']` (int→string) — and the run proceeds. (Note `campaign_id`
   inferring as string only shows up because Day 3 ships `"cmp_07"`; the guard should print both.)
2. **A broken file:** drop the `user_id` column from a copy of a day's CSV, re-upload, and
   confirm the guard **fails** with something like `missing required column(s): ['user_id']`
   instead of loading a fact with no user ids.

---

## Submit — open a Pull Request

1. `git checkout -b homework/session-05/<your-name>`.
2. Add your jobs + DAG under `session_05_pyspark_lakehouse/homework/submissions/`.
3. Commit, push, open a **Pull Request** against `main` (review-only, not merged).
4. In the PR describe: the `dim_ad` row count + what happened to `last_active_date` when you
   re-ran an old date (HW1); the guard's output on Day 3 and on the broken file (HW2); and your
   short answer below.

### Short answer (in the PR)

In a few sentences: **schema enforcement vs evolution** — when should a pipeline `mergeSchema`
and accept a change, and when should it reject and page someone? And one line on **partitioning
+ `OPTIMIZE`/`ZORDER`**: why partition `fact_ad_events` by `event_date` and not by `ad_id`?

## Acceptance criteria

- [ ] HW1: `dim_ad` upserted with `MERGE INTO` (one row per `ad_id`); the merge is idempotent and
  order-independent (`greatest`/`least` on the dates), proven by re-running an old date.
- [ ] HW1: `reconcile_dim` fails the DAG on any broken rule; job via `SparkSubmitOperator`, creds
  from env, `on_failure_callback` wired.
- [ ] HW2: `check_schema` classifies added/missing/retyped; the guard allows additive + retyped
  ids, **fails** on a missing required column; demonstrated on Day 3 and a broken file.
- [ ] Short answer included.
