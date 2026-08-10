# Session 3 Homework — Music Streaming DB→DB Pipeline

Build a daily pipeline that reads from a **relational database**, transforms with a
join + aggregation, and writes the result **back into a table** — applying every
operating principle from session 3. This is the "real" version of the lab, which
only generated data in memory.

You run it on the same Airflow stack from session 2.

## The scenario

A music service stores raw listening data in Postgres. Two normalised source tables:

| Table | Columns |
|---|---|
| `tracks` | `track_id`, `title`, `artist`, `genre` |
| `plays` | `play_id`, `track_id`, `user_id`, `played_at`, `ms_played` |

Your pipeline produces one row per (day, genre) in the target table:

| Table | Columns |
|---|---|
| `daily_genre_stats` | `dt`, `genre`, `play_count`, `unique_listeners`, `minutes_played` |

To get `genre` you must **join** `plays` to `tracks`.

## 1. Setup (once)

Reuse the Postgres container from the session 2 stack — you only add a separate
`homework` database inside it, so nothing touches Airflow's own metadata.

```bash
cd session_02_airflow_intro/lab

# 1. Create the homework database
docker compose exec postgres createdb -U airflow homework

# 2. Load the schema + deterministic seed data (6 tracks, 200 plays over 5 days)
cat ../../session_03_airflow_dag/homework/seed/schema.sql \
  | docker compose exec -T postgres psql -U airflow -d homework

# 3. Register an Airflow Connection pointing at it
docker compose exec airflow-scheduler airflow connections add homework_db \
  --conn-uri 'postgresql://airflow:airflow@postgres:5432/homework'
```

Put your DAG file in `session_03_airflow_dag/homework/submissions/` —
that folder is mounted into Airflow at `/opt/airflow/dags/homework`, so the
scheduler picks it up within ~30s.

## 2. Your task

Write a DAG `session_03_genre_daily_stats` that, for each logical date, reads
`plays` of that day, joins to `tracks`, aggregates per genre, and writes the result
into `daily_genre_stats`. Then reconcile.

Suggested shape: `transform_load >> reconcile` (or split extract/transform/load if
you prefer). Connect to the database with
`PostgresHook(postgres_conn_id="homework_db")`.

### Requirements — the transform MUST follow these

| Principle | What your pipeline must do |
|---|---|
| **Determinism** | Filter the source by the run's date window: `played_at >= {{ ds }}` and `< {{ ds }} + 1 day`. Never use `now()` / `CURRENT_DATE`. |
| **Idempotency** | Load = `DELETE FROM daily_genre_stats WHERE dt = {{ ds }}` then `INSERT`. Re-running a date must not duplicate rows. |
| **Atomicity** | Run the DELETE + INSERT in a **single transaction** (commit once; roll back on error). A failure must not leave a half-written day. |
| **Fault tolerance** | Set `retries`, `retry_delay`, and `execution_timeout`. |
| **Alerting** | Wire `on_failure_callback=notify_on_failure` (from `dataops_common.notifications`) so a failing task — e.g. a reconcile mismatch — alerts. It posts to Slack when `slack_webhook_url` is set, and just logs otherwise. |
| **Reconciliation** | A final task must assert `COUNT(*) of plays for the day == SUM(play_count) in daily_genre_stats for that day`, and fail loudly if not. |
| **No hardcoded credentials** | Connect only via the `homework_db` Connection — no user/password in code. |
| **Scheduling** | `@daily`, a fixed `start_date` (`2024-03-01`), `catchup=False`, `max_active_runs=1`. |

### How to build it — hints

**Which operator?** For this homework use the TaskFlow `@task` decorator with a `PostgresHook`, so you drive the DELETE + INSERT transaction (commit once, roll back on error) explicitly in Python and see every step. Scaffold the DAG the same way `../demo/dags/campaign_daily_metrics.py` does (`@dag`, `default_args`, `@task`), so read that file first.

> **Heads-up for session 4.** Doing the transaction by hand here makes the mechanics visible, but it is *not* the pattern you'll standardise on. Session 4 moves SQL out of Python entirely — a `SQLExecuteQueryOperator` running an external `.sql` file with `BEGIN … COMMIT` inside it gives you the same atomic DELETE + INSERT with no hand-written connection code. Treat this hook version as the "see it work manually" step before that.

**Getting `{{ ds }}` inside a task:** name a parameter `ds` and Airflow injects the run's date as a `"YYYY-MM-DD"` string — no manual templating needed:

```python
@task
def transform_load(ds: str | None = None):
    ...
```

**The transaction (this is the shape — you fill in the SQL):**

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id="homework_db")
conn = hook.get_conn() # psycopg2 connection, autocommit OFF by default
try:
    with conn.cursor() as cur:
        # Write your logic code here.
    conn.commit()
except Exception:
    conn.rollback()
    raise  # re-raise so the task fails (and on_failure_callback fires)
finally:
    conn.close()
```

**Reconcile** is a second `@task`: read `COUNT(*)` from `plays` for the day and `SUM(play_count)` from `daily_genre_stats` for the day (use `hook.get_first(sql, parameters=...)`), and `raise` if they differ. Wire it `transform_load(...) >> reconcile(...)`.

**Alerting:** `from dataops_common.notifications import notify_on_failure`, then put `"on_failure_callback": notify_on_failure` in `default_args` (same as the lab DAGs).

## 3. Test it yourself

1. **Runs clean:** trigger for `2024-03-01`, confirm `daily_genre_stats` has 3 rows (pop / rock / jazz) and reconcile passes.
2. **Idempotent:** rerun the same date by **clearing** its run (a second `dags trigger` for the same date is refused as `DagRunAlreadyExists`) — still 3 rows for that day, not 6.
3. **Backfill:** `docker compose exec airflow-scheduler airflow dags backfill session_03_genre_daily_stats -s 2024-03-01 -e 2024-03-05 --reset-dagruns` → one set of rows per day, each with that day's data.
4. **Fault tolerance:** on a **triggered** run (from step 1, not a `backfill` run), clear one task in the UI → only that task reruns. (Clearing a task inside a `backfill` run isn't rescheduled — use a manual trigger.)

Inspect results:

```bash
docker compose exec postgres psql -U airflow -d homework \
  -c "SELECT * FROM daily_genre_stats ORDER BY dt, genre;"
```

## 4. Acceptance criteria

- [ ] Reads from `plays` + `tracks` via a join, filtered by the run's date window (no `now()`).
- [ ] Load is idempotent (DELETE-by-date + INSERT) and atomic (one transaction).
- [ ] `retries` + `execution_timeout` set; reconcile task fails on a mismatch.
- [ ] `on_failure_callback=notify_on_failure` wired so a task failure alerts (Slack if configured, else logged).
- [ ] Connects only through the `homework_db` Connection.
- [ ] Backfilling 5 days produces correct per-day rows; re-running any day does not duplicate.

## 5. Submit — open a Pull Request

1. Create a branch: `git checkout -b homework/session-03/<your-name>`.
2. Add your DAG under `session_03_airflow_dag/homework/submissions/`.
3. Commit and push, then **open a Pull Request** against `main`.
4. In the PR description, include: what you did, a screenshot of `daily_genre_stats` after a backfill, and answers to:
   - Why must the load be one transaction rather than a separate DELETE then INSERT?
   - If you filtered with `CURRENT_DATE` instead of the run's date, what would a backfill of 2024-03-02 produce?

The PR is for review only — a reviewer reads your code and the answers above directly on the PR. It will not be merged, so there's no need to worry about conflicts with other submissions.
