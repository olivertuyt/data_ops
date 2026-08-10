# Session 6 Homework — Performance Diagnosis

## Run

**HW 1 / HW 2 (Spark):**
```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /homework/hw1.py
```
Swap `hw1.py` for `hw2.py`. Analyze completed runs at **http://localhost:18080**.

**HW 3 (host DuckDB — no Spark):**
```bash
# from session_06_performance_optimization/
pip install -r requirements.txt   # one-time: just duckdb
python homework/hw3.py
```

---

## The tickets

| HW | Reported problem |
|---|---|
| HW 1 | "The seller/product stats job takes forever and sometimes crashes the workers." |
| HW 2 | "The risk-score report reads modest data but pins the CPU for a very long time." |
| HW 3 | "This SQL report is slow, and yesterday's numbers don't match today's re-run." |

---

## HW 1 — Seller/product stats

**Business context:** a daily job aggregates `silver/order_items` (50M rows) into a seller × product × category stats table written to `s3a://gold/hw1_seller_product_stats`.

**Where to look:**
- History Server → the hw1 app → **Executors** tab.
- History Server → **Stages** tab: task count, task duration spread, any failed tasks.

**Evidence to capture (before fix):**
- Executor tab: memory and cores per executor
- Stage metrics: failed tasks or error messages in logs

**Acceptance criteria:**
- The fix changes only the Spark session config in `hw1.py` — not the aggregation logic or output path.
- Re-run completes without task failures.
- Before/after numbers recorded.

---

## HW 2 — Risk-score report

**Business context:** a risk-score report joins `order_items`, `orders`, and `payments`, computes a score per row, and aggregates by `order_status`. It runs correctly but takes far longer than the data size suggests.

**Where to look:**
- History Server → the hw2 app → **Stages** tab: elapsed time and input row count of the scoring stage.
- History Server → **SQL/DataFrame** tab: look at the physical plan for the scoring step.

**Evidence to capture (before fix):**
- Scoring stage elapsed time
- SQL physical plan (screenshot or paste)

**Acceptance criteria:**
- The output (`total_risk_score` by `order_status`) is numerically identical before and after.
- Scoring stage elapsed time drops by at least 1.5×.
- Before/after numbers recorded.

---

## HW 3 — DuckDB daily report

**Business context:** a daily report filters `orders` by a single date. The data is already split into 22 daily files — one per day — but the query still reads every file on every run.

**Run:**
```bash
# from session_06_performance_optimization/
python3 homework/hw3.py
```

**Where to look:**
- `EXPLAIN ANALYZE` → `TABLE_SCAN` node: where is the date filter applied — `Filters` or `File Filters`?
- `Total Files Read` — how many files does DuckDB open?
- Compare against the actual file count in `data/orders_by_day/`

**Evidence to capture (before fix):**
- Value of `Total Files Read`
- Which section the date filter appears in (`Filters` vs `File Filters`)

**Acceptance criteria:**
- `EXPLAIN ANALYZE` shows `Scanning Files: 1/22` and `Total Files Read: 1`
- The date filter moves from `Filters` to `File Filters`
- Do not change `DATE = "2026-07-07"` or the query logic — only change how data isgi organized and read

**Hints:**

<details>
<summary>Hint 1</summary>
DuckDB can only skip files if it knows which file covers which date — before opening any file. The current layout gives DuckDB no way to know that without reading the content first.
</details>

<details>
<summary>Hint 2</summary>
Two changes are required and must go together: reorganize the files so the folder name encodes the date (<code>order_date=2026-07-07/</code>), then tell DuckDB to use that information when scanning (<code>hive_partitioning=true</code>). Either change alone does nothing.
</details>

---

## Completion checklist (per homework)

- [ ] Ran the broken pipeline and captured evidence (screenshot or pasted metrics — not prose)
- [ ] Named the real root cause backed by that evidence
- [ ] Applied the fix and re-ran the pipeline
- [ ] Recorded before/after numbers in a table
- [ ] Explained in 2–3 sentences why the fix addresses the root cause
