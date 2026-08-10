-- Lab 2 source (given) — the day's customer snapshot, deterministic from {{ ds }}.
-- 5,000 customers (id 201..5200). The slice id 201..700 moves gold -> platinum on
-- 2024-01-03 (that is what SCD2 versions). The feed is noisy on purpose — ~100
-- duplicate rows and ~50 null-tier rows — so the Silver validate step has real work.
-- Idempotent: rerunning a date replaces that date's rows.
BEGIN;

DELETE FROM bronze.customer_snapshots WHERE snapshot_date = DATE '{{ ds }}';

INSERT INTO bronze.customer_snapshots
WITH gen AS (
    SELECT UNNEST(RANGE(5000)) AS i
)

SELECT
    201 + i AS customer_id,
    CASE
        WHEN i < 500
            THEN (CASE WHEN DATE '{{ ds }}' >= DATE '2024-01-03' THEN 'platinum' ELSE 'gold' END)
        ELSE (['bronze', 'silver', 'gold', 'platinum'])[(i % 4) + 1]
    END AS tier,
    DATE '{{ ds }}' AS snapshot_date
FROM gen
UNION ALL
-- ~100 duplicate retries (replay id 5101..5200 verbatim) — Silver must dedup
SELECT
    5101 + i AS customer_id,
    (['bronze', 'silver', 'gold', 'platinum'])[((4900 + i) % 4) + 1] AS tier,
    DATE '{{ ds }}' AS snapshot_date
FROM (SELECT UNNEST(RANGE(100)) AS i)
UNION ALL
-- ~50 rows with a missing tier — Silver must drop these
SELECT
    6001 + i AS customer_id,
    NULL AS tier,
    DATE '{{ ds }}' AS snapshot_date
FROM (SELECT UNNEST(RANGE(50)) AS i);

COMMIT;
