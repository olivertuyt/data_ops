-- Lab 2 Silver step (given) — validate the raw snapshot before it becomes a dimension.
-- Drops null/unknown tiers and dedups to one row per (customer_id, snapshot_date),
-- so gold.dim_customer_scd2 is built from clean data. Idempotent + atomic.
BEGIN;

DELETE FROM silver.customer_snapshots WHERE snapshot_date = DATE '{{ ds }}';

INSERT INTO silver.customer_snapshots
SELECT
    customer_id,
    tier,
    snapshot_date
FROM bronze.customer_snapshots
WHERE
    snapshot_date = DATE '{{ ds }}'
    AND tier IS NOT NULL
    AND tier IN ('bronze', 'silver', 'gold', 'platinum')
QUALIFY row_number() OVER (PARTITION BY customer_id, snapshot_date ORDER BY tier) = 1;

COMMIT;
