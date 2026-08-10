BEGIN;

-- Step A — CLOSE: đóng version hiện tại có tier khác snapshot hôm nay
UPDATE gold.dim_customer_scd2 AS d
SET effective_to = DATE '{{ ds }}', is_current = false
WHERE
    d.is_current
    AND EXISTS (
        SELECT 1 FROM silver.customer_snapshots AS s
        WHERE
            s.snapshot_date = DATE '{{ ds }}'
            AND s.customer_id = d.customer_id
            AND s.tier <> d.tier
    );

-- Step B — OPEN: mở version mới cho customer chưa có current-row cùng tier
--                (bỏ customer_key để sequence tự điền surrogate key)
INSERT INTO gold.dim_customer_scd2
(customer_id, tier, effective_from, effective_to, is_current)
SELECT
    s.customer_id,
    s.tier,
    DATE '{{ ds }}',
    null,
    true
FROM silver.customer_snapshots AS s
WHERE
    s.snapshot_date = DATE '{{ ds }}'
    AND NOT EXISTS (
        SELECT 1 FROM gold.dim_customer_scd2 AS d
        WHERE
            d.customer_id = s.customer_id
            AND d.tier = s.tier
            AND d.is_current
    );

COMMIT;
