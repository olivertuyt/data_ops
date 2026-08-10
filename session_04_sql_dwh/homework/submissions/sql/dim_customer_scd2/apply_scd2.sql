BEGIN;

-- CLOSE: đóng version hiện tại của customer có attribute đổi so với snapshot
UPDATE gold.dim_customer_scd2 AS d
SET
    expiration_date = DATE '{{ ds }}' - INTERVAL 1 DAY,
    is_current = false
WHERE
    d.is_current
    AND EXISTS (
        SELECT 1 FROM stage.customer_snapshot AS s
        WHERE
            s.snapshot_date = DATE '{{ ds }}'
            AND s.customer_id = d.customer_id
            AND (s.name, s.tier) <> (d.name, d.tier)
    );

-- OPEN: mở version mới cho customer thay đổi + customer mới
INSERT INTO gold.dim_customer_scd2
SELECT
    s.customer_id,
    s.name,
    s.tier,
    DATE '{{ ds }}' AS effective_date,
    DATE '9999-12-31' AS expiration_date,
    true AS is_current,
    COALESCE(
        (
            SELECT MAX(version) FROM gold.dim_customer_scd2 AS d
            WHERE d.customer_id = s.customer_id
        ),
        0
    ) + 1 AS version
FROM stage.customer_snapshot AS s
WHERE
    s.snapshot_date = DATE '{{ ds }}'
    AND NOT EXISTS (
        SELECT 1 FROM gold.dim_customer_scd2 AS d
        WHERE
            d.customer_id = s.customer_id
            AND d.is_current
            AND d.name = s.name
            AND d.tier = s.tier
    );

COMMIT;
