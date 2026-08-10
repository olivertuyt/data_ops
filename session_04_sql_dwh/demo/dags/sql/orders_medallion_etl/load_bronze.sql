BEGIN;

DELETE FROM bronze.orders_raw WHERE order_date = DATE '{{ ds }}';

-- 400,000 orders/day across 5,000 customers. order_id = date * 1_000_000 + i keeps
-- the offset wider than the row count so ids never collide within or across days.
INSERT INTO bronze.orders_raw
WITH gen AS (
    SELECT UNNEST(RANGE(400000)) AS i
)

SELECT
    CAST(STRFTIME(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 1000000 + i AS order_id,
    101 + (i % 5000) AS customer_id,
    'Customer ' || CAST(101 + (i % 5000) AS VARCHAR) AS customer_name,
    ROUND(20.0 + (i % 500) * 7.5, 2) AS amount,
    DATE '{{ ds }}' AS order_date
FROM gen
UNION ALL
SELECT
    CAST(STRFTIME(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 1000000 + 999998 AS order_id,
    NULL AS customer_id,
    NULL AS customer_name,
    42.0 AS amount,
    DATE '{{ ds }}' AS order_date
UNION ALL
SELECT
    CAST(STRFTIME(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 1000000 + 999999 AS order_id,
    102 AS customer_id,
    'Customer 102' AS customer_name,
    -15.0 AS amount,
    DATE '{{ ds }}' AS order_date;

COMMIT;
