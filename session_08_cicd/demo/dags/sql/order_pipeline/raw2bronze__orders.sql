BEGIN;

DELETE FROM bronze.orders_raw WHERE order_date = DATE '{{ ds }}';

INSERT INTO bronze.orders_raw
WITH gen AS (SELECT UNNEST(RANGE(400000)) AS i)
SELECT
    CAST(STRFTIME(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 1000000 + i AS order_id,
    101 + (i % 5000) AS customer_id,
    'Customer ' || CAST(101 + (i % 5000) AS VARCHAR) AS customer_name,
    ROUND(20.0 + (i % 500) * 7.5, 2) AS amount,
    DATE '{{ ds }}' AS order_date
FROM gen
UNION ALL
SELECT
    CAST(STRFTIME(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 1000000 + 999998,
    NULL, NULL, 42.0, DATE '{{ ds }}'
UNION ALL
SELECT
    CAST(STRFTIME(DATE '{{ ds }}', '%Y%m%d') AS BIGINT) * 1000000 + 999999,
    102, 'Customer 102', -15.0, DATE '{{ ds }}';

COMMIT;
