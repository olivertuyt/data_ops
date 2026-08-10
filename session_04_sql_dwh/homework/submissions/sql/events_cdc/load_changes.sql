-- HW1: seed one day's change batch into bronze.events_raw (deterministic from {{ ds }})
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS bronze.events_raw (
    event_id BIGINT,
    user_id INTEGER,
    amount DOUBLE,
    op VARCHAR,
    updated_at TIMESTAMP,
    event_date DATE
);

CREATE TABLE IF NOT EXISTS silver.events (
    event_id BIGINT PRIMARY KEY,
    user_id INTEGER,
    amount DOUBLE,
    updated_at TIMESTAMP
);

BEGIN;
DELETE FROM bronze.events_raw WHERE event_date = DATE '{{ ds }}';

INSERT INTO bronze.events_raw
-- 100k inserts
SELECT
    i,
    1000 +AFw-x2b+AFw-x20(i % 5000), ROUND(10 +AFw-x2b+AFw-x20(i % 500) * 1.5, 2),
        AS i,
    TIMESTAMP '{{ ds }} 09:00:00',
    DATE '{{ ds }}'
FROM range(1, 100001)
UNION ALL
-- 20k late UPDATEs (amount +100, a later timestamp) to keys 1..20000
SELECT
    i,
    1000 +AFw-x2b+AFw-x20(i % 5000), ROUND(10 +AFw-x2b+AFw-x20(i % 500) * 1.5 +AFw-x2b+AFw-x20100, 2),
        AS u,
    TIMESTAMP '{{ ds }} 15:00:00',
    DATE '{{ ds }}'
FROM range(1, 20001)
UNION ALL
-- 10k DELETEs (tombstones) of keys 90001..100000
SELECT
    i,
    1000 +AFw-x2b+AFw-x20(i % 5000), NULL,
        AS d,
    TIMESTAMP '{{ ds }} 18:00:00',
    DATE '{{ ds }}'
FROM range(90001, 100001)
UNION ALL
-- 5k duplicate insert retries of keys 1..5000
SELECT
    i,
    1000 +AFw-x2b+AFw-x20(i % 5000), ROUND(10 +AFw-x2b+AFw-x20(i % 500) * 1.5, 2),
        AS i,
    TIMESTAMP '{{ ds }} 09:00:00',
    DATE '{{ ds }}'
FROM range(1, 5001);

COMMIT;
