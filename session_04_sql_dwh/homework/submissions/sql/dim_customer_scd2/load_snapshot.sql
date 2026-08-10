-- HW2: seed daily customer snapshot into stage.customer_snapshot
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS stage.customer_snapshot (
    customer_id   INTEGER,
    name          VARCHAR,
    tier          VARCHAR,
    snapshot_date DATE
);

CREATE TABLE IF NOT EXISTS gold.dim_customer_scd2 (
    customer_id     INTEGER,
    name            VARCHAR,
    tier            VARCHAR,
    effective_date  DATE,
    expiration_date DATE,
    is_current      BOOLEAN,
    version         INTEGER
);

BEGIN;
DELETE FROM stage.customer_snapshot WHERE snapshot_date = DATE '{{ ds }}';

INSERT INTO stage.customer_snapshot
SELECT
    i AS customer_id,
    'Customer ' || i AS name,
    CASE WHEN i <= {{ params.silver_upto }} THEN 'SILVER' ELSE 'BRONZE' END AS tier,
    DATE '{{ ds }}' AS snapshot_date
FROM range(1, 1001) AS t(i);

COMMIT;
