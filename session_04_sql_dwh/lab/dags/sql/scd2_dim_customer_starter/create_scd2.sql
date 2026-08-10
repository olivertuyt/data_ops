-- Lab 2 tables (given).
-- customer_key is a SURROGATE key: one per VERSION (Kimball), filled by the sequence.
-- customer_id is the business key: it repeats across a customer's versions.
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE SEQUENCE IF NOT EXISTS gold.seq_customer_key START 1;

-- Bronze: raw daily snapshot as received (with the noise a real feed carries).
CREATE TABLE IF NOT EXISTS bronze.customer_snapshots (
    customer_id INTEGER,
    tier VARCHAR,
    snapshot_date DATE
);

-- Silver: validated snapshot — nulls dropped, tier domain enforced, deduped.
-- The SCD2 dimension is built from THIS, never straight from bronze (Medallion:
-- bronze -> silver -> gold, same as the demo's dim_customer).
CREATE TABLE IF NOT EXISTS silver.customer_snapshots (
    customer_id INTEGER,
    tier VARCHAR,
    snapshot_date DATE
);

CREATE TABLE IF NOT EXISTS gold.dim_customer_scd2 (
    customer_key BIGINT DEFAULT nextval('gold.seq_customer_key') PRIMARY KEY,
    customer_id INTEGER,
    tier VARCHAR,
    effective_from DATE,
    effective_to DATE,      -- NULL while the version is current
    is_current BOOLEAN
);
