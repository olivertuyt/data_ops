CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id      VARCHAR(36)     PRIMARY KEY,
    full_name        VARCHAR(255)    NOT NULL,
    phone            VARCHAR(20)     NOT NULL,
    email            VARCHAR(255)    NOT NULL,
    cccd             VARCHAR(12)     NOT NULL,
    date_of_birth    DATE            NOT NULL,
    address          TEXT            NOT NULL,
    segment          VARCHAR(50)     NOT NULL,
    region           VARCHAR(50)     NOT NULL,
    registered_at    TIMESTAMP       NOT NULL DEFAULT now(),
    account_balance  DECIMAL(18, 2)  NOT NULL DEFAULT 0,
    credit_score     INT             NOT NULL DEFAULT 650
);

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_reader') THEN
        CREATE ROLE etl_reader WITH LOGIN PASSWORD 'EtlReader_DemoOnly_2026!';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA raw TO etl_reader;
GRANT SELECT ON raw.customers TO etl_reader;
