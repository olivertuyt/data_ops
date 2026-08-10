-- Lab 1 target table (given). Grain: one row per (customer_id, order_date).
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS gold.fact_customer_daily (
    customer_id INTEGER,
    order_date DATE,
    order_count INTEGER,
    total_amount DOUBLE,
    PRIMARY KEY (customer_id, order_date)
);
