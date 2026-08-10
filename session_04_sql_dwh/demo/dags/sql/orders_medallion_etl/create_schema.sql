CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS bronze.orders_raw (
    order_id BIGINT,
    customer_id INTEGER,
    customer_name VARCHAR,
    amount DOUBLE,
    order_date DATE
);

CREATE TABLE IF NOT EXISTS silver.orders (
    order_id BIGINT PRIMARY KEY,
    customer_id INTEGER,
    customer_name VARCHAR,
    amount DOUBLE,
    order_date DATE
);

CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_id INTEGER PRIMARY KEY,
    customer_name VARCHAR,
    first_seen_date DATE,
    last_updated_date DATE
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    order_id BIGINT PRIMARY KEY,
    customer_id INTEGER,
    amount DOUBLE,
    order_date DATE
);
