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
    order_id BIGINT,
    customer_id INTEGER,
    customer_name VARCHAR,
    amount DOUBLE,
    order_date DATE,
    PRIMARY KEY (order_id, order_date)
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    order_id BIGINT,
    customer_id INTEGER,
    amount DOUBLE,
    order_date DATE,
    PRIMARY KEY (order_id, order_date)
);
