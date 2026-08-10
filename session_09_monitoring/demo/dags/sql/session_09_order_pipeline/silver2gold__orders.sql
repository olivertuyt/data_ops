DELETE FROM gold.fact_orders WHERE order_date = DATE '{{ ds }}';

INSERT INTO gold.fact_orders
SELECT order_id, customer_id, customer_name, amount, order_date
FROM silver.orders
WHERE order_date = DATE '{{ ds }}';
