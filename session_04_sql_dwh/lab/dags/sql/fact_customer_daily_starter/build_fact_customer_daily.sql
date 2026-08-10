BEGIN;
DELETE FROM gold.fact_customer_daily WHERE order_date = DATE '{{ ds }}';
INSERT INTO gold.fact_customer_daily
SELECT
    customer_id,
    order_date,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM silver.orders
WHERE order_date = DATE '{{ ds }}'
GROUP BY customer_id, order_date;
COMMIT;
