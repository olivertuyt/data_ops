BEGIN;

DELETE FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}';

INSERT INTO gold.fact_sales
SELECT
    order_id,
    customer_id,
    amount,
    order_date
FROM silver.orders
WHERE order_date = DATE '{{ ds }}';

COMMIT;
