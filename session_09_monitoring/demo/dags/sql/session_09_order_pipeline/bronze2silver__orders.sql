DELETE FROM silver.orders WHERE order_date = DATE '{{ ds }}';

INSERT INTO silver.orders
SELECT order_id, customer_id, customer_name, amount, order_date
FROM bronze.orders_raw
WHERE order_date = DATE '{{ ds }}'
  AND order_id IS NOT NULL
  AND customer_id IS NOT NULL
  AND customer_name IS NOT NULL
  AND amount > 0;
