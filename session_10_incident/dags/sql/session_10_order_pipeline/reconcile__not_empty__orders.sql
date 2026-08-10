SELECT count(*) > 0
FROM delta.silver.orders
WHERE order_date = DATE '{{ data_interval_end | ds }}'
