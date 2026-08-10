SELECT (
    SELECT COUNT(*) FROM silver.orders WHERE order_date = DATE '{{ ds }}'
) = (
    SELECT COUNT(*) FROM gold.fact_orders WHERE order_date = DATE '{{ ds }}'
)
