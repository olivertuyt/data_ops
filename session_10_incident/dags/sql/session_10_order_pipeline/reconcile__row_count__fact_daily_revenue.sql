SELECT
    (
        SELECT count(*) FROM delta.silver.orders
        WHERE order_date = DATE '{{ data_interval_end | ds }}'
    )
    =
    (
        SELECT coalesce(sum(order_count), 0) FROM delta.gold.fact_daily_revenue
        WHERE order_date = DATE '{{ data_interval_end | ds }}'
    )
