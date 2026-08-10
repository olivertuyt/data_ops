SELECT coalesce(sum(total_revenue), 0) > 0
FROM delta.gold.fact_daily_revenue
WHERE order_date = DATE '{{ data_interval_end | ds }}'
