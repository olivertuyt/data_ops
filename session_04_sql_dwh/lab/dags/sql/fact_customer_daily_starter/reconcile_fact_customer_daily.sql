SELECT
    ROUND((SELECT SUM(amount) FROM silver.orders WHERE order_date = DATE '{{ ds }}'), 2)
    = ROUND((SELECT COALESCE(SUM(total_amount), 0) FROM gold.fact_customer_daily WHERE order_date = DATE '{{ ds }}'), 2) AS sum_match,
    (SELECT COUNT(*) FROM silver.orders WHERE order_date = DATE '{{ ds }}')
    = (SELECT COALESCE(SUM(order_count), 0) FROM gold.fact_customer_daily WHERE order_date = DATE '{{ ds }}') AS count_match;
