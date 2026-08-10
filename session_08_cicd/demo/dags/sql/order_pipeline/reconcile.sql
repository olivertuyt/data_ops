SELECT
    (SELECT COUNT(*) FROM silver.orders WHERE order_date = DATE '{{ ds }}')
    =
    (SELECT COUNT(*) FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}') AS count_match,

    ROUND((SELECT COALESCE(SUM(amount), 0) FROM silver.orders WHERE order_date = DATE '{{ ds }}'), 2)
    =
    ROUND((SELECT COALESCE(SUM(amount), 0) FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}'), 2) AS sum_match,

    (
        SELECT COUNT(*)
        FROM gold.fact_sales
        WHERE
            order_date = DATE '{{ ds }}'
            AND (amount <= 0 OR customer_id IS NULL)
    ) = 0 AS rules_ok;
