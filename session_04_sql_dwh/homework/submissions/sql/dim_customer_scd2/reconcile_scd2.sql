SELECT
    (SELECT COUNT(*) FROM (
        SELECT customer_id FROM gold.dim_customer_scd2
        WHERE is_current
        GROUP BY customer_id
        HAVING COUNT(*) <> 1
    )) = 0 AS one_current_per_key,
    (
        SELECT COUNT(*)
        FROM gold.dim_customer_scd2 AS a
        INNER JOIN gold.dim_customer_scd2 AS b
            ON
                a.customer_id = b.customer_id
                AND a.version < b.version
                AND a.expiration_date >= b.effective_date
    ) = 0 AS no_overlap;
