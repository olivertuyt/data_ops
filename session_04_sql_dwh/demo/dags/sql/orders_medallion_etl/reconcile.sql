-- SQLCheckOperator fails the task if any column is falsy, so every check is a
-- boolean that must be TRUE for the run to pass.
WITH today AS (
    SELECT COUNT(*) AS n
    FROM gold.fact_sales
    WHERE order_date = DATE '{{ ds }}'
),

prev AS (
    SELECT COUNT(*) AS n
    FROM gold.fact_sales
    WHERE order_date = DATE '{{ ds }}' - INTERVAL 1 DAY
)

SELECT
    (SELECT COUNT(*) FROM silver.orders WHERE order_date = DATE '{{ ds }}')
    =
    (SELECT COUNT(*) FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}') AS count_match,

    ROUND((SELECT COALESCE(SUM(amount), 0) FROM silver.orders WHERE order_date = DATE '{{ ds }}'), 2)
    =
    ROUND((SELECT COALESCE(SUM(amount), 0) FROM gold.fact_sales WHERE order_date = DATE '{{ ds }}'), 2) AS sum_match,

    (
        SELECT COUNT(*)
        FROM gold.fact_sales AS f
        WHERE
            f.order_date = DATE '{{ ds }}'
            AND (
                f.amount < 0
                OR f.customer_id IS NULL
                -- NOT EXISTS, not NOT IN: a single NULL in the dim key makes
                -- `NOT IN` never true, silently passing every orphan row.
                OR NOT EXISTS (
                    SELECT 1
                    FROM gold.dim_customer AS d
                    WHERE d.customer_id = f.customer_id
                )
            )
    ) = 0 AS rules_ok,

    -- Day-over-day guard: the same-day count/sum checks prove internal
    -- consistency but not plausibility — a half-loaded or doubled day still
    -- reconciles. Flag when today's rows swing >50% from the prior day. The
    -- first day has no prior (prev = 0) and is not flagged.
    (
        (SELECT n FROM prev) = 0
        OR ABS((SELECT n FROM today) - (SELECT n FROM prev))
        <= 0.5 * (SELECT n FROM prev)
    ) AS dod_ok;
