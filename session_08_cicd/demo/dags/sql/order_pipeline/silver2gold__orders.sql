-- noqa: disable=all
MERGE INTO gold.fact_sales AS target
USING (
    SELECT order_id, customer_id, amount, order_date
    FROM (
        SELECT
            order_id,
            customer_id,
            amount,
            order_date,
            ROW_NUMBER() OVER (PARTITION BY order_id, order_date ORDER BY order_id) AS rn
        FROM silver.orders
        WHERE order_date = DATE '{{ ds }}'
    )
    WHERE rn = 1
) AS source
ON target.order_id = source.order_id
    AND target.order_date = source.order_date
WHEN MATCHED THEN
    UPDATE SET
        customer_id = source.customer_id,
        amount = source.amount
WHEN NOT MATCHED THEN
    INSERT (order_id, customer_id, amount, order_date)
    VALUES (source.order_id, source.customer_id, source.amount, source.order_date);
