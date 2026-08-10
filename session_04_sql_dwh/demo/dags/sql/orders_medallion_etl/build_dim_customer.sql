MERGE INTO gold.dim_customer AS t
USING (
    SELECT DISTINCT
        customer_id,
        customer_name
    FROM silver.orders
    WHERE order_date = DATE '{{ ds }}'
) AS s ON t.customer_id = s.customer_id
WHEN MATCHED THEN
    UPDATE SET
        customer_name = s.customer_name,
        last_updated_date = DATE '{{ ds }}'
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    customer_name,
    first_seen_date,
    last_updated_date
) VALUES (
    s.customer_id,
    s.customer_name,
    DATE '{{ ds }}',
    DATE '{{ ds }}'
);
