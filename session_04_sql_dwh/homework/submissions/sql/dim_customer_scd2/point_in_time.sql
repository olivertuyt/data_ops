-- Point-in-time query: trả trạng thái dimension "as of" {{ ds }}
SELECT
    customer_id,
    name,
    tier,
    version
FROM gold.dim_customer_scd2
WHERE DATE '{{ ds }}' BETWEEN effective_date AND expiration_date
ORDER BY customer_id;
