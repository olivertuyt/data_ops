BEGIN;

DELETE FROM silver.orders WHERE order_date = DATE '{{ ds }}';

INSERT INTO silver.orders
SELECT
    order_id,
    customer_id,
    customer_name,  -- TODO Bug2: customer_name is not in silver schema — remove this line
    amount,
    order_date
FROM bronze.orders_raw
WHERE order_date = DATE '{{ ds }}';
-- TODO Bug3: NULL customer_id rows reach silver — add: AND customer_id IS NOT NULL
-- TODO Bug4: negative amounts reach silver — add: AND amount > 0

COMMIT;
