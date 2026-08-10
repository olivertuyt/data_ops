DELETE FROM bronze.orders_raw WHERE order_date = DATE '{{ ds }}';

INSERT INTO bronze.orders_raw
SELECT order_id, customer_id, customer_name, amount, order_date
FROM read_parquet('{{ var.value.get("orders_dataset", "/opt/airflow/dags/session_09/data/orders_clean.parquet") }}')
WHERE order_date = DATE '{{ ds }}';
