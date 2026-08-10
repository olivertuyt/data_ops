CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.write_patterns (
    order_id INTEGER, amount DOUBLE, order_date DATE
);
BEGIN;

DELETE
FROM demo.write_patterns
WHERE order_date = '{{ ds }}';

INSERT INTO demo.write_patterns VALUES
(1, 10.0, '{{ ds }}'),
(2, 20.0, '{{ ds }}'),
(3, 30.0, '{{ ds }}');
COMMIT;
