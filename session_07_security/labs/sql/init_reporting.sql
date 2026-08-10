-- Bootstrap for the reporting service account.

-- TODO (Finding 1 & 2): SUPERUSER and ALL PRIVILEGES violate least privilege.
-- This account only needs to SELECT from raw.customers.
-- Replace with a read-only account following the principle of least privilege.
-- Reference: demo/sql/init_source.sql — see how etl_reader is created.

-- run inside postgres-source
CREATE USER lab_reporter WITH SUPERUSER PASSWORD 'LabReporter_2026!';
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA raw TO lab_reporter;
