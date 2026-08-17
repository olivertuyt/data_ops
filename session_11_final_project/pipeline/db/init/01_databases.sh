#!/bin/sh
set -eu

for database_name in "$AIRFLOW_DB_NAME" "$POLARIS_DB_NAME"; do
  if ! psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres \
      -tAc "SELECT 1 FROM pg_database WHERE datname = '$database_name'" | grep -q 1; then
    createdb --username "$POSTGRES_USER" --owner "$POSTGRES_USER" "$database_name"
  fi
done

