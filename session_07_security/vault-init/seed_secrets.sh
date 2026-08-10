#!/bin/sh
# Runs once against the dev-mode Vault server to seed the secrets this demo reads
# at runtime. VAULT_ADDR / VAULT_TOKEN come from this container's environment
# (set in docker-compose.yml) -- nothing here is baked into DAG or job code.
set -eu

echo "Waiting for Vault to be ready..."
until vault status >/dev/null 2>&1; do
  sleep 1
done

# KV v2 is enabled by default at "secret/" in -dev mode.

# 1. Raw credentials for whatever needs to reach postgres-source/minio directly
#    (the Spark job scripts, via hvac).
vault kv put secret/dataops/postgres_source \
  username="etl_reader" \
  password="EtlReader_DemoOnly_2026!" \
  host="postgres-source" \
  port="5432" \
  database="dataops_source"

vault kv put secret/dataops/minio \
  access_key="etl-svc" \
  secret_key="etl-svc-2026" \
  endpoint="http://minio:9000"

vault kv put secret/dataops/trino \
  admin_user="admin" \
  admin_password="AdminDemo_2026!" \
  analyst_user="analyst" \
  analyst_password="AnalystDemo_2026!"

# 2. Airflow-Connection-shaped secret, consumed automatically by Airflow's native
#    Vault secrets backend (AIRFLOW__SECRETS__BACKEND=...VaultBackend) so that
#    PostgresHook(postgres_conn_id="postgres_source_conn") just works with zero
#    password in DAG code.
vault kv put secret/airflow/connections/postgres_source_conn \
  conn_type="postgres" \
  host="postgres-source" \
  login="etl_reader" \
  password="EtlReader_DemoOnly_2026!" \
  port="5432" \
  schema="dataops_source"

# 3. Lab 1 secret — postgres_source_lab starts as superuser (Finding 3);
#    after creating lab_reader, students update this secret and retest.
vault kv put secret/dataops/postgres_source_lab \
  username="postgres" \
  password="postgres" \
  host="postgres-source" \
  port="5432" \
  database="dataops_source"

echo "Vault secrets seeded."
