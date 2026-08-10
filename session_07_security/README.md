# Session 07 — Security & Sensitive Data

Stack: Airflow (Celery) + Spark + MinIO (Delta Lake) + Hive Metastore + Trino + HashiCorp Vault + Postgres.

## Quick start

```bash
cd session_07_security
mkdir -p logs/audit
chmod 777 logs/audit   # Spark container writes here; Docker Desktop on macOS needs world-writable
docker compose up -d
```

Wait for all services to go healthy (`docker compose ps`), then initialize Delta schemas once:

```bash
docker exec s07-spark-master /opt/spark/bin/spark-submit /jobs/init_schema.py
```

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| Airflow UI | http://localhost:8888 | airflow / airflow |
| Vault UI | http://localhost:8200 | token: `root-demo-token` |
| Trino (HTTP, healthcheck only) | http://localhost:8092 | — |
| Trino (HTTPS, queries) | https://localhost:8443 | admin / AdminDemo_2026! |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Spark UI | http://localhost:8080 | — |

## Structure

```
session_07_security/
├── demo/
│   ├── dags/                  # DAG: session_07_customer_pii_pipeline
│   │   └── sql/               # SQL files for DAG operators
│   ├── jobs/                  # Spark jobs: ingest_bronze, transform_silver, transform_gold, init_schema
│   │   └── security_utils.py  # shared Vault + audit helpers
│   └── README.md              # full demo walkthrough (7-beat script)
├── labs/                      # 3 hands-on labs
│   ├── dags/                  # Lab 1 DAG skeleton
│   ├── jobs/                  # Lab 2 & 3 Spark job skeletons
│   └── sql/                   # Lab 3 SQL scripts
├── conf/
│   ├── spark-defaults.conf    # Spark JAR paths (mounted into spark-master + spark-worker)
│   └── trino/                 # Trino config, TLS certs, access-control rules.json
├── vault-init/
│   └── seed_secrets.sh        # seeds all demo secrets into Vault on first boot
├── scripts/                   # generate_customers.py, query_trino.py
├── sql/                       # init_source.sql — creates raw.customers in postgres-source
└── logs/audit/                # pii_access.log — written by log_pii_access() in every Spark job
```

## Common Mistakes & Best Practices

Five patterns that consistently show up in DataOps security reviews — and what to do instead.

**1. Hardcoded credentials in DAG code**
The most common finding. Credentials in Python files get committed to git, logged by Airflow's task import system, and live in git history even after "deletion." Use a secrets backend (Vault, AWS Secrets Manager) and let Airflow's `BaseHook.get_connection()` resolve them at runtime. The rule: if `grep -rni password dags/` finds a real value, it's a finding.

**2. Superuser service accounts**
ETL jobs that connect as `postgres` (or any SUPERUSER/admin) can DROP tables, ALTER schemas, and access any database on the server. Grant only the minimum needed: `SELECT` on the specific tables the job reads. A read-only account cannot accidentally truncate a production table even if the job has a bug.

**3. DEBUG log level + `.show()` in Spark jobs**
`spark.setLogLevel("DEBUG")` and `df.show()` dump row data — including PII — to stdout, container logs, and Airflow task logs. Any engineer with log access now has access to production PII. Always set `WARN` in production jobs, use `.count()` for reconcile checks, and never call `.show()` outside a local notebook.

**4. No audit trail on PII access**
When a data breach occurs, the first question is "who accessed what, when, and how much?" Without an audit log, the answer is "we don't know." Log every PII read and write: job name, table, columns touched, row count, timestamp — but never the PII values themselves. A JSON-per-line file works; a dedicated audit table in a write-only schema is better.

**5. Analyst access without column masking or row filtering**
Giving analysts `SELECT` on a gold table sounds safe — until you realize the table still contains `cccd`, `full_name`, and raw `account_balance`. Role-based column masking (Trino `rules.json`) and row-level filters (`"filter": "region = 'UK'"`) enforce the access policy at the query engine, independent of the application. Defense in depth: even if an analyst finds a direct S3 path, the MinIO ACL blocks them.

## 🚑 Common errors & fixes

| Symptom | Cause | Fix |
|---|---|---|
| `WARNING: Could not write audit log file: [Errno 13] Permission denied: '/var/log/pii_audit'` | Docker Desktop on macOS does not map container root to host user — `logs/audit` (755) blocks writes | `chmod 777 logs/audit` on the host, then retry the Airflow task |
| DBeaver/JDBC → Trino 8443: `toDerInputStream rejects tag type 45` or `SSL peer shut down` | JKS truststore created with wrong store type (`keytool` defaults to PKCS12 on Java 9+) | Re-import with `-storetype JKS`: `keytool -importcert -alias trino -file conf/trino/certs/trino.pem -keystore trino-truststore.jks -storetype JKS -storepass changeit -noprompt`. Set `SSLTrustStoreType=JKS` in the JDBC URL. See `demo/README.md` & DBeaver setup. |
| `Access Denied: Cannot access catalog system` | `rules.json` missing a catalog-level entry — table-level rules do not imply catalog access | Add `{ "user": "analyst_uk\|analyst_us", "catalog": "system", "allow": "read-only" }` before the table rules, then `docker compose restart trino` |
| Spark job: `ClassNotFoundException: delta.DeltaSparkSessionExtension` | Session 05 JAR volume not mounted, or `spark-defaults.conf` not loaded | Check `ls ../session_05_pyspark_lakehouse/jars` and confirm both `spark-master` and `spark-worker` mount it; verify `spark-defaults.conf` is bind-mounted to `/opt/spark/conf/spark-defaults.conf` |
| Airflow task: `hvac.exceptions.InvalidPath` or `KeyError` on Vault read | `vault-init` exited before seeding finished, or secret path mismatch | Run `docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv list secret/dataops` and compare against the path used in the job |
| SparkSubmitOperator: `Connection refused` to spark-master | `AIRFLOW_CONN_SPARK_DEFAULT` has wrong scheme or hostname | Value must be `{"conn_type": "spark", "host": "spark://spark-master", "port": 7077}`. Verify: `docker compose exec airflow-worker env \| grep SPARK` |

## Shutdown

```bash
docker compose down -v
```
