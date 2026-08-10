# Session 07 — Labs

Three hands-on labs on the demo stack (`session_07_security/docker-compose.yml` must be running and `session_07_customer_pii_pipeline` must have completed at least once so `silver`/`gold` contain data).

| Lab | Topic | File(s) | Findings |
|---|---|---|---|
| Lab 1 | Secrets Management | `dags/customer_export_dag.py` | 3 |
| Lab 2 | PII — Data Minimization & Masking | `jobs/build_marketing_customers.py`, `trino_rules_patch.json` | 3 |
| Lab 3 | Least Privilege & Log Hygiene | `jobs/sync_customers.py`, `sql/init_reporting.sql` | 3 |

## Rules

1. Code in each lab **runs correctly** — the issues are **security**, not functionality.
2. For each finding, submit all four parts: **(a)** exact file + line, **(b)** specific risk explanation (not just "insecure"), **(c)** a working fix, **(d)** evidence of retesting after the fix.
3. Do not modify the demo infrastructure (docker-compose, Vault seed, base Trino config) unless the lab explicitly requires it.

## Running lab code

The `labs/` directory is already mounted into both Airflow and Spark — **no file copying needed**:
- Airflow: `labs/dags/` → `/opt/airflow/dags/labs/dags/` — DAGs appear automatically in the UI.
- Spark: `/labs` → run directly; `PYTHONPATH=/jobs` allows lab jobs to `import security_utils`.

Edit lab files directly on the host — changes reflect inside containers immediately.

```bash
# Lab 2
docker exec s07-spark-master /opt/spark/bin/spark-submit /labs/jobs/build_marketing_customers.py --ds <YYYY-MM-DD>

# Lab 3
docker exec s07-spark-master /opt/spark/bin/spark-submit /labs/jobs/sync_customers.py --ds <YYYY-MM-DD>

# Lab 3 SQL
docker exec -i session_07_security-postgres-source-1 \
  psql -U postgres -d dataops_source < labs/sql/init_reporting.sql

# Query Trino
docker compose exec demo-cli python /scripts/query_trino.py --user <user> --password '<password>' --sql "..."
```

## Lab 1 — Secrets Management

**File:** `dags/customer_export_dag.py`

The BI team needs a daily CSV export of customer counts by segment and region. A team member wrote this DAG — it works correctly and delivers the file.

This week the security team scanned the repository and **flagged this DAG with 3 findings**. Your task: identify all three, explain the risk of each, and fix the DAG so it still runs successfully.

**Setup (run once before triggering the DAG):**

```bash
# Create the lab_reader account in postgres-source
docker exec -i $(docker compose ps -q postgres-source) \
  psql -U postgres -d dataops_source -c \
  "CREATE ROLE lab_reader WITH LOGIN PASSWORD 'LabReader_2026!'; GRANT USAGE ON SCHEMA raw TO lab_reader; GRANT SELECT ON raw.customers TO lab_reader;"
```

**Available resources:**
- Vault is running at `http://vault:8200` — inspect: `docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv list secret/dataops`
- Airflow is configured with the Vault secrets backend (see `docker-compose.yml`, `AIRFLOW__SECRETS__*`)
- Airflow containers have `VAULT_ADDR` / `VAULT_TOKEN` env vars and `hvac`, `apache-airflow-providers-postgres` pre-installed
- Reference: how the main DAG (`demo/dags/customer_pii_pipeline.py`) connects to each system

**Acceptance criteria:**
1. `grep -niE "password|secret|access.?key" labs/dags/customer_export_dag.py` returns no credential **values** (variable names and Vault paths are fine).
2. DAG runs green and the CSV still appears in MinIO.
3. The Postgres connection does not use an account with more privileges than this job needs.
4. Explain: if this file had already been committed to Git, is fixing the code enough? What else needs to happen?

**Run & verify:**
```bash
# 1. Trigger DAG
curl -s -X POST http://localhost:8888/api/v1/dags/session_07_lab1_customer_export/dagRuns \
  -H 'Content-Type: application/json' -u airflow:airflow -d '{"conf":{}}' | python3 -m json.tool

# 2a. AC1 — no hardcoded credential values
grep -niE "password|secret|access.?key" labs/dags/customer_export_dag.py

# 2b. AC2 — CSV landed in MinIO (run after DAG is green)
docker compose exec demo-cli python3 -c "
from minio import Minio
c = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
for o in c.list_objects('gold', prefix='exports/'): print(o.object_name, o.size, 'bytes')
"

# 2c. AC3 — lab_reader is denied INSERT
docker exec -i $(docker compose ps -q postgres-source) \
  psql -U lab_reader -d dataops_source \
  -c "INSERT INTO raw.customers (customer_id,full_name,phone,email,cccd,date_of_birth,address,segment,region,account_balance,credit_score) VALUES ('x','x','x','x','x','2000-01-01','x','x','x',0,0)"
# → expected: ERROR:  permission denied for table customers
```

**How to fix:**

*F1 + F2 — pull all credentials from Vault directly via `hvac` (same pattern as demo Spark jobs):*
```python
import os
import hvac

vault = hvac.Client(url=os.environ["VAULT_ADDR"], token=os.environ["VAULT_TOKEN"])

# F1: DB credentials from Vault
pg_secret = vault.secrets.kv.v2.read_secret_version(
    path="dataops/postgres_source_lab", raise_on_deleted_version=True
)["data"]["data"]

conn = psycopg2.connect(
    host=pg_secret["host"], port=int(pg_secret["port"]),
    dbname=pg_secret["database"], user=pg_secret["username"], password=pg_secret["password"],
)

# F2: MinIO credentials from Vault
minio_secret = vault.secrets.kv.v2.read_secret_version(
    path="dataops/minio", raise_on_deleted_version=True
)["data"]["data"]

client = Minio(minio_secret["endpoint"].replace("http://", ""),
               access_key=minio_secret["access_key"],
               secret_key=minio_secret["secret_key"], secure=False)
```
Delete all hardcoded constants (`DB_*`, `MINIO_*`) at the top of the file.

*F3 — replace superuser account with least-privilege role:*
```sql
-- run inside postgres-source
CREATE USER lab_reader WITH PASSWORD 'LabReader_2026!';
GRANT CONNECT ON DATABASE dataops_source TO lab_reader;
GRANT USAGE ON SCHEMA raw TO lab_reader;
GRANT SELECT ON raw.customers TO lab_reader;
```
Then update the Vault secret — no code change needed:
```bash
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault \
  vault kv patch secret/dataops/postgres_source_lab \
  username='lab_reader' password='LabReader_2026!'
```

---

## Lab 2 — PII: Data Minimization & Masking

**Files:** `jobs/build_marketing_customers.py`, `trino_rules_patch.json`

> **Note on `trino_rules_patch.json`:** The patch file intentionally grants `marketing` broad access (`"schema": ".*", "table": ".*"`) — this is the broken starting point for F3. Your task is to replace those table entries with a rule that restricts `marketing` to exactly `gold.marketing_customers`. Do **not** add the patch file as-is; read it, understand what's wrong, then write the correct entry.

The Marketing team needs data to analyze customer cohorts by segment, region, registration date, and financial tier, along with the ability to verify contact reachability for SMS/email campaigns.

The DPO (Data Protection Officer) reviewed both files and **rejected them with 3 findings**, noting: *"Violates data minimization and access control principles. Revise."*

**Confirmed business requirements (validated by the DPO):**
- Cohort analysis: `segment`, `region`, registration timestamp
- Financial distribution: **tier** only (not raw balance or score)
- SMS/email campaigns: need to know whether a customer is **contactable** — partially masked phone/email is sufficient, full values are not required
- No requirement exists for full customer identity

**Setup:**
```bash
# add the marketing user to Trino (provided — not a finding):
echo 'marketing:$2y$10$G.Xe8rLs5ALq7IBf5dgsZurmOnIozrb5RTbT5rxKjcD1p0J46mDG6' >> conf/trino/password.db
# password: MarketingDemo_2026!
```

When merging into `conf/trino/rules.json`, place the `marketing` catalog entries **before** the catch-all `{ "catalog": ".*", "allow": "none" }` line — Trino evaluates rules top-to-bottom and the catch-all must always be last. After merging:

```bash
docker compose restart trino
```

**Acceptance criteria:**
1. `gold.marketing_customers` contains no direct PII column that does not serve one of the confirmed business requirements above.
2. User `marketing` can query `gold.marketing_customers` with phone/email partially masked; **all other tables** → Access Denied.
3. Users `admin` and `analyst` are unaffected.
4. Demonstrate with actual query output for all three users.

**Run & verify:**
```bash
# 1. Build marketing_customers table
docker exec s07-spark-master /opt/spark/bin/spark-submit \
  /labs/jobs/build_marketing_customers.py --ds $(date +%Y-%m-%d)

# 2. AC1 — verify schema: must not contain full_name/cccd/date_of_birth/address/account_balance/credit_score
docker compose exec demo-cli python /scripts/query_trino.py \
  --user admin --password 'AdminDemo_2026!' \
  --sql "DESCRIBE delta.gold.marketing_customers"

# 3. AC2 — marketing sees data with phone/email masked
docker compose exec demo-cli python /scripts/query_trino.py \
  --user marketing --password 'MarketingDemo_2026!' \
  --sql "SELECT customer_id, phone, email, balance_tier FROM delta.gold.marketing_customers LIMIT 3"

# 4. AC2 — marketing is denied on all other tables
docker compose exec demo-cli python /scripts/query_trino.py \
  --user marketing --password 'MarketingDemo_2026!' \
  --sql "SELECT * FROM delta.gold.customer_analytics LIMIT 1"
# → expected: Access Denied

docker compose exec demo-cli python /scripts/query_trino.py \
  --user marketing --password 'MarketingDemo_2026!' \
  --sql "SELECT * FROM delta.silver.customers LIMIT 1"
# → expected: Access Denied

# 5. AC3 — admin and analyst are unaffected
docker compose exec demo-cli python /scripts/query_trino.py \
  --user admin --password 'AdminDemo_2026!' \
  --sql "SELECT customer_id, phone FROM delta.gold.customer_analytics LIMIT 2"

docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst --password 'AnalystDemo_2026!' \
  --sql "SELECT customer_id, phone FROM delta.gold.customer_analytics LIMIT 2"
```

**How to fix:**

*F1 — drop excess PII columns, replace raw financials with tier:*
```python
from pyspark.sql import functions as F

df_out = (
    df
    .withColumn("balance_tier",
        F.when(F.col("account_balance") < 50_000_000, "low")
         .when(F.col("account_balance") < 200_000_000, "medium")
         .otherwise("high"))
    .withColumn("updated_at", F.lit(ds).cast("date"))
    .select("customer_id", "segment", "region", "registered_at",
            "phone", "email", "balance_tier", "updated_at")
)
```

*F2 — mask phone/email at query layer via Trino (not in Spark):*

Do NOT mask in the Spark job — if you write masked values to disk, `admin` also loses access to the real values. Masking belongs in `trino_rules_patch.json`:
```json
"columns": [
  { "name": "phone", "mask": "regexp_replace(phone, '(\\d{3})\\d{4}(\\d{3})', '$1****$2')" },
  { "name": "email", "mask": "regexp_replace(email, '(^[^@]{2})[^@]+(@.+)', '$1***$2')" }
]
```

*F3 — restrict `marketing` to only `gold.marketing_customers` in Trino:*

Edit `labs/trino_rules_patch.json` so `marketing` has `SELECT` on exactly one table, and is denied everything else. Merge into `conf/trino/rules.json` then restart Trino:
```bash
docker compose restart trino
```

---

## Lab 3 — Least Privilege & Log Hygiene

**Files:** `jobs/sync_customers.py`, `sql/init_reporting.sql`

An intern was assigned to write a customer sync job from the source system into the lakehouse staging area, plus a SQL script to create a service account for an upcoming reporting system. The code runs correctly and produces accurate numbers.

An internal audit **flagged this pair of files with 3 findings**. Each finding is marked with a `# TODO` comment in the code. Your task: identify each one, explain the risk, and fix it.

**Available resources:**
- `demo/jobs/security_utils.py` — read it for what helpers are available
- `demo/sql/init_source.sql` — see how `etl_reader` is created as a reference for least privilege

**Acceptance criteria:**
1. After running the fixed job, **no PII values appear in logs or stdout**. Prove: `docker logs s07-spark-master 2>&1 | grep -i "phone\|cccd\|full_name"` returns nothing.
2. The reporting service account has only `SELECT` on `raw.customers`. Prove: `INSERT` with that account must be denied.
3. The job still produces the correct row count in `bronze.customers_sync`.

**How to fix:**

*F1 — remove `setLogLevel("DEBUG")` (line 20 of `sync_customers.py`):*

`spark-submit` defaults to `WARN` — remove this line entirely. At `DEBUG`, Spark dumps raw partition data including PII to stdout and executor logs:
```python
spark.sparkContext.setLogLevel("DEBUG")  # remove this line
```

*F2 — remove `df.show()` (line 35 of `sync_customers.py`):*

`df.show()` prints all rows including raw PII to stdout. Remove the line entirely:
```python
df.show(20)  # remove this line
```

*F3 — remove PII from the warning log (lines 40–44 of `sync_customers.py`):*

Logging individual rows exposes raw PII. Log the count only:
```python
# before
for row in flagged.collect():
    logger.warning(
        f"Low credit score record: {row['full_name']} ({row['cccd']}), ..."
    )

# after
logger.warning("Low credit score records: %d", flagged.count())
```

*`init_reporting.sql` — replace SUPERUSER account with least-privilege role:*

`SUPERUSER` bypasses all permission checks server-wide; `ALL PRIVILEGES` grants INSERT/UPDATE/DELETE far beyond what a read-only reporting account needs. Replace with:
```sql
CREATE USER lab_reporter WITH LOGIN PASSWORD 'LabReporter_2026!';
GRANT CONNECT ON DATABASE dataops_source TO lab_reporter;
GRANT USAGE ON SCHEMA raw TO lab_reporter;
GRANT SELECT ON raw.customers TO lab_reporter;
```

---

**Run & verify:**
```bash
# 0. Restart spark-master to clear old logs before verifying AC1
docker compose restart spark-master spark-worker

# 1. Setup: create the service account (run after fixing init_reporting.sql)
docker exec -i $(docker compose ps -q postgres-source) \
  psql -U postgres -d dataops_source < labs/sql/init_reporting.sql

# 2. Run the fixed sync job
# → expected output includes: synced 200 customers for <date>
docker exec s07-spark-master /opt/spark/bin/spark-submit \
  /labs/jobs/sync_customers.py --ds $(date +%Y-%m-%d)

# 3. AC1 — no PII in logs (step 0 ensures only current run's logs are present)
docker logs s07-spark-master 2>&1 | grep -i "phone\|cccd\|full_name"
# → expected: no output

# 4. AC2 — reporting account is denied INSERT
docker exec -i $(docker compose ps -q postgres-source) \
  psql -U lab_reporter -d dataops_source \
  -c "INSERT INTO raw.customers (customer_id,full_name,phone,email,cccd,date_of_birth,address,segment,region,account_balance,credit_score) VALUES ('x','x','x','x','x','2000-01-01','x','x','x',0,0)"
# → expected: ERROR:  permission denied for table customers
```
