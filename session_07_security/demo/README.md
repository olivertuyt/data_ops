# Session 07 Demo — Secure Bronze/Silver/Gold PII Pipeline

This is a **working, secure** implementation of the pipeline described in the lab
spec (`../README.md`) — instead of the 5 injected vulnerabilities, every one of
them is fixed here, and you can point at exactly where. Stack: Airflow (Celery) +
Spark + MinIO + Hive Metastore + Trino + Vault + Postgres, reusing the same
pattern as `session_05_pyspark_lakehouse` / `session_06_performance_optimization`.

| Original vuln | How this demo fixes it |
|---|---|
| #1 Hardcoded DB/MinIO credentials | Vault-backed Airflow Connection (`dags/customer_pii_pipeline.py`) + `hvac` fetch in every Spark job (`jobs/security_utils.py`) |
| #2 Unmasked PII copied to DWH | Gold drops `full_name`/`address`/`date_of_birth` and buckets `account_balance`/`credit_score` (`jobs/transform_gold.py`); Trino masks the rest per role |
| #3 Over-privileged service account | `etl_reader` is `SELECT`-only on one table (`sql/init_source.sql`) |
| #4 DEBUG logs / `.show()` leaking PII | Every job sets `WARN`, uses `.count()`, never dumps rows |
| #5 No audit trail | `log_pii_access()` writes a JSON line per read/write, before and after (`jobs/security_utils.py`) |
| Bonus: masked view for analysts | Trino file-based access control (`conf/trino/rules.json`) instead of a hand-rolled SQL view |

## 1. Start the stack

```bash
cd session_07_security
mkdir -p logs/audit
chmod 777 logs/audit   # Spark container writes here; Docker Desktop on macOS needs world-writable
docker compose up -d
```

Wait for everything to go healthy (`docker compose ps`) — Vault, `postgres-source`,
`hive-metastore` and `trino` all have healthchecks. First boot takes a few minutes
(image pulls + pip installs inside the Spark/Airflow containers).

Then create the empty Delta tables once:

```bash
docker exec s07-spark-master /opt/spark/bin/spark-submit /jobs/init_schema.py
```

Airflow UI: http://localhost:8888 (airflow/airflow) · Vault UI: http://localhost:8200 (token `root-demo-token`) · Trino: http://localhost:8092 · MinIO console: http://localhost:9001 (minioadmin/minioadmin).

## 2. How Trino SSL is enabled — and why it's mandatory

This demo runs Trino over HTTPS only. Walk through the three files that wire it up together:

**`docker-compose.yml` — entrypoint generates the keystore on first boot:**

```bash
keytool -genkeypair -alias trino -keyalg RSA -keysize 2048 -validity 3650 \
  -dname 'CN=trino, OU=demo, O=dataops-from-scratch' \
  -ext 'SAN=dns:trino,dns:localhost' \
  -keystore /etc/trino/certs/keystore.p12 -storetype PKCS12 \
  -storepass demo-keystore-password -keypass demo-keystore-password
keytool -exportcert -rfc -alias trino \
  -keystore /etc/trino/certs/keystore.p12 -storepass demo-keystore-password \
  -file /etc/trino/certs/trino.pem
```

`trino.pem` is the self-signed CA cert exported for clients to verify the server. The `certs/` directory is bind-mounted into both the Trino container and any client container (`demo-cli`, `airflow-worker`) — so they all share the same cert file without copying.

**`conf/trino/config.properties` — the three lines that enable HTTPS + auth:**

```properties
http-server.authentication.type=PASSWORD        # requires HTTPS — won't work over HTTP
http-server.https.enabled=true
http-server.https.port=8443
http-server.https.keystore.path=/etc/trino/certs/keystore.p12
http-server.https.keystore.key=demo-keystore-password
internal-communication.shared-secret=demo-internal-shared-secret-2026
```

Port 8080 (mapped to host 8092) stays open **only** for the unauthenticated `/v1/info` healthcheck — SQL queries sent to 8080 are rejected.

**`conf/trino/password-authenticator.properties` — file-based identity:**

```properties
password-authenticator.name=file
file.password-file=/etc/trino/password.db   # bcrypt-hashed passwords
file.refresh-period=5s                       # live reload — add a user, takes effect in 5s
```

**Why SSL is not optional here:** Trino enforces a protocol-level rule — `http-server.authentication.type=PASSWORD` refuses to serve SQL over HTTP. Without TLS, an attacker on the same network could intercept credentials and query results in plaintext, bypassing column masking entirely (the data is masked at the engine but travels unencrypted to the client). TLS closes that gap.

**Verify SSL is working:**

```bash
# should return Trino version info over HTTPS
curl -k https://localhost:8443/v1/info

# should be blocked (SQL requires auth over HTTPS, not HTTP)
curl http://localhost:8092/v1/statement -d "SELECT 1"
```

---

## 3. Prove there are no hardcoded secrets (gitleaks)

Use [gitleaks](https://github.com/gitleaks/gitleaks) — scans git history and working tree for leaked secrets (API keys, tokens, passwords matching known patterns):

```bash
# MacOs
brew install gitleaks   # or: https://github.com/gitleaks/gitleaks/releases
# Windows
winget install gitleaks

# scan full git history
gitleaks git --no-banner .

# scan only staged files before committing
gitleaks protect --staged --no-banner
```

Running against `demo/dags/` and `demo/jobs/` returns no findings — no credential values, only Vault path strings (`"dataops/minio"`) and dict key names. Compare with `labs/dags/customer_export_dag.py` to see the contrast.

> **Note:** gitleaks catches high-confidence patterns (API keys, tokens, private keys). Generic passwords like `"postgres"` require code review or a custom rule — no tool is a silver bullet.

## 4. Look at what's actually in Vault

```bash
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/dataops/minio
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/dataops/postgres_source
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/dataops/trino
docker exec -e VAULT_ADDR=http://127.0.0.1:8200 -e VAULT_TOKEN=root-demo-token s07-vault vault kv get secret/airflow/connections/postgres_source_conn
```

Point out: the DAG (`dags/customer_pii_pipeline.py`) never references
`postgres_source_conn`'s host/login/password directly — Airflow's
`VaultBackend` resolves `PostgresHook(postgres_conn_id="postgres_source_conn")`
against this exact path at task-run time.

## 5. Run the pipeline

In the Airflow UI, unpause and trigger `session_07_customer_pii_pipeline`. Watch
`validate_source_connection -> ingest_bronze -> transform_silver -> transform_gold
-> reconcile -> show_audit_summary` go green. `show_audit_summary`'s task log
prints the tail of the audit trail (see step 6).

## 6. Query as admin vs analyst — the masking payoff

Trino requires a password now (`file` password authenticator, see
`conf/trino/password.db` / `conf/trino/password-authenticator.properties`).
Pass `--password` explicitly. Demo bootstrap credentials (dev/test only):

> ⚠️ **Dev/test only.** These passwords are for classroom use. Never use hardcoded
> credentials in a real deployment — rotate and inject via a secrets manager.

| user | password | access |
|---|---|---|
| `admin` | `AdminDemo_2026!` | all columns, all regions |
| `analyst` | `AnalystDemo_2026!` | masked PII, all regions |
| `analyst_uk` | `AnalystUK_2026!` | masked PII, UK region only |
| `analyst_us` | `AnalystUS_2026!` | masked PII, US region only |

```bash
docker compose exec demo-cli python /scripts/query_trino.py \
  --user admin --password 'AdminDemo_2026!' \
  --sql "SELECT customer_id, phone, email, cccd FROM delta.gold.customer_analytics LIMIT 5"

docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst --password 'AnalystDemo_2026!' \
  --sql "SELECT customer_id, phone, email, cccd FROM delta.gold.customer_analytics LIMIT 5"
```

Connecting with DBeaver or any JDBC tool? Use **HTTPS port 8443** with the
self-signed cert bundled at `conf/trino/certs/trino-truststore.jks`
(password: `changeit`). Paste this as the JDBC URL:

```
jdbc:trino://localhost:8443/delta/gold?SSL=true&SSLTrustStorePath=<absolute-path>/session_07_security/conf/trino/certs/trino-truststore.jks&SSLTrustStorePassword=changeit&SSLTrustStoreType=JKS
```

Replace `<absolute-path>` with the full path on your machine (`pwd` inside
`session_07_security` gives it). Trino CLI: `trino --server
https://localhost:8443 --insecure --user admin --password` (`--insecure`
skips cert verification — acceptable for CLI, but JDBC clients should use
the truststore above). Port 8092 is plain-HTTP for the internal healthcheck
only and rejects authenticated queries.

`admin` sees real phone/email/cccd. `analyst` sees `phone`/`email` masked
(`090****678`, `jo***@gmail.com`) and `cccd` as `NULL` — same query, same table,
different masking rule, enforced by Trino (`conf/trino/rules.json`), not by the
application.

Then show defense in depth on the raw layer:

```bash
docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst --password 'AnalystDemo_2026!' \
  --sql "SELECT * FROM delta.bronze.customers LIMIT 5"
# -> blocked: Trino ACL denies SELECT on bronze (rules.json) + MinIO trino-svc has no bronze bucket access (defense in depth)
```

## 7. Row-level filtering — analyst_uk vs analyst_us

`analyst_uk` and `analyst_us` are scoped analysts: same masking as `analyst`,
but each sees only their own region's rows. The enforcement lives in
`conf/trino/rules.json` under the `tables` section — the `"filter"` field:

```json
{
  "user": "analyst_uk",
  "catalog": "delta",
  "schema": "gold",
  "table": "customer_analytics",
  "privileges": ["SELECT"],
  "filter": "region = 'UK'",
  "columns": [ ... ]
}
```

Trino injects this as a `WHERE` clause on every query — the analyst cannot
bypass it, even with `SELECT *`. Run both users and compare:

```bash
# analyst_uk — only UK rows, phone/email masked, cccd NULL
docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst_uk --password 'AnalystUK_2026!' \
  --sql "SELECT customer_id, region, phone, email, cccd FROM delta.gold.customer_analytics LIMIT 5"

# analyst_us — only US rows, same masking
docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst_us --password 'AnalystUS_2026!' \
  --sql "SELECT customer_id, region, phone, email, cccd FROM delta.gold.customer_analytics LIMIT 5"
```

Expected output for `analyst_uk`:

```
 customer_id | region |    phone     |       email        | cccd
-------------+--------+--------------+--------------------+------
 C0042       | UK     | 090****123   | jo***@gmail.com    | NULL
 C0071       | UK     | 098****456   | na***@outlook.com  | NULL
 ...
```

The `region` column always shows `UK` — rows from other regions are invisible,
not just hidden. Point out: this is row-level security at the query engine
layer, with no application code involved.

## 8. Tail the audit trail

```bash
tail -20 logs/audit/pii_access.log
```

Each line is a JSON record: job, table, columns touched, row count, `ds`,
timestamp — answers "who read what PII, when, how much" without ever writing a
real phone/email/cccd value into a log file.

## 9. Least privilege at the DB layer

```bash
docker exec -it $(docker compose ps -q postgres-source) \
  psql -U etl_reader -d dataops_source -c "INSERT INTO raw.customers (customer_id) VALUES ('x')"
# -> ERROR: permission denied for table customers
```

`etl_reader` (used by the Vault-backed Airflow connection and by the Spark JDBC
read) can only `SELECT` — it cannot write, `TRUNCATE`, or `DROP`, unlike the
injected `SUPERUSER` from vuln #3.

## Demo simplifications (call these out explicitly)

- **Vault dev mode**: single root token, in-memory storage, no TLS. A real
  deployment uses a properly unsealed/HA Vault cluster and short-lived
  AppRole/Kubernetes-auth tokens instead of a static root token.
- **Trino identity**: password-authenticated (`file` authenticator,
  `conf/trino/password.db`, bcrypt-hashed) over HTTPS, but the TLS certificate
  is self-signed and regenerated on every container restart — clients must
  skip verification (`SSLVerification=NONE`). A real deployment uses a
  CA-issued certificate (or LDAPS/OAuth2) with verification left on.
- **Bootstrap passwords** (`docker-compose.yml`, `sql/init_source.sql`,
  `vault-init/seed_secrets.sh`) are demo-only literals, same tier as this repo's
  existing `MINIO_ROOT_PASSWORD: minioadmin`. Application code never contains
  them — only infra bootstrap files do.

## Shutdown

```bash
docker compose down -v   # -v also drops the Postgres/MinIO/metastore volumes
```
