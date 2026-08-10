# Session 07 Homework — Cross-Regional Compliance: UK GDPR & US GLBA

## Background

The company's fintech platform has expanded internationally. `gold.customer_analytics` now contains customers from two regions: **US** and **UK**. The data team receives two simultaneous compliance demands:

**From the UK DPO (ICO pre-audit, July 2026):**

> The Data (Use and Access) Act 2026 and UK GDPR Article 5(1)(b) require that personal data is processed only for the purpose for which it was collected. Our UK customer records contain financial data subject to strict purpose limitation. A US-based analyst querying UK customer rows has no documented lawful basis under Article 6. This must be corrected before the ICO audit. Additionally, Article 25 (Data Protection by Design and by Default) requires that access controls be embedded at the infrastructure level — application-layer filtering is not sufficient.

**From the CISO (FTC Safeguards Rule review):**

> The FTC Safeguards Rule (§314.4(c), enforceable June 2023) requires access controls that implement least privilege and include periodic credential review. Our audit found that the `etl_reader` database credential has never been rotated since initial deployment. This is a mandatory remediation item. The credential must be rotated with zero downtime — no service restart, no code change.

The demo stack is running. `gold.customer_analytics` has 200 rows split evenly between `region = 'US'` and `region = 'UK'`. Two new Trino users have been provisioned for you:

| User | Password | Intended scope |
|---|---|---|
| `analyst_uk` | `AnalystUK_2026!` | UK customers only |
| `analyst_us` | `AnalystUS_2026!` | US customers only |

---

## Part 1 — Row-Level Security (UK GDPR Article 5(1)(b) + Article 25)

`conf/trino/rules.json` currently gives the existing `analyst` user column-masking on `gold.customer_analytics` but no row restriction — both `analyst_uk` and `analyst_us` inherit no rules yet and can see all rows.

Your task: add row filters to `conf/trino/rules.json` so that:

- `analyst_uk` can only see rows where `region = 'UK'`
- `analyst_us` can only see rows where `region = 'US'`
- `admin` is unaffected — sees all rows across both regions
- The existing `analyst` column-masking rules are untouched

You can research information from [this site](https://trino.io/docs/current/security/file-system-access-control.html#filter-and-mask-environment) of Trino document.

After editing `rules.json`, apply it:

```bash
docker compose restart trino
```

### Acceptance criteria

1. Running the same query as all three users produces different row counts:

```bash
# from session_07_security/
docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst_uk --password 'AnalystUK_2026!' \
  --sql "SELECT COUNT(*) FROM delta.gold.customer_analytics"

docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst_us --password 'AnalystUS_2026!' \
  --sql "SELECT COUNT(*) FROM delta.gold.customer_analytics"

docker compose exec demo-cli python /scripts/query_trino.py \
  --user admin --password 'AdminDemo_2026!' \
  --sql "SELECT COUNT(*) FROM delta.gold.customer_analytics"
```

Expected: `analyst_uk` → 100, `analyst_us` → 100, `admin` → 200.

2. `analyst_uk` attempting to filter for US rows explicitly returns 0 rows — the row filter cannot be bypassed by a `WHERE` clause:

```bash
docker compose exec demo-cli python /scripts/query_trino.py \
  --user analyst_uk --password 'AnalystUK_2026!' \
  --sql "SELECT COUNT(*) FROM delta.gold.customer_analytics WHERE region = 'US'"
```

3. The existing `analyst` user's column masking still works correctly (phone/email masked, cccd = NULL).

4. Answer in writing: UK GDPR Article 25(2) states that "by default, personal data shall not be... accessible to an indefinite number of natural persons." How does a Trino row filter enforce this principle technically? What would a purely application-layer filter (e.g., a `WHERE` clause added by the application) fail to guarantee that a row filter at the query engine level does?

---

## Part 2 — Credential Rotation (GLBA Safeguards Rule 314.4(c))

The `etl_reader` Postgres user's password is currently `EtlReader_DemoOnly_2026!` (visible in Vault at `secret/dataops/postgres_source`). You must rotate it to a new value **without restarting any container and without changing any line of code**.

The Spark jobs fetch credentials from Vault at runtime on every execution — this is the property that makes zero-downtime rotation possible.

### Rotation procedure (your task is to execute this correctly)

Step 1 — change the password in Postgres:

```bash
docker exec -i $(docker compose ps -q postgres-source) \
  psql -U postgres -d dataops_source -c \
  "ALTER USER etl_reader WITH PASSWORD '<your-new-password>';"
```

Step 2 — update the secret in Vault (use single quotes to avoid shell expansion):

```bash
docker exec \
  -e VAULT_ADDR=http://127.0.0.1:8200 \
  -e VAULT_TOKEN=root-demo-token \
  s07-vault \
  vault kv patch secret/dataops/postgres_source password='<your-new-password>'
```

Step 3 — trigger the pipeline immediately after (no restart):

```bash
docker exec s07-spark-master /opt/spark/bin/spark-submit \
  /jobs/ingest_bronze.py --ds $(date +%Y-%m-%d)
```

### Acceptance criteria

1. Prove the old password no longer works:

```bash
docker exec -i $(docker compose ps -q postgres-source) \
  psql "postgresql://etl_reader:EtlReader_DemoOnly_2026!@localhost/dataops_source" \
  -c "SELECT 1" 2>&1
# must return: FATAL: password authentication failed
```

2. Prove Vault holds the new password:

```bash
docker exec \
  -e VAULT_ADDR=http://127.0.0.1:8200 \
  -e VAULT_TOKEN=root-demo-token \
  s07-vault vault kv get secret/dataops/postgres_source
```

3. `ingest_bronze` job succeeds with the new credential — no code change, no container restart.

4. Answer in writing (two parts):

   **a)** Why does `get_vault_secret()` in `demo/jobs/security_utils.py` make zero-downtime rotation possible? What specific line in that function is the key, and what would break if Spark cached the credential at startup instead?

   **b)** If the credential had been hardcoded directly in `ingest_bronze.py` (as in Lab 1), what would the rotation procedure have required? List every step, including the git implications.

---

## Self-check

Run the automated verifier from `session_07_security/`:

```bash
bash homework/verify.sh
```

All checks must pass before submission.

---

## Submission checklist

- [ ] Part 1: screenshot or terminal output showing `analyst_uk` → 100, `analyst_us` → 100, `admin` → 200
- [ ] Part 1: screenshot showing `analyst_uk` querying `WHERE region = 'US'` returns 0
- [ ] Part 1: written answer on Article 25(2) vs application-layer filtering
- [ ] Part 2: terminal output showing old password rejected
- [ ] Part 2: terminal output showing `ingest_bronze` success after rotation
- [ ] Part 2: written answer (a) on `get_vault_secret()` — which line, why
- [ ] Part 2: written answer (b) on what hardcoded credential rotation would require
