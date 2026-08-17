# Validation Plan and Evidence Matrix

Use the actual command output, Airflow run link, audit row, Trino query result, or
dashboard screenshot as evidence. A test is `PASS` only when executed; code presence is
not evidence of runtime success.

| Gate | Evidence source | Pass condition |
|---|---|---|
| Python syntax | `python -m compileall -q spark dags monitoring tests scripts` | Exit 0 |
| Format/lint | `ruff format --check . && ruff check .` | Exit 0 |
| Secret scan | `python scripts/check_for_secrets.py` and repository scanner | No blocking findings |
| Security scan | `bandit -q -r spark dags monitoring` | No unreviewed blocking findings |
| Unit/contract/model/DAG tests | `pytest` | All required tests pass, none silently skipped |
| Compose config | Compose config command from README | Exit 0 with private `.env` |
| Three source health | Source and platform `docker compose ps` | PostgreSQL, API, SFTP and required platform services healthy |
| API compatibility | API test/logs | Batch ≤50, response contract, 429, timeout, 404, 500 behavior demonstrated |
| SFTP integrity | Manifest and fault run | Missing, corrupt, and corrected/late paths demonstrated |
| Spark/Trino compatibility | Same Iceberg table read from both | Matching schema/count |
| End-to-end | Airflow `2026-06-01` run | All applicable domain markers PASS before 08:00 |
| Flash sale | Airflow `2026-06-07` and `2026-06-15` | Three-times volume completes before SLA |
| Schema evolution | Additive and incompatible-type fixtures | Additive tolerated; incompatible type blocks without corruption |
| Idempotency | Run same date three times + `sql/reconciliation.sql` | Identical Gold counts and sums; no duplicate keys |
| Intentional anomalies | DQ and Gold SQL | Discount/quantity anomalies do not contaminate revenue/units |
| PII | Silver/Gold schema + sample scan | No plaintext name, phone, email, ward, or review comment |
| Observability | Airflow, Grafana, operations dashboard | Completion, service health, freshness, DQ, rejected data and revenue reconciliation answerable without SSH |
| Recovery | Timed drill using runbook | Correct data and safe visibility restored within two hours |

## Rerun evidence template

| Run | Run ID | Logical date | Gold count | Direct net revenue | Marketplace earned revenue | Duplicate keys | Result |
|---:|---|---|---:|---:|---:|---:|---|
| 1 | `runtime-e2e-20260601-2` | 2026-06-01 | 5 | 61,907,313,150.00 | 16,079,186,871.00 | 0 | PASS |
| 2 | `runtime-idempotency-20260601-2` | 2026-06-01 | 5 | 61,907,313,150.00 | 16,079,186,871.00 | 0 | PASS |
| 3 | `runtime-idempotency-20260601-3` | 2026-06-01 | 5 | 61,907,313,150.00 | 16,079,186,871.00 | 0 | PASS |

## Exception rules

- The course image cannot trigger real late delivery because files are pre-baked; mark
  the live wait as `NOT RUN` and attach the conceptual recovery drill.
- A missing local dependency or unavailable Docker daemon is `NOT RUN`, not `PASS`.
- A skipped required test keeps the project incomplete unless the owner explicitly
  approves an equivalent test and the replacement evidence is linked.
