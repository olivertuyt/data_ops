# Validation Report

**Date**: 2026-08-15  
**Environment**: Windows + Docker Desktop, local ShopVN source images, Airflow 2.9.3,
Spark 3.5.1, Iceberg 1.10.1, Polaris 1.7.0, Trino 483, and MinIO.  
**Overall status**: **INCOMPLETE**. The normal June 1 path, both flash-sale windows,
contract/fault checks, engineering gates, and full three-DAG-run idempotency test pass.
The production-scheduled 08:00 SLA proof and complete dashboard walkthrough remain
outstanding.

## Engineering quality

| Check | Command or evidence | Status | Result / reason |
|---|---|---|---|
| Python syntax | `python -m compileall -q spark dags monitoring tests scripts` | PASS | Exit 0. |
| Format | `ruff format --check .` | PASS | 54 files already formatted in the final image. |
| Lint | `ruff check .` | PASS | No findings. |
| Unit, contract, model, and DAG tests | `pytest -q` in the runtime image | PASS | 26 passed, none skipped. |
| Secret/service endpoint scan | `python scripts/check_for_secrets.py` | PASS | No fixture secret or embedded concrete service URL found. |
| Security scan | `bandit -q -r spark dags monitoring` | PASS | No blocking finding; only notices for reviewed scoped `nosec B608` SQL identifiers. |
| Compose validation | `docker compose --env-file config/version.env --env-file .env config --quiet` | PASS | Exit 0. |
| Runtime image build | Compose build | PASS | Final image `shopvn-pipeline-runtime:local-docker-validation` built after the Dockerfile permission edit. |
| Git whitespace | `git diff --check` | PASS | No whitespace errors; Windows CRLF conversion notices are non-blocking. |

## Runtime and data-correctness evidence

| Check | Evidence | Status | Result / reason |
|---|---|---|---|
| Three source systems | Source Compose health plus live PostgreSQL/API/SFTP probes | PASS | PostgreSQL, logistics API, and SFTP healthy; PostgreSQL read-only access returned 211,806 orders. |
| Platform health | Platform Compose health | PASS | PostgreSQL metadata DB, MinIO, Polaris, and Trino healthy; Airflow, dashboard, Prometheus, and Grafana reachable. |
| Spark/Trino compatibility | Spark count probe and Trino query over the same Iceberg data | PASS | Spark read 211,806 Bronze orders; Trino read persisted serving data. |
| Normal end-to-end window | Airflow run `runtime-e2e-20260601-2` | PASS | All 18 task instances succeeded after scoped fixes/recovery. |
| Domain publication gates | `polaris.audit.data_quality_results` for June 1 | PASS | Customer 15, Finance 7, Operations 9, Product 8 blocking checks; zero failures. |
| Finance reconciliation | Trino `fact_daily_revenue`, 2026-06-01 | PASS | 5 rows, net revenue 77,986,500,021.00, 5,555 orders. |
| API behavior | `tests/test_api_contract.py` plus live response-shape probe | PASS | Batch <=50, normal/mixed/not-found responses, 429 Retry-After, timeout 1/2/4, 404 no retry, and one 500 retry verified. |
| SFTP missing file | `runtime-fault-missing-20260604` manifest and Finance DQ | PASS | TikTok marked MISSING; Finance publication blocked; serving rows for June 4 remained zero. |
| SFTP corrupt checksum | `runtime-fault-corrupt-20260609` manifest and run audit | PASS | Expected/actual MD5 differed; ingestion failed and quarantined; serving rows for June 9 remained zero. |
| Late-file replay | `runtime-late-arrival-20260618` manifests | PASS | All three corrected files and checksums validated for an affected-window replay. This is a replay drill, not a real-time wait event. |
| Schema evolution | `python scripts/runtime_schema_evolution_smoke.py` | PASS | `SCHEMA_EVOLUTION_PASS additive=accepted incompatible=blocked rows=2`. |
| Intentional anomalies | Silver anomaly queries | PASS | 622 discount and 4,420 zero-quantity anomalies contributed zero eligible revenue/units. |
| Plaintext PII | Silver/serving schema scan | PASS | No `full_name`, `phone`, `email`, `comment`, or `ward` columns in Silver/serving. |
| Publication rerun stability | Three repeated publication writes for June 1 | PASS | Each produced 5 rows, revenue 77,986,500,021, and 5,555 orders. This proves publication-path idempotency only. |
| Three full DAG reruns | `runtime-e2e-20260601-2`, `runtime-idempotency-20260601-2`, and `runtime-idempotency-20260601-3` | PASS | All three complete source-to-serving runs succeeded with 18/18 tasks, 39/39 blocking DQ checks, and 4/4 domain markers. Each produced 5 revenue rows, direct revenue 61,907,313,150.00, marketplace revenue 16,079,186,871.00, total revenue 77,986,500,021.00, 5,555 orders, and zero duplicate revenue keys. |
| Dashboard availability | HTTP/health probes | PASS | Operations dashboard healthy; Prometheus and Grafana returned HTTP 200. |
| Dashboard business questions | Reviewer walkthrough/screenshots | NOT RUN | Endpoints are healthy, but the complete on-call walkthrough was not captured because the in-app browser runtime was blocked by a Windows `EPERM` error before navigation. The dashboard remains available at `http://localhost:8501`. |
| Recovery drill | Polaris restart, data probes, and exact-run Airflow recovery | PASS | Catalog metadata persisted; earlier restart recovered in about 11.5 seconds. After host suspend, Spark and Trino data reads passed after scoped Polaris/Trino restart. |
| June 7 flash-sale volume | `runtime-flashsale-20260607-1` | PASS | 18/18 tasks succeeded after exact-run recovery; API audited 17,730 requested orders, 15,065 shipments, and 647,660 ms. All 39 blocking DQ checks passed; 4 domain markers PASS; serving revenue 278,837,774,594 across 23,439 orders. |
| June 15 flash-sale volume | `runtime-flashsale-20260615-1` | PASS | 18/18 tasks succeeded in 1,753 seconds; API audited 16,000 requested orders, 13,514 shipments, and 877,287 ms while recovering live timeouts and HTTP 500s. All 39 blocking DQ checks passed; 4 domain markers PASS; serving revenue 266,371,497,013 across 22,623 orders. |
| Before-08:00 production SLA | Scheduled production-like execution | NOT RUN | Manual daytime validation does not prove the daily 08:00 SLA even when task duration is measured. |

## Runtime defects found and corrected

- Initialized named landing volumes for the Airflow UID and added a permission test.
- Made SFTP source-file metadata deterministic instead of relying on
  `input_file_name()` through a MERGE.
- Matched marketplace earned-revenue arithmetic to the fixture's binary-float/floor
  contract and added the one-VND edge-case test.
- Allowed the contractually sparse rating fact to publish an empty logical window.
- Replaced cross-engine-incompatible Spark Iceberg views with physical Iceberg serving
  tables readable by Trino.
- Added live catalog and schema-evolution smoke scripts and aligned the runbook with
  physical serving-table atomicity.

## Full-DAG idempotency evidence

| Run | Airflow run ID | Revenue rows | Direct net revenue | Marketplace earned revenue | Total net revenue | Orders | Duplicate revenue keys | Blocking DQ | Domain markers |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 | `runtime-e2e-20260601-2` | 5 | 61,907,313,150.00 | 16,079,186,871.00 | 77,986,500,021.00 | 5,555 | 0 | 39/39 PASS | 4/4 PASS |
| 2 | `runtime-idempotency-20260601-2` | 5 | 61,907,313,150.00 | 16,079,186,871.00 | 77,986,500,021.00 | 5,555 | 0 | 39/39 PASS | 4/4 PASS |
| 3 | `runtime-idempotency-20260601-3` | 5 | 61,907,313,150.00 | 16,079,186,871.00 | 77,986,500,021.00 | 5,555 | 0 | 39/39 PASS | 4/4 PASS |

The DQ audit also recorded the same four non-blocking warning checks per run: one
passed and three intentionally failed anomaly warnings. These warnings did not block
publication and did not alter the reconciled revenue or order metrics.

## Recovery incident observed during validation

The Docker host stopped advancing scheduler heartbeats between approximately 07:25 and
08:48 UTC. Airflow correctly detected `bronze_api` as a zombie and sent SIGTERM. Docker
reported zero container restarts. The next Spark sessions received expired/cached
Polaris vended S3 credentials and MinIO returned 403. Restarting only Polaris and Trino,
then requiring both a Spark Iceberg data read and a Trino data query, restored access.
Only `failed` and `upstream_failed` tasks for the exact flash-sale run ID were cleared;
successful PostgreSQL/SFTP tasks were preserved.

## Assumptions and limitations

- Source fixtures are authoritative for the exercise, use Asia/Ho_Chi_Minh business
  dates, and contain no approved hard-delete feed.
- The supplied SFTP image pre-bakes late files, so the late-arrival evidence is a scoped
  replay rather than observation of an actual delayed delivery.
- Publication is atomic per Iceberg table, not transactionally atomic across every
  table in a domain; the runbook now requires snapshot-aware recovery for a
  mid-publication failure.
- The project must not be called production-ready until every required NOT RUN item
  above is resolved or explicitly accepted by the owner.
