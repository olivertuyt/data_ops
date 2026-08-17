# ShopVN Pipeline Runbook

**Owner**: Data Platform  
**Business escalation**: domain steward in `contracts/`  
**SLA**: Gold available by 08:00 Asia/Ho_Chi_Minh  
**RTO**: two hours from detection  
**RPO**: latest successful daily publication

## First response

1. In Airflow, capture DAG run ID, logical window, failed task, exception class, attempt,
   start/end time, and log link. Do not clear tasks yet.
2. In the operations dashboard, check domain markers, source manifests, blocking DQ,
   source/target counts, amount deltas, and last successful publication.
3. Classify the failure as transient infrastructure, source availability, contract/data,
   transformation, DQ/reconciliation, or publication.
4. Identify affected domains and dates. Candidate tables are not copied into the
   physical `serving` tables until blocking DQ passes. Existing serving rows for other
   dates and domains remain available when a new marker is missing, `PENDING`, or
   `FAIL`.
5. Preserve logs, manifests, quarantined inputs, candidates, and audit rows. Never
   truncate broad tables, modify a source, suppress a DQ rule, or manually set PASS.

## Safe rerun

After correcting the cause, trigger `shopvn_daily` with the exact affected inclusive
date range. Do not overlap a scheduled run; `max_active_runs=1` is a safety boundary.
Confirm source manifests, candidate counts, DQ rows, and PASS markers. Run the
reconciliation SQL and compare count/sum to the pre-incident or prior rerun evidence.

## Scenario 1 — API timeout, 429, 404, or 500

- 429: confirm `retry_after` was honored and the client remained at or below 95 rpm.
- Timeout: confirm at most three retries with 1/2/4-second backoff.
- 500: confirm one retry and a terminal technical error if it repeats.
- 404: confirm it was recorded as expected missing shipment and not retried.

If the terminal-error count is nonzero, Operations publication is blocked. Check API
health without exposing its key. When healthy, rerun only the affected date window.
Confirm the orders-to-delivery LEFT JOIN preserved every eligible order and that carrier
metrics exclude null shipment attributes from denominators where documented.

## Scenario 2 — Missing or late SFTP file

Confirm partner/date in the manifest and that the status is `MISSING`, not checksum
failure. Independent PostgreSQL/API and Customer work may continue; Finance, Operations
return-rate, and Product stay unpublished for that run. Ask the partner to place both
CSV and `.md5` atomically. When present, rerun the same date. Verify manifest `READY`,
checksum equality, no duplicate file rows, dependent domain PASS, and unchanged results
for independent domains.

The supplied image pre-bakes late-file scenarios, so delayed arrival must be documented
as a conceptual drill if it cannot be triggered live.

## Scenario 3 — Corrupt checksum or malformed CSV

Do not rename the `.part` file to ready and do not load it. Preserve the quarantine
path, expected/actual MD5, byte count, and contract error. Obtain a corrected file from
the partner; never edit the received file locally to make the checksum match. Rerun the
same date and confirm only the corrected source hash becomes eligible.

## Scenario 4 — Blocking DQ or reconciliation failure

Locate the exact rule and observed/threshold values. Determine whether the cause is
source data, an incompatible schema, duplicate key, join-grain expansion, or business
logic. Retain the candidate. A rule change requires contract-owner review; do not turn a
blocking result into a warning to meet the SLA. After source/code correction, rerun and
compare source, Silver, candidate, and Gold counts and amounts.

## Scenario 5 — Mid-publication failure

Publication is atomic per Iceberg table, not across every table in a domain. A failure
between table commits can therefore leave only part of that domain's serving tables
updated while its marker remains `PENDING` or `FAIL`. Stop consumers that require a
cross-table-consistent snapshot, inspect every affected table snapshot and model count,
then either finish an idempotent rerun or roll each affected table back to its recorded
pre-run snapshot. Confirm the domain marker becomes PASS only after every model and
audit record is complete.

## Scenario 6 — Host suspend, zombie task, or expired vended credential

If the scheduler reports a heartbeat gap and a zombie task after the Docker host sleeps,
record the gap and SIGTERM before recovery. A subsequent Spark error such as S3 `403`
with an unknown access key can indicate cached, expired Polaris vended credentials; do
not rotate or expose static MinIO keys. Restart only Polaris and Trino, wait for both
health checks, and prove recovery with both a Spark Iceberg data read and a Trino data
query. Clear only `failed` and `upstream_failed` task instances for the exact DAG run ID;
preserve successful upstream tasks and their manifests. If either data probe fails,
keep publication blocked and escalate as a storage/catalog incident.

## Rollback

If a bad run was published, stop new runs, record the affected marker and Iceberg
snapshot IDs, and obtain domain-owner approval. Preferred recovery is an Iceberg
snapshot rollback or a view/marker correction to the last known-good version after
reconciliation. Never use broad deletion as the first response. Record who approved,
snapshot before/after, queries, counts, amounts, and consumer impact.

## Recovery drill evidence

Record start/detection/recovery times, run ID, window, failure injected, affected
domains, commands/queries used, DQ and reconciliation output, resulting snapshot/marker,
screenshots or dashboard links, RTO outcome, root cause, and prevention action. A drill
is PASS only if data is correct, publication visibility is safe, and recovery is under
two hours.
