# Quality gates and completion

Separate gates into two groups.

Engineering quality:

- format, lint, type/static checks;
- dependency and secret/security scans;
- unit, contract, model, and orchestration import tests;
- Compose/config validation and compatibility smoke tests.

Data correctness:

- schema/grain/nullability/key checks;
- DQ for validity, completeness, consistency, anomalies, and freshness;
- source-to-target count/amount/key reconciliation;
- idempotent rerun/replay verification;
- integration, failure classification, quarantine, rollback, and recovery tests;
- end-to-end validation against required real or controlled source fixtures.

Engineering tests passing never compensates for a failed data-correctness gate. Skipped
required tests keep the task incomplete unless the user explicitly approves the exception.

For this repository's ShopVN final project, also use
`session_11_final_project/guideline/review.md` as the acceptance matrix. Verify, as
applicable, three healthy sources, end-to-end execution, API 429/timeout/404/500 behavior,
SFTP checksum/missing/late handling, volume spike, schema evolution, three identical
reruns, anomaly isolation, no plaintext PII in Silver/Gold, dashboard questions, and the
recovery drill.

For release readiness, verify immutable versioned artifacts, pinned dependencies/images,
commit/config/schema versions, post-deploy smoke tests, rollback, RPO/RTO, restore drill,
lineage/ownership, audit metadata, retention, and runbook coverage.

The final report must list each check with its command or evidence source and `PASS`,
`FAIL`, or `NOT RUN` plus a reason. Never mark a project complete by inference.
