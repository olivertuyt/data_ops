# Contracts, schema evolution, and configuration

Store source and model contracts as reviewable artifacts under `contracts/`:

```text
contracts/
├── sources/
├── models/
└── README.md
```

Each contract records owner/steward, source of truth, consumers, version, grain, keys,
schema, nullability, units, timezone, SLA/freshness, DQ thresholds, PII class, retention,
delete semantics, replay window, and breaking-change policy.

Schema rules:

- Treat additive nullable fields as potentially compatible only after contract review.
- Treat rename, drop, incompatible type, precision/unit, semantic, nullability, and
  domain changes as breaking unless a migration or compatibility layer is documented.
- Run schema diff checks before processing; never silently coerce incompatible types.
- Define behavior for unknown columns and missing required columns; quarantine invalid
  records while preserving raw evidence.
- For CDC, preserve source sequence/version and operation type; test duplicates and
  out-of-order events. For snapshots, define hard-delete/soft-delete/tombstone behavior.

Configuration rules:

- Commit `.env.example` with safe placeholders and documented variables.
- Keep `.env`, `.env.local`, credentials, and production config untracked.
- Read environment variables only in a config boundary and pass a validated config object
  to business code.
- Validate required credentials, URLs, ports, timeouts, buckets, catalogs, and paths at
  startup. Fail fast; never use real-looking fallback credentials or endpoints.
- Inject staging/production config at deployment time. Never bake secrets or production
  addresses into code, images, SQL, DAGs, tests, or artifacts.

Backfill rules:

- Use an explicit bounded logical-date range and documented MERGE, partition-replacement,
  or append mode.
- Define watermark movement, late-data handling, concurrency, approval, and post-run
  reconciliation. Reopen only affected windows and rerun idempotently.
