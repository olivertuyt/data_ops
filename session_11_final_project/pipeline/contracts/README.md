# ShopVN Data Contracts

These contracts are the reviewable source of truth for dataset ownership, grain,
keys, schema, freshness, privacy, replay, retention, and publication controls.

- `sources/` describes the three external systems. Bronze preserves source fields and
  technical lineage; physical source deletion is not supported by the fixture.
- `models/silver_models.yaml` describes typed internal models. Silver is restricted to
  the data-platform role and contains no plaintext direct PII.
- `models/gold_models.yaml` describes versioned Gold storage and physical serving
  tables. Blocking DQ must pass before any serving write. The domain PASS marker
  records cross-table completion; consumers that require a domain-consistent snapshot
  must check it because commits are atomic per Iceberg table, not across a domain.

Contract changes require Data Platform review. Changes to Finance measures, customer
privacy, model grain, required fields, or blocking DQ also require the affected domain
steward. Additive nullable Bronze columns are compatible after review; removal, rename,
type, unit, nullability, enum, or semantic changes are breaking by default.
