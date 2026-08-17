# Project structure and maintainability

Use cohesive boundaries. Prefer a structure like:

```text
pipeline/
├── jobs/
│   ├── ingestion/
│   │   ├── postgres_orders/
│   │   ├── api_shipments/
│   │   └── sftp_marketplace/
│   ├── transformation/
│   │   ├── silver_orders/
│   │   │   ├── transform.py
│   │   │   ├── schema.py
│   │   │   ├── test_transform.py
│   │   │   └── README.md
│   │   └── silver_order_shipments/
│   ├── validation/
│   └── publication/
├── common/
├── contracts/
└── tests/
```

- Keep one target table/model's business transformation in one module or package.
- Allow multiple functions in that module only when they are stages of the same model
  and share its grain.
- Group related tables only for a real shared contract, join, intermediate, or atomic
  publication boundary; document each output grain.
- Keep DAGs/workflows thin. Put business logic in model packages, not orchestration files.
- Put only reusable technical behavior in `common/`: Spark/session setup, storage
  writers, audit, retry, schema primitives, and config validation.
- Do not create abstractions for one call, split trivial expressions into many functions,
  or use line count as the reason to split a cohesive module.
