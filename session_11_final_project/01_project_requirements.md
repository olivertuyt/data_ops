# ShopVN Analytics Platform — Final Project Requirements

**Course**: Master Class DataOps for Modern Data Platforms: From Pipeline to Production

---

## 1. Context

ShopVN is an e-commerce platform selling through two channels:

- **Direct channel** (web / app): managed in-house, data in PostgreSQL
- **Marketplace channel** (Lazada, Shopee, TikTok): external platforms, daily settlement files via SFTP

Logistics is handled by third-party carriers (GHN, GHTK), tracked via a REST API.

Currently, data is scattered across three isolated systems. No team has a complete picture. The goal of this project is to build a **centralized data platform** that serves analytics for internal teams — with data available every morning before 8:00 AM.

---

## 2. Business Requirements

Each use case describes what a business team needs. You should implement the data pipeline that makes this possible. **How** to design the solution (DWH vs Lakehouse, SCD type, etc.) is the your decision.

### UC1 — Finance: Revenue & Spending

**Goal**: Finance team needs to track daily revenue and customer spending behavior.

**Required outputs**:
- Daily revenue broken down by channel (direct vs marketplace)
- Actual revenue from each marketplace after deducting commissions
- Average customer spend per day and per month
- Total discount and voucher cost impact on revenue
- Historical tracking of order value changes (returns and refunds must be reflected accurately)

**Sample business questions**:
> - "On the June 7 flash sale, what was total net revenue after discounts and returns?"
> - "Did Shopee or Lazada contribute more revenue in June?"
> - "Who are the top 10 customers by spend in June?"

> **Sources**: PostgreSQL + SFTP
> **Key tables**: `orders`, `order_items`, `vouchers`, `returns` + SFTP CSV files
> **Design consideration**: Direct and marketplace revenue must be modeled separately before being combined — they have different schemas and different revenue rules. Refund deduction only applies to `returns` with `status = 'refunded'`.

---

### UC2 — Operations: Delivery Performance

**Goal**: Operations team needs to monitor logistics service quality.

**Required outputs**:
- Delivery success / failure rate by day and by carrier (GHN vs GHTK)
- Average delivery time from pickup to successful delivery
- Rate of re-delivery attempts (delivery_attempts > 1)
- Distribution of delivery failure reasons (wrong_address, not_at_home, etc.)
- Return rate by product category and by sales channel

**Sample business questions**:
> - "Does GHN or GHTK have a higher delivery success rate in June?"
> - "Which province has the highest failed delivery rate?"
> - "Which product category has the highest return rate?"

> **Note**: Carrier, delivery time, delivery attempts, and failure reasons are **only available from the Logistics API** — not from the database. This use case requires joining PostgreSQL and API data.

> **Sources**: PostgreSQL + API
> **Key tables**: `orders` + API shipments (LEFT JOIN on `order_id`)
> **Design consideration**: ~8% of orders will have no shipment record (cancelled before pickup) — your model must handle NULL carrier/delivery fields gracefully.

---

### UC3 — Customer & Marketing: Customer Behavior

**Goal**: Marketing team needs to understand purchase behavior to optimize campaigns and loyalty programs.

**Required outputs**:
- Customer segmentation by loyalty tier and actual spending
- History of loyalty tier changes per customer
- Effectiveness of each voucher / campaign (usage count, revenue generated, discount cost)
- Customer lifecycle analysis: first order, repeat purchase, inactive
- Product ratings by category and by sales channel

**Sample business questions**:
> - "How much do Platinum customers spend compared to Bronze in June?"
> - "How much revenue did the FLASHSALE campaign generate, and what was its discount cost?"
> - "What is the repeat purchase rate within 30 days?"

> **Note**: Loyalty tier history — the source system (`customers` table) only stores the **current** tier. Historical changes can only be captured from the day the pipeline goes live. Backfilling historical tier changes is not possible.

> **Sources**: PostgreSQL only
> **Key tables**: `customers`, `orders`, `order_items`, `vouchers`, `product_reviews`
> **Design consideration**: `loyalty_tier` only stores current state — SCD Type 2 is needed to track historical tier changes, but history can only be captured from pipeline go-live date. Voucher effectiveness requires joining `vouchers` → `orders` → `order_items`.

---

### UC4 — Inventory & Product: Stock & Product Performance

**Goal**: Product team needs to track inventory status and sales performance by channel.

**Required outputs**:
- End-of-day stock quantity per product
- Best-selling and slowest-selling products by category
- Products currently selling at a loss (cost_price > actual sale price)
- Sales volume comparison for the same product: marketplace vs direct channel
- Sell-through rate to forecast stockout dates

**Sample business questions**:
> - "Which products are currently selling at a loss?"
> - "Does TikTok sell Thời trang category faster or slower than direct channel?"
> - "At the current sell-through rate, which products will run out of stock within 7 days?"

> **Note**: The `inventory` table only stores current stock. End-of-day stock for past dates must be reconstructed from `inventory_transactions`. Direct vs marketplace comparison requires joining PostgreSQL and SFTP data at the product level.

> **Sources**: PostgreSQL + SFTP
> **Key tables**: `products`, `inventory`, `inventory_transactions`, `order_items` + SFTP CSV files
> **Design consideration**: `inventory` must be snapshotted daily — the table only reflects current state. Marketplace vs direct comparison joins at `product_id` level only — there is no order-level join between PostgreSQL and SFTP.

---

## 3. Functional Requirements

### 3.1 Pipeline Coverage

The pipeline must ingest data from all three sources:

| Source | Type | What to extract |
|--------|------|----------------|
| PostgreSQL `shopvn` | OLTP Database | All 9 tables |
| REST API `localhost:8000` | Logistics API | Shipment data per order |
| SFTP `localhost:2222` | File server | Daily marketplace CSV files |

### 3.2 Data Transformation

- Transform raw source data into analytics-ready tables that can answer all four use cases
- Handle all intentional data quality issues in source data (see Section 5)
- Apply business logic: revenue calculation, settlement lag, inventory reconstruction

### 3.3 Data Loading

- Load transformed data into a DWH or Lakehouse (your choice of stack)
- All loads must be **idempotent**: running the same pipeline for the same date range multiple times must produce identical results — no duplication, no data loss.

### 3.4 Orchestration

- Pipeline must be orchestrated with **Apache Airflow**
- DAGs must have: retry logic, SLA alerts, task dependencies, and catchup=False. Please configure the value with a valid reason.
- Backfill for any historical date range must work safely

### 3.5 Data Quality

- Automated quality checks must run **before** data is loaded into the analytics layer
- If a quality check fails, the load must be blocked and an alert must be triggered
- Minimum checks required: row count reconciliation, amount reconciliation, null checks on key fields, SFTP checksum verification

### 3.6 Security

- Customer PII (`phone`, `email`,..) must be **masked** before loading into the analytics layer or specify the permission for each role that could access the data.
- All credentials (DB passwords, API keys, SFTP passwords) must be managed via environment variables or secret managers — never hardcoded in code
- Source access is read-only — no writes to source systems

---

## 4. Non-Functional Requirements

### 4.1 Data Freshness (SLA)

| Requirement | Target |
|-------------|--------|
| Data available for all teams | Before **8:00 AM** every day |
| Flash sale days (volume ×3) | Still meet 8:00 AM SLA |
| Pipeline failure recovery | Data available within **2 hours** of incident detection |

### 4.2 Reliability & Idempotency

- Rerunning the pipeline for the same date produces the same result (determinism)
- A mid-run failure followed by a full rerun must not corrupt already-loaded data (atomicity)
- Backfilling any date range in the past must produce correct results

### 4.3 Schema Evolution

Source schemas may change without notice. The pipeline must handle:

| Scenario | Requirement |
|----------|-------------|
| New column added to source | Pipeline must not crash. You must document the strategy (ignore or propagate). |
| Column type changed | Pipeline must detect type mismatch and not silently corrupt data |
| New SFTP partner added | Pipeline must handle new partners without requiring a full rewrite |

### 4.4 Volume Spike Handling

Flash sale days (Jun 7 and Jun 15) have ~3× normal volume:

| Metric | Normal day | Flash sale day |
|--------|-----------|---------------|
| Orders | 5,000–7,000 | 15,000–18,000 |
| SFTP file size (Shopee) | ~400 KB | ~12–18 MB |
| API batch calls needed | ~140 | ~360 |

The pipeline must handle large files via streaming (not loading entirely into memory) and must not be blocked by API rate limits.

### 4.5 Observability

After the pipeline runs, an on-call engineer must be able to answer the following questions **using only dashboards and logs** — without SSH access to servers:

- What time did the pipeline finish today?
- Is the entire system operating stably? Are there any components or services experiencing disruptions?
- Does today's revenue match the source?

### 4.6 Incident Response

When the pipeline fails at 3 AM, the you must have a **runbook** to:

1. Identify which step failed (extract / transform / load / quality check)
2. Assess the impact: which data is affected and for which date range
3. Fix and rerun safely without duplicating data
4. Confirm data correctness after rerun via reconciliation
5. Document the root cause and prevention steps

---

## 5. Source-Specific Requirements

### 5.1 PostgreSQL — Data Quality Handling

These issues exist intentionally in the source. The pipeline must handle all of them:

| Issue | Location | Rate | Required handling | Why |
|-------|----------|------|------------------|-----|
| `phone IS NULL` | `customers` | ~0.6% | Handle nullable, do not crash | Valid — some customers don't provide a phone. One NULL should not crash your entire pipeline. |
| `shipping_fee = 0` (no freeship voucher) | `orders` | ~2% | Flag as anomaly, continue | Order is valid, just anomalous. Do not reject. Flag it so the analytics team can investigate. |
| `discount_amount > subtotal` (flash sale bug) | `orders` | ~0.3% | Flag and exclude from revenue — do not treat as zero-revenue order | The source system already clamps `total_amount` to `0` for these rows — the value is never negative in the DB. However, `total_amount = 0` here is a bug, not a free order. Flag these rows and exclude them from revenue aggregations. **Document your decision**. |
| `quantity = 0` (legacy edge case) | `order_items` | ~1% | Flag or filter, document decision | `total_price = unit_price × 0 = 0`. Contributes nothing to revenue but corrupts "units sold" metrics. Filter it out or exclude from quantity aggregations — **document your decision**. |
| `cost_price > base_price` (data entry error) | `products` | 50 rows | Flag — product selling at a loss | Not a pipeline error — it's a business insight. Load normally but add an `is_loss_making` flag so Finance and Product teams can act on it. |

### 5.2 REST API — Failure Handling

| Error | Rate | Required handling |
|-------|------|------------------|
| HTTP 429 — Rate limit | >100 req/min | Wait `retry_after` seconds, then retry |
| Timeout (35s server sleep) | 5% of requests | Retry up to 3×, exponential backoff: 1s → 2s → 4s |
| HTTP 404 — Not found | ~8% of order IDs | Skip and log. Do not retry. |
| HTTP 500 — Server error | 1% of requests | Retry once. If still failing, skip and log. |
| HTTP 400 — Batch too large | >50 order IDs | Fix batch size (client-side bug) |

The pipeline must chunk API calls into batches of ≤50 order IDs.

### 5.3 SFTP — File Failure Handling

Each CSV file has a companion `.md5` checksum file. Checksum verification is **mandatory** before processing.

| Failure | Count | Required handling |
|---------|-------|------------------|
| Missing file | 3 days × 1 partner | Log warning, skip file, continue with remaining partners |
| Corrupt file (checksum mismatch) | 2 days × 1 partner | Reject file, alert, do not process |
| Late arrival | 2 days × 1 partner | ⚠️ Conceptual only — see note below |

> **Note on Late Arrival**: Files are pre-baked into the Docker image and always available immediately. The `LATE` entries in `FAILURE_MANIFEST.txt` document a real-world scenario your pipeline must be designed to handle (partial run + sensor + idempotent rerun), but the scenario cannot be triggered live in this environment. Design your pipeline to support it; test it conceptually in your runbook.

Large files (flash sale days up to 18 MB) must be **stream-read in chunks** — not loaded entirely into memory.

---

## 6. Architecture Options

You can choose **one** of two stacks. The choice must be justified in the project presentation.

### Option A — SQL Data Warehouse (DuckDB)

```
PostgreSQL / API / SFTP
    └── Airflow DAGs
        └── SQL transforms (MERGE / UPSERT)
            └── DuckDB (local DWH)
                └── Analytics queries
```

Suitable when: data volume fits on a single machine, fast iteration is preferred, no cloud dependency needed. DuckDB runs in-process, requires zero server setup, and supports standard SQL with excellent analytical performance.

### Option B — Lakehouse

```
PostgreSQL / API / SFTP
    └── Airflow DAGs
        └── PySpark jobs
            └── Delta Lake / Apache Iceberg (object storage)
                └── Analytics queries
```

Suitable when: large data volume, need for time travel, ACID writes on partitioned data.

Both options require the same **orchestration**, **Quality checks & Reconciliation**, **monitoring**, and **security controls**.

---

## 7. Deliverables

| Deliverable | Description |
|-------------|-------------|
| Working pipeline | Runs end-to-end with all 3 data sources |
| Data model diagram | Shows all analytics tables and their relationships |
| Architecture diagram | Full data flow from source to analytics layer |
| Airflow DAGs | With dependency, retry, SLA monitoring |
| Data quality suite | Automated validation before each load |
| Monitoring dashboard | Prometheus + Grafana or equivalent |
| CI/CD configuration | Automated test and deploy on code change |
| Runbook | Incident handling for at least 3 failure scenarios |
| Demo presentation | Design decisions and technical walkthrough |

---

## 8. Evaluation Criteria

The project is evaluated on the following criteria. **Stack choice (DWH vs Lakehouse) does not affect the score** — what matters is that the choice is justified.

| Criterion | Weight | Pass condition |
|-----------|--------|---------------|
| Pipeline runs end-to-end with all 3 sources | 20% | Loads data from PostgreSQL + API + SFTP |
| Idempotency + reconciliation | 20% | Rerun 3× produces identical count and sum |
| Failure handling | 20% | Handles all API errors + SFTP failures + DQ issues |
| Schema evolution strategy | 10% | Documented approach, pipeline does not crash |
| Monitoring & observability | 15% | Dashboard answers the 5 questions in Section 4.5 |
| Security (PII + secrets) | 10% | phone/email masked, no hardcoded credentials |
| Runbook & incident response | 5% | Runbook covers at least 3 scenarios |

---

## 9. Nice to Have

Not required but will strengthen the portfolio:

| Feature | Description |
|---------|-------------|
| Real-time / streaming | Process order events near real-time instead of daily batch |
| BI Dashboard | Connect analytics layer to Grafana / Superset / Metabase |
| Data Lineage | Trace data from source through each transformation step |
| Data Catalog | Auto-document schema and metadata of analytics tables |
| LLM-assisted RCA | Use an LLM agent to auto-investigate pipeline failures |
| Multi-source reconciliation | Auto-reconcile revenue across direct + marketplace + logistics |

---

*Questions? Reach out to me :))*
