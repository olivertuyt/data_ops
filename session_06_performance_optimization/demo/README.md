# Demos Session

Both demos run against the silver Delta tables (run `scripts/generate_data.py` and
`scripts/setup_delta_tables.py` first — see the session README).

---

## Lakehouse schema

All tables live under `s3a://silver/` as Delta format.

### `silver/orders`

| Column | Type | Description |
|---|---|---|
| `order_id` | `string` | Unique order identifier, e.g. `ord_000000001` |
| `customer_id` | `string` | Foreign key to `customers`, e.g. `cust_00000001` (30% of rows share hot customer `cust_00000001`) |
| `order_status` | `string` | One of: `delivered` (~3/7), `shipped`, `processing`, `cancelled`, `unavailable` |
| `order_purchase_timestamp` | `timestamp` | When the customer placed the order (range: 2026-07-01 – 2026-07-21) |
| `order_approved_at` | `timestamp` | Approval time (0–48 h after purchase) |
| `order_delivered_timestamp` | `timestamp` | Delivery time (`null` for non-delivered orders) |
| `order_estimated_delivery_date` | `date` | Estimated delivery (purchase date + 10 days) |
| `order_channel` | `string` | One of: `web`, `mobile_app`, `marketplace` |
| `device_type` | `string` | One of: `desktop`, `ios`, `android` |
| `coupon_code` | `string` | Uppercase hex coupon code (~20% of orders; `null` otherwise) |
| `customer_note` | `string` | 48-char hex freetext note |
| `order_date` | `date` | Derived from `order_purchase_timestamp`; partition column |

### `silver/order_items`

| Column | Type | Description |
|---|---|---|
| `order_id` | `string` | Foreign key to `orders` |
| `order_date` | `date` | Matches `order_date` of the parent order; partition column |
| `order_item_id` | `int` | Line-item sequence within the order (1–5) |
| `product_id` | `string` | Product identifier, e.g. `prod_00001` |
| `seller_id` | `string` | Seller identifier, e.g. `seller_00001` (hot seller in ~35% of rows) |
| `product_category` | `string` | One of: `electronics`, `furniture`, `toys`, `health_beauty`, `sports`, `auto`, `books`, `garden` |
| `product_name` | `string` | Category name + 12-char hex suffix |
| `product_weight_g` | `int` | Product weight in grams (50–30,050) |
| `product_length_cm` | `int` | Product length in cm (5–105) |
| `product_height_cm` | `int` | Product height in cm (2–102) |
| `product_width_cm` | `int` | Product width in cm (5–105) |
| `price` | `double` | Item price, rounded to 2 dp (range 5–1,000) |
| `shipping_charges` | `double` | Shipping fee, rounded to 2 dp (range 2–50) |

### `silver/payments` *(no partition; 8 files)*

| Column | Type | Description |
|---|---|---|
| `order_id` | `string` | Foreign key to `orders` |
| `payment_sequential` | `int` | Payment sequence within the order (1–3; one order can have multiple payments) |
| `payment_type` | `string` | One of: `credit_card`, `debit_card`, `voucher`, `boleto` |
| `payment_installments` | `int` | Number of installments (1–12 for `credit_card`; always 1 for others) |
| `payment_value` | `double` | Payment amount, rounded to 2 dp (range 10–500) |
| `payment_provider` | `string` | One of: `cielo`, `stone`, `pagseguro`, `adyen` |
| `card_brand` | `string` | One of: `visa`, `mastercard`, `elo`, `amex` (`null` for non-credit-card payments) |
| `authorization_code` | `string` | 16-char uppercase hex authorization code |

### `silver/customers`

| Column | Type | Description |
|---|---|---|
| `customer_id` | `string` | Unique customer identifier, e.g. `cust_00000001` |
| `customer_name` | `string` | First + last name drawn from fixed Brazilian name lists |
| `customer_zip_code_prefix` | `string` | 5-digit zip prefix (1000–91000) |
| `customer_city` | `string` | One of: `sao paulo`, `rio de janeiro`, `belo horizonte`, `curitiba`, `porto alegre`, `salvador` |
| `customer_state` | `string` | One of: `SP`, `RJ`, `MG`, `PR`, `RS`, `BA` |
| `customer_street` | `string` | Street name + 12-char hex suffix |
| `customer_number` | `int` | Street number (1–9,999) |
| `customer_neighborhood` | `string` | One of: `centro`, `jardins`, `vila nova`, `boa vista`, `santa cruz`, `liberdade` |
| `customer_phone` | `string` | Brazilian mobile number with `+55` prefix |
| `customer_email` | `string` | 10-char hex handle + `@example.com` |

---

## Demo 1 — one slow task holds the whole job

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo1_pipeline.py
```

Walkthrough on http://localhost:18080:
1. Open the completed app → **Stages** → the final join stage.
2. **Summary Metrics**: compare Max vs Median task duration, and the Shuffle Read
   spread across tasks.
3. Re-run, open both apps side by side in the History Server, compare stage duration.

Note: the History Server persists every run — the before/after comparison works even
after the driver exits.

## Demo 2 — 200 tasks for a tiny batch

```bash
docker exec s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo2_pipeline.py

docker exec -e AQE=true s06-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 --py-files /scripts/spark_common.py /demo/demo2_pipeline.py
```
