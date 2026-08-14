# Packet 1 — Bronze raw ingestion

## Phần còn thiếu và lý do quan trọng

Source project chưa có ingestion code. Nếu Bronze thực hiện cast, filter hoặc gắn
business anomaly ngay khi đọc nguồn thì không còn khả năng replay và điều tra dữ
liệu gốc. Vì vậy Bronze trong reference chỉ giữ field nguồn và metadata kỹ thuật.

## Reference code

| File | Mục đích |
|---|---|
| `reference_code/common/config.py` | Đọc secret bắt buộc, validate date/run ID |
| `reference_code/common/spark_session.py` | Spark + Polaris REST Catalog |
| `reference_code/common/iceberg.py` | Schema check, additive evolution, MERGE |
| `reference_code/common/audit.py` | Audit theo run/stage/object/date |
| `reference_code/jobs/postgres_bronze.py` | Đọc đủ 9 PostgreSQL tables |
| `reference_code/jobs/api_bronze.py` | API batch tối đa 50, retry và NDJSON landing |
| `reference_code/jobs/sftp_bronze.py` | Stream download, `.part` → `.ready`, MD5 |

Các file trên là full code; không cần ghép snippet từ tài liệu này.

## Contract chi tiết

### PostgreSQL

- Tài khoản nguồn chỉ có quyền SELECT.
- Đọc đủ `customers`, `products`, `orders`, `order_items`, `vouchers`,
  `inventory`, `inventory_transactions`, `returns`, `product_reviews`.
- MERGE bằng source PK; `order_items` dùng `item_id`, không dùng composite key tự tạo.
- Không loại phone NULL, quantity 0 hoặc discount anomaly ở Bronze.
- Column mới được thêm vào Bronze; type change làm job fail để tránh silent corruption.

Reference đang full-scan chín bảng vì volume homework nhỏ. Khi đưa ra production,
phải thay bằng CDC/watermark dựa trên source contract; không tự thêm watermark từ
`created_at` vì update trạng thái cũ và refund có thể đến muộn.

### Logistics API

- Contract ưu tiên theo `02_dataset_guide.md`:
  `GET /v1/shipments?order_ids=id1,id2,...`.
- Batch không vượt 50 order IDs; iterator không collect toàn bộ order lên driver.
- 429 chờ `Retry-After`/`retry_after`; timeout retry 1/2/4 giây; 500 retry một lần;
  404 ghi nhận là không có shipment và không retry; 400/401 block ngay.
- Exhausted timeout/500 được ghi vào `audit.api_errors` và block downstream.
- Mỗi response giữ `_raw_payload` để trace source.

HTML cũ có chỗ nói `POST /v1/shipments/batch`; phải smoke-test container trước khi
chốt. Không sửa client theo suy đoán.

### SFTP

- Download theo chunk 1 MiB vào `.part`.
- Tính MD5 trong lúc stream, so với companion `.md5`, chỉ rename `.ready` khi hợp lệ.
- MISSING ghi warning/manifest và tiếp tục partner khác.
- CORRUPT không được đọc vào Bronze; job fail sau khi các file VALID đã được merge
  idempotently để alert và chặn Silver/Gold.
- CSV field giữ kiểu string ở Bronze. Cast và formula check nằm ở Silver.

## Các bước chạy thử

```powershell
docker compose up -d
docker compose ps

Set-Location pipeline
docker compose --env-file .env config
docker compose up -d platform-db minio setup-bucket polaris polaris-setup trino
docker compose build airflow-scheduler
docker compose up -d airflow-init airflow-webserver airflow-scheduler
```

Trigger một date khỏe trước, ví dụ `2026-06-01`. Sau đó test:

- `2026-06-07`: flash-sale volume;
- `2026-06-09`: corrupt TikTok file phải block;
- `2026-06-04`: missing TikTok phải ghi MISSING và candidate completeness phải block.

## Gate hoàn thành

- Ba lần chạy cùng business date tạo count và key count giống nhau.
- Không có plaintext PII ngoài Bronze.
- Không có record từ file checksum sai.
- API audit phân biệt rõ not-found hợp lệ và technical failure.
- Iceberg schema/type check fail trước khi corrupt target.
