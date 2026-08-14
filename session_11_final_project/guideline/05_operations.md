# Packet 5 — Test, quan sát hệ thống và incident response

## Test và CI/CD

Reference gồm:

- `tests/test_config.py`: validate run ID/date window;
- `tests/test_api_contract.py`: batch limit, 404, 429 và 500 retry;
- `tests/reconciliation.sql`: idempotency và publish-gate queries;
- `.github/workflows/ci.yml`: compile, pytest và Compose validation.

CI mới chứng minh static/unit contract. End-to-end test phải chạy trong local stack
với source containers thật.

## Dashboard

`reference_code/monitoring/dashboard.py` dùng Trino để trả lời:

- pipeline thành công gần nhất kết thúc lúc nào;
- stage/service nào đang FAIL;
- blocking DQ/reconciliation nào không khớp;
- dữ liệu revenue đã mới tới ngày nào;
- SFTP file nào missing/corrupt và checksum ra sao.

Chạy tham khảo:

```powershell
$env:TRINO_HOST='localhost'
$env:TRINO_PORT='8080'
streamlit run monitoring\dashboard.py
```

Airflow UI bổ sung task duration, retry, SLA miss và log URL. Dashboard không thay
thế alert; DAG callback gửi email khi `SHOPVN_ALERT_EMAIL` được cấu hình.

## Runbook 1 — API timeout/500 lúc 03:00

1. Mở Airflow task log và xác định `run_id`, batch order IDs, error type.
2. Kiểm tra API health và `audit.api_errors`; không retry 404.
3. Xác nhận `publish_gold` chưa chạy.
4. Khi API ổn định, clear từ `bronze_api`; PostgreSQL/SFTP MERGE có thể rerun an toàn.
5. Kiểm tra shipment count, missing ratio và delivery reconciliation.
6. Ghi RCA: duration, batch, carrier/API behavior và preventive action.

## Runbook 2 — SFTP corrupt hoặc missing

1. Query `audit.source_manifests` theo date/partner.
2. CORRUPT: giữ file ngoài Bronze, liên hệ partner, không bypass MD5.
3. MISSING: các partner khác vẫn ingest; Gold window bị completeness gate chặn.
4. Khi file hợp lệ tới, rerun cùng date. `.ready` + row hash + MERGE ngăn duplicate.
5. Reconcile marketplace net revenue và expected partner count trước publish.

## Runbook 3 — Pre-Gold DQ/reconciliation fail

1. Query `audit.data_quality_results` và candidate theo `run_id`.
2. So sánh source/Silver/candidate count và amount; không sửa trực tiếp Gold.
3. Fix transform hoặc source contract; tạo lại candidate.
4. Chạy validator. Chỉ PASS mới chạy publish.
5. So sánh Iceberg snapshot trước/sau và chạy `tests/reconciliation.sql`.

## Failure recovery và rollback

- Mục tiêu recovery là trong hai giờ từ lúc phát hiện.
- Rerun đúng date range, giữ `max_active_runs=1`.
- Nếu Gold code bug đã publish, xác định snapshot ID và rollback đúng table; sau đó
  rerun candidate/DQ/publish. Không dùng truncate toàn bộ lakehouse.
- Ghi incident: timeline, impact dates/tables, root cause, correction, prevention và
  người xác nhận reconciliation.
