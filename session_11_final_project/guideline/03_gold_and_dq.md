# Packet 3 — Gold candidates, reconciliation và publish

## Nguyên tắc bắt buộc

Gold là analytics-serving layer cuối cùng, do đó không được ghi Gold rồi mới chạy
blocking DQ. Reference tách ba executable jobs:

1. `build_gold_candidates.py` ghi `polaris.work.*_candidate` theo `run_id`.
2. `validate_gold_candidates.py` ghi toàn bộ kết quả vào
   `polaris.audit.data_quality_results` và raise nếu có blocking failure.
3. `publish_gold.py` kiểm lại DQ PASS trước khi MERGE Gold.

Candidate FAIL được giữ lại để điều tra; analytics không được cấp quyền vào schema
`work`.

## Gold tables được cung cấp

| Bảng | Grain | Use case |
|---|---|---|
| `fact_daily_revenue` | date/channel/basis | Earned revenue và marketplace cash received T+15 |
| `fact_customer_daily` | date/customer | Daily/monthly spend, top customers, lifecycle |
| `fact_delivery_daily` | date/carrier/province/reason | Success, failure, attempts, duration |
| `fact_voucher_daily` | date/voucher | Campaign usage, revenue, discount cost |
| `fact_return_daily` | date/category/channel | Return rate |
| `fact_product_rating_daily` | date/category/direct channel | Rating theo source có thật |
| `fact_inventory_eod` | date/product/warehouse | Historical EOD stock |
| `fact_product_channel_daily` | date/product/channel | Units, revenue, channel comparison |
| `dim_customer_scd2` | customer/version | Loyalty tier history từ go-live |

Rating marketplace không tồn tại trong ba source nên không được fabricate.

## Blocking checks

- Required key và duplicate theo đúng grain.
- Direct và marketplace net revenue reconcile từ Silver sang candidate.
- Customer spend reconcile với direct revenue.
- LEFT JOIN shipments không làm mất direct orders.
- Không có negative Finance metric.
- Inventory snapshot có đủ date × inventory key.
- Mỗi date có đủ ba SFTP partner và status VALID trước khi Finance/Product publish.

SFTP MISSING vẫn cho ingestion tiếp tục nhưng gate chặn Gold của window đó. Khi file
đến muộn, rerun cùng date; MERGE đảm bảo không duplicate.

## SCD Type 2

`publish_gold.py` chỉ tạo lịch sử từ ngày pipeline chạy. Khi tier thay đổi, version
cũ được đóng ở `effective_date - 1` và version mới mở. Không backfill tier trước
go-live vì source chỉ có current state.

## Atomicity và rollback

Mỗi Iceberg MERGE là atomic ở cấp table. Nhiều Gold tables không nằm trong một
distributed transaction; nếu publish fail giữa chừng, rerun cùng date/run sẽ hoàn
tất idempotently. Trước production, nếu yêu cầu cross-table atomic visibility,
cần thêm published-run serving views; homework hiện không cung cấp contract đó.

Rollback một table bằng Iceberg snapshot rollback sau khi xác định snapshot trước
run lỗi. Không xóa Bronze hoặc sửa source để “fix” số liệu.

## Gate hoàn thành

- Cố ý tạo duplicate candidate: publish không chạy.
- Cố ý sửa amount: reconciliation fail và Gold snapshot không đổi.
- Run PASS tạo đủ Gold tables; rerun ba lần giữ count/sum giống nhau.
- Query trong `reference_code/sql/analytics_queries.sql` trả lời được các business
  questions có source support.
