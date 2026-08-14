# Packet 2 — Silver transformation và business DQ

## Phần còn thiếu

Project thật chưa có typed/business-ready layer. Full reference nằm tại
`reference_code/jobs/bronze_to_silver.py`.

Job tạo toàn bộ output trong memory, chạy structural checks trước rồi mới MERGE
từng Silver table. Đây là ranh giới xử lý business DQ, masking và eligibility.

## Quy tắc đã triển khai

| Input | Silver behavior |
|---|---|
| customers | Hash `full_name`, `phone`, `email` bằng salt; phone NULL vẫn hợp lệ |
| products | Cast giá; flag `is_loss_making` khi cost > base |
| orders | Flag zero shipping/discount anomaly; tính refunded và eligible revenue |
| order_items | Giữ row quantity 0 nhưng eligible units/revenue bằng 0 |
| vouchers | Chuẩn hóa type/code; flag usage vượt giới hạn |
| inventory | Cast stock; flag negative; giữ current state |
| transactions | Chuẩn hóa movement để reconstruct EOD |
| returns | Chỉ `refunded` mới được trừ revenue |
| reviews | Rating phải nằm trong 1–5; comment được phép NULL |
| API | Parse timestamp; missing shipment được biểu diễn bằng LEFT JOIN ở Gold |
| marketplace | Cast CSV, dedup bản giao lại, kiểm formula net revenue |

## Blocking và non-blocking

Blocking: key NULL/duplicate, cast bắt buộc thành NULL, invalid enum, API delivery
trước shipped time, marketplace formula mismatch và schema breaking change.

Non-blocking: phone NULL, free shipping không voucher, quantity 0, loss-making,
negative inventory flag và trạng thái marketplace chưa completed. Những dòng này
vẫn trace được nhưng không làm bẩn eligible metric.

## PII

`PII_HASH_SALT` phải là secret ổn định. Không rotate salt nếu chưa có migration plan,
vì toàn bộ hash sẽ thay đổi. Silver không select plaintext `full_name`, `phone`,
`email`; Bronze cần quyền truy cập hạn chế riêng.

## Cách adapt

Copy job vào `pipeline/jobs/bronze_to_silver.py`. Trước khi chạy production, xác
nhận assumption direct revenue trong README. Nếu Finance dùng revenue recognition
khác (ví dụ chỉ delivered), thay duy nhất biểu thức `is_revenue_eligible` và thêm
test/reconciliation tương ứng.

## Gate hoàn thành

- Silver chứa 11 business tables và không chứa plaintext PII.
- Mọi business key unique.
- Discount anomaly không đóng góp revenue; zero quantity không đóng góp units.
- Marketplace calculated net revenue khớp source.
- Job rerun không tăng count ngoài thay đổi source hợp lệ.
