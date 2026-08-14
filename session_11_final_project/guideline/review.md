# Final review theo yêu cầu và tiêu chí chấm điểm

## 1. Đối chiếu functional requirements

| Yêu cầu | Reference coverage | Trạng thái bài thật |
|---|---|---|
| PostgreSQL đủ 9 tables | `postgres_bronze.py` | Chưa chạy |
| Logistics API | `api_bronze.py` | Chưa smoke-test endpoint |
| SFTP + mandatory MD5 | `sftp_bronze.py` | Chưa chạy |
| Analytics cho 4 use cases | 8 Gold facts + SCD2 + analytics SQL | Chưa publish |
| Idempotent load | Iceberg MERGE theo source/candidate key | Chưa test rerun 3 lần |
| Airflow | DAG dependency/retry/SLA/alert/manual backfill | Chưa deploy |
| Pre-load DQ | work candidate → blocking validator → Gold | Code reference có |
| PII/secrets | Silver hashing + env placeholders | Chưa audit runtime |

## 2. Đối chiếu intentional failures

| Failure | Expected behavior trong reference |
|---|---|
| Phone NULL | Giữ hợp lệ, hash NULL thành NULL, flag missing |
| Shipping fee 0 | Flag, không reject |
| Discount > subtotal | Flag và loại khỏi eligible revenue |
| Quantity 0 | Giữ trace, eligible units/revenue bằng 0 |
| Cost > base | Flag loss-making |
| API 429 | Chờ retry-after |
| API timeout | Retry 1/2/4 giây |
| API 404 | Skip/log, không retry |
| API 500 | Retry một lần rồi audit/fail |
| API >50 IDs | Client validate và fail |
| SFTP missing | Manifest warning, tiếp tục source khác, block completeness trước Gold |
| SFTP corrupt | Không load file, alert/fail |
| SFTP late | Rerun date idempotently khi file tới |

## 3. Đối chiếu rubric

| Criterion | Weight | Reference evidence | Kết luận hiện tại |
|---|---:|---|---|
| End-to-end 3 sources | 20% | 3 ingestion jobs + DAG | Chưa chứng minh runtime |
| Idempotency/reconciliation | 20% | MERGE, DQ checks, SQL test | Chưa có log rerun 3 lần |
| Failure handling | 20% | Retry matrix, manifest, DQ flags | Unit/static reference |
| Schema evolution | 10% | Add column; block type change | Chưa integration-test |
| Monitoring | 15% | Audit tables + Streamlit + Airflow UI | Chưa chạy dashboard |
| Security | 10% | Hash PII, env-only secrets, read-only source | Cần secret scan runtime |
| Runbook | 5% | Ba kịch bản trong packet 5 | Đã có reference |

## 4. Deliverables

- Working pipeline: reference code đủ file, nhưng chưa phải working evidence.
- Data model diagram: logical table/grain đã mô tả; cần vẽ diagram nộp bài.
- Architecture diagram: có trong README và HTML.
- Airflow DAG: có full reference.
- Data quality suite: có Silver checks và pre-Gold reconciliation.
- Monitoring dashboard: có full Streamlit reference.
- CI/CD: có GitHub Actions reference.
- Runbook: có ba failure scenarios.
- Demo presentation: **chưa có**, cần làm sau khi có screenshots và runtime evidence.

## 5. Test matrix bắt buộc trước khi đánh dấu hoàn thành

- [ ] Source Compose có ba container healthy.
- [ ] API batch endpoint và response shape khớp dataset guide.
- [ ] Polaris catalog tồn tại sau restart.
- [ ] Spark và Trino đọc cùng Iceberg table.
- [ ] June 1 chạy end-to-end PASS.
- [ ] June 7 xử lý volume x3 và vẫn trước SLA.
- [ ] June 9 corrupt checksum block Gold.
- [ ] June 4 missing partner được audit và block completeness.
- [ ] Một timeout, 429 và 500 được quan sát trong log/test.
- [ ] Cùng date rerun ba lần: count/sum giống nhau.
- [ ] Discount anomaly và zero quantity không contaminate metric.
- [ ] Silver/Gold không có plaintext phone/email/full name.
- [ ] Dashboard trả lời các câu hỏi vận hành trong Section 4.5.
- [ ] Recovery drill hoàn tất trong hai giờ.

## 6. Kết luận cuối

Bộ `guideline/` hiện **phủ đầy đủ về reference implementation và hướng dẫn** cho
các phần thiếu của homework. Homework thực tế vẫn là **partially covered** vì chưa
copy code vào pipeline thật, chưa chạy Docker integration, chưa có runtime evidence,
data-model diagram hoàn chỉnh và demo presentation. Không nên tuyên bố bài hoàn
thành end-to-end trước khi toàn bộ checkbox ở trên PASS.
