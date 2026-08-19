# Architecture Decision Record: LLM Observability at 1B Requests/Day

**Author:** Lakehouse Architect On-Call  
**Topic:** A — High-Scale LLM Observability & FinOps Tiering  
**Target:** 1B requests/day, ~5 KB/request (~5 TB/day raw)  
**Hard cap:** ≤ $5,000/month for storage and data-platform compute

---

## 1. Problem Statement

Hệ thống foundation-model API phục vụ trung bình 11.600 req/s, peak 30.000 req/s. Mỗi request tạo khoảng 5 KB gồm prompt, completion, token counts, latency và tenant metadata. Dashboard phải cập nhật p50/p95/p99, token cost và error rate theo tenant mỗi 5 phút, với ad-hoc query p95 < 1,5 giây. Prompt/response đầy đủ được giữ đúng 7 ngày để điều tra incident; sau đó chỉ giữ aggregates trong một năm. PII phải được redact hoặc tokenize trước khi analyst hay engineer có thể đọc. Kiến trúc phải chịu được retry, late data, schema drift và small-file explosion, đồng thời giữ tổng chi phí dưới $5.000/tháng.

---

## 2. Architecture Diagram

```text
 [1B req/day API gateways]
             │ batched HTTP/gRPC
             ▼
 [Kafka/Redpanda: tenant hash, RF=3, retention=6h]
             │ 1-minute micro-batch
             ▼
 ┌──────────────────────────────────────────┐
 │ BRONZE — restricted raw Delta            │
 │ date/hour; encrypted; retention 24h      │
 │ only ingestion + privacy service can read│
 └────────────────────┬─────────────────────┘
                      │ redact/tokenize + quality checks
                      ▼
 ┌──────────────────────────────────────────┐
 │ SILVER — curated full payload            │
 │ date/hour; cluster tenant_id + ts        │
 │ daily encryption key; retention 7 days   │
 └────────────────────┬─────────────────────┘
                      │ 5-minute window aggregation
                      ▼
 ┌──────────────────────────────────────────┐
 │ GOLD — tenant metrics Delta              │
 │ partition date; cluster tenant_id        │
 │ aggregates retained 365 days             │
 └────────────────────┬─────────────────────┘
                      │ Trino/DuckDB SQL
                      ▼
             [FinOps & Ops dashboard]

 [Unity Catalog OSS: ownership, policy, audit and lineage for all tables]
```

Gold chỉ đọc Silver; không có đường tắt Kafka→Gold. Vì vậy mọi dữ liệu analyst nhìn thấy đều đã đi qua ranh giới privacy.

---

## 3. Key Decisions and Rejected Alternatives

### Quyết định 1: Table format — Delta Lake + CDF

- **Chọn:** Delta Lake 3.x cho ACID, MERGE, time travel và CDF phục vụ downstream audit/index consumers.
- **Loại Iceberg:** REST catalog và hidden partitioning tốt, nhưng workload này ưu tiên Delta streaming/CDF và một định dạng duy nhất trên hot path.
- **Loại raw Parquet + Hive:** không có transaction protocol, schema enforcement hoặc safe concurrent compaction.

### Quyết định 2: Ingestion — ưu tiên SLA rồi compact

- **Chọn:** ghi mỗi 60 giây. Với 5 TB/ngày chia 100 writer, file trung bình chỉ khoảng 35 MB; maintenance 2 giờ/lần compact thành 256 MB.
- **Loại single-event append:** sẽ tạo tới 1B object/ngày và phá vỡ cả chi phí PUT lẫn query planning.
- **Loại buffer đến 128 MB trước khi flush:** ở tải trung bình cần gần 4 phút và không còn đủ headroom cho SLA dashboard 5 phút.

### Quyết định 3: Partitioning và clustering

- **Chọn:** Bronze/Silver partition `(date, hour)`, Gold partition `date`; Z-Order Silver/Gold theo `(tenant_id, request_timestamp)`.
- **Loại partition theo `tenant_id`:** 50.000 tenant × ngày/giờ tạo cardinality và metadata quá lớn.
- **Loại chỉ partition theo thời gian mà không cluster:** query một tenant vẫn phải đọc gần như toàn bộ file trong time range.

### Quyết định 4: PII boundary — Bronze restricted, Silver tokenized

- **Chọn:** privacy service dùng deterministic HMAC cho join keys và Presidio/regex rules có version; analyst không có quyền đọc Bronze.
- **Loại query-time redaction:** dễ bị bypass và trả chi phí compute lặp lại ở mỗi query.
- **Loại LLM-based redaction:** chi phí, latency và false-negative rate không phù hợp 1B request/ngày.

### Quyết định 5: Lifecycle — table-aware delete + crypto-shred

- **Chọn:** prompt/response mã hóa bằng daily data key. Khi đủ 7 ngày, hủy key trước, sau đó DELETE/drop partition qua Delta và VACUUM theo retention an toàn. Snapshot cũ chỉ còn ciphertext không thể giải mã.
- **Loại S3 Lifecycle trên Delta data prefix:** xóa file vật lý ngoài transaction log sẽ làm hỏng snapshot.
- **Loại `VACUUM RETAIN 0 HOURS` thường trực:** có thể phá reader và streaming consumer đang chạy.

### Quyết định 6: Catalog — Unity Catalog OSS

- **Chọn:** một control plane cho ownership, row/column policy, audit và lineage; bảng vẫn là Delta trên object storage.
- **Loại AWS Glue-only:** tăng lock-in và vẫn cần ghép thêm governance/lineage.
- **Loại Hive Metastore:** tìm được bảng nhưng không đáp ứng policy enforcement và audit PII.

---

## 4. Failure Modes and 3:00 AM Runbook

| Failure mode | Detection | Root cause | Containment and recovery |
|---|---|---|---|
| **Commit conflict storm** | Conflict rate >1% hoặc ingestion lag >5 phút | Nhiều writer commit cùng partition | Tạm dừng consumer; một writer sở hữu mỗi partition; retry optimistic commit với backoff; resume từ checkpointed Kafka offset. Directory naming không thay thế transaction protocol. |
| **PII rule drift** | Canary scanner thấy PII trong Silver và tự động thu hồi quyền | Payload mới thoát regex/NER rules | Dùng time travel khoanh vùng version; vá rule; MERGE ghi đè trường rò rỉ và replay từ Bronze restricted. Hủy daily key của dữ liệu bị ảnh hưởng nếu cần hard containment. Không RESTORE toàn bảng vì sẽ làm mất dữ liệu sạch đến sau. |
| **Small-file explosion** | >10.000 files/partition hoặc planning p95 >5 giây | Compaction dừng 12 giờ | Cô lập hot partitions, tăng worker cho OPTIMIZE target 256 MB, throttle ingestion nếu metadata tiếp tục tăng, rồi backfill từng giờ. Deletion Vectors không thay thế compaction. |
| **Privacy deletion lag** | Key tuổi >7 ngày vẫn active hoặc CDF consumer lag >30 phút | Lifecycle scheduler/KMS job hỏng | Chặn quyền đọc payload, hủy key quá hạn trước, replay delete events từ CDF, rồi đối chiếu lakehouse với derived indexes. |

---

## 5. Back-of-the-Envelope FinOps Model

### Data volume assumptions

- Raw: `1B × 5 KB = 5 TB/day`.
- Snappy ratio modeled at 3,5× → 1,43 TB/day on disk.
- Bronze 24h: 1,43 TB; Silver 7 days: ~10 TB; Gold one year: 365 GB.
- Kafka 6h with RF=3: `5 TB × 0,25 × 3 = 3,75 TB` local NVMe.

| Item | Assumption | Monthly cost |
|---|---|---:|
| S3 Bronze + Silver | 11,43 TB × $0,023/GB-month | $263 |
| S3 Gold | 365 GB × $0,023/GB-month | $8 |
| S3 PUT | 100 writers × 1 PUT/minute | $22 |
| S3 GET | 2M GET/month | $1 |
| Kafka/Redpanda | 3 NVMe nodes, modeled $0,69/node-hour | $1.511 |
| Ingestion + tokenizer | 4 c6i.2xlarge Spot/Savings, $0,17/hour | $496 |
| Compaction + aggregation | 2 workers, 6 hours/day | $90 |
| Query engine | autoscaled Trino budget envelope | $600 |
| Catalog, KMS and monitoring | two small control-plane nodes + telemetry | $250 |
| Subtotal | | **$3.241** |
| Risk buffer | 20% | **$648** |
| **Total** | | **~$3.889/month** |

Giá compute là modeled rate cần xác nhận lại trước procurement. Hai biến nhạy nhất là NVMe nodes và query concurrency. Nếu forecast vượt $4.500, giảm Kafka retention trước; không giảm Silver dưới 7 ngày.

---

## 6. One-Week Shippable MVP

1. **Ngày 1–2:** ingest 100K synthetic traces vào Bronze Delta; test retry, schema enforcement và checkpointed offsets.
2. **Ngày 3:** triển khai HMAC + redaction rules có version; chứng minh analyst role không đọc được Bronze.
3. **Ngày 4:** tạo Gold metrics 5 phút; benchmark tenant query <1 giây; compact file lên ~256 MB.
4. **Ngày 5:** mô phỏng PII leak và data tuổi 7 ngày; chứng minh crypto-shred, table-aware delete, CDF propagation và Gold query vẫn đúng. Canary xác nhận S3 Lifecycle không target Delta data prefix.
