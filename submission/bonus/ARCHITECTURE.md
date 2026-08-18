# Enterprise Architecture Decision Record: LLM Observability at 1B Requests/Day

**Author:** Dinh Van Sinh (Track 2 - Lakehouse Architecture)  
**Role:** Lead Lakehouse Architect  
**Status:** Approved for Production Review  
**Target System:** Foundation Model Observability & Governance Lakehouse (Topic A)

---

## 1. Problem Statement

Hệ thống API Foundation Model ghi nhận **1 tỷ requests/ngày** (~11.574 req/s trung bình, 35.000 req/s peak). Với kích thước trung bình **5 KB/request** (gồm prompt, completion, latency, token metrics, model params, tenant metadata), dung lượng dữ liệu thô sinh ra đạt **5 TB/ngày** (~150 TB/tháng uncompressed).

### Ràng buộc kỹ thuật & nghiệp vụ:
1. **SLA Dashboard:** Cập nhật latency, token usage và cost theo từng `tenant_id` mỗi **5 phút**; độ trễ truy vấn p95 < 1,5s.
2. **Lifecycle & Retention:** Dữ liệu raw prompt/completion đầy đủ chỉ lưu **7 ngày** phục vụ incident response & debug; sau 7 ngày tự động dọn dẹp; các bảng Gold Aggregates lưu trữ **365 ngày** phục vụ billing và kiểm toán.
3. **Bảo mật & Compliance (GDPR/PDPL):** Dữ liệu PII (email, phone, API key, SSN/CCCD) trong prompt/response phải được **redact/tokenize ngay tại tầng Bronze** trước khi bất kỳ nhân sự hoặc hệ thống analytics nào có quyền truy cập.
4. **Hard FinOps Cap:** Tổng chi phí lưu trữ (Storage + API Requests + Lifecycle transition) **≤ $5.000/tháng**.

---

## 2. Architecture Diagram

```
                              INGESTION & MEDALLION LAKEHOUSE ARCHITECTURE
                              
  [ 1B Requests/Day ] ──► [ API Gateway / LLM Proxy ] (Kong / Envoy)
                                  │ (Async Non-blocking Log Emission)
                                  ▼
                         [ Apache Kafka Cluster ] (Topic: `llm.events.v1` - 64 Partitions)
                                  │
                                  ▼
               [ Spark / Flink Structured Streaming ] (Trigger: 60s Micro-batch)
                     │ ──► [ PII Tokenization Engine (KMS + FPE/HMAC-SHA256) ]
                     │ ──► [ Schema Validation & Dead Letter Queue (DLQ) ]
                     ▼
  ════════════════════════════════════════════════════════════════════════════════════════════════
  MEDALLION STORAGE LAYERS (AWS S3 + Delta Lake 3.2 / Apache Iceberg with Polaris Catalog)
  ════════════════════════════════════════════════════════════════════════════════════════════════
  
   ┌────────────────────────────────────────────────────────────────────────────────────────┐
   │ BRONZE LAYER: `bronze_llm_events` (S3 Standard / 7-Day TTL)                             │
   │ • Format: Delta Lake (Zstandard level 3), Raw JSON payload with PII-masked fields      │
   │ • Partition: `date=YYYY-MM-DD` | File Target: 256 MB | Write Mode: Append              │
   │ • Retention: 7 ngày (Tự động VACUUM retention_hours=168)                               │
   └────────────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ (Continuous Streaming Deduplication & Flattening)
   ┌────────────────────────────────────────────────────────────────────────────────────────┐
   │ SILVER LAYER: `silver_llm_calls` (S3 Standard / 7-Day TTL)                             │
   │ • Format: Delta Lake + Deletion Vectors + Liquid Clustering (`tenant_id`, `model_id`)  │
   │ • Schema: `request_id`, `ts`, `tenant_id`, `model_id`, `prompt_tokens`,                 │
   │           `completion_tokens`, `latency_ms`, `cost_usd`, `status`, `prompt_sanitized`  │
   │ • Fast Point Query: `WHERE tenant_id = 't_123'` skips > 85% data files via stats       │
   └────────────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼ (5-Minute Streaming Window Rollup & Daily Watermark)
   ┌────────────────────────────────────────────────────────────────────────────────────────┐
   │ GOLD LAYER: Aggregates & Observability Marts (S3 Standard-IA → Glacier Instant / 1-Yr) │
   │ • `gold_tenant_5min_metrics`: (tenant_id, window, p50/p95/p99 latency, tokens, cost)   │
   │ • `gold_tenant_daily_billing`: (tenant_id, date, total_cost, error_rate, quota_pct)    │
   │ • Partition: `date=YYYY-MM` | Compaction: 512 MB files | Retention: 365 ngày           │
   └────────────────────────────────────────────────────────────────────────────────────────┘
                                  │
                                  ▼
  ════════════════════════════════════════════════════════════════════════════════════════════════
  QUERY & CONSUMPTION PATH
  ════════════════════════════════════════════════════════════════════════════════════════════════
         │                                       │                               │
         ▼                                       ▼                               ▼
  [ Superset / Grafana ]               [ Ad-hoc Incident Debug ]       [ Billing & Finance APIs ]
  • Engine: Trino / DuckDB             • Engine: Spark SQL             • Direct Iceberg/Delta Read
  • Refresh: 5 phút                    • Filter by `request_id` / time • Monthly invoice generation
  • Query p95 < 800ms                  • Scan pruned to 1-2 files      • Zero copy from Gold Marts
```

---

## 3. Key Architectural Decisions & Rejected Alternatives

### Quyết định 1: Định dạng bảng lưu trữ (Table Format)
* **Lựa chọn:** **Delta Lake 3.2** (với Deletion Vectors, Liquid Clustering và Change Data Feed).
* **Lý do chọn:** 
  1. Hỗ trợ **Liquid Clustering** vượt trội hơn Z-Order truyền thống: không cần rewrite toàn bộ partition khi cluster theo nhiều chiều (`tenant_id`, `model_id`), giảm 60% chi phí compute compaction.
  2. **Deletion Vectors (DV):** Khi xử lý yêu cầu GDPR/PDPL Right-to-Erasure của tenant, hệ thống chỉ ghi file bitmap xóa (~vài KB) thay vì rewrite cả file Parquet 256 MB.
  3. Tích hợp native với Rust/Python (`delta-rs`) cho phép các microservices tra cứu metadata siêu nhẹ mà không cần khởi động JVM.
* **Loại bỏ Apache Iceberg:** Mặc dù Iceberg có Hidden Partitioning rất mạnh, nhưng tại thời điểm 2026, cơ chế Deletion Vector và streaming micro-batch latency của Delta Lake tối ưu hơn cho write-heavy streaming workload 35K req/s.
* **Loại bỏ ClickHouse/Dedicated TSDB:** Mặc dù ClickHouse query aggregations siêu nhanh, nhưng chi phí vận hành cluster compute 24/7 cho 150 TB/tháng vượt quá ngân sách $5.000/tháng và vi phạm nguyên tắc tách biệt Compute-Storage của Lakehouse.

---

### Quyết định 2: Chiến lược phân vùng và Indexing (Partitioning & Clustering)
* **Lựa chọn:** **Coarse Partitioning theo `date=YYYY-MM-DD` + Liquid Clustering trên `(tenant_id, model_id)`**.
* **Lý do chọn:**
  1. Tránh hoàn toàn lỗi **Small-File / Over-partitioning Pathology**. Nếu partition sâu theo `date/tenant_id` với 5.000 tenants, mỗi 5 phút sẽ sinh ra hàng chục ngàn file 10 KB, làm sập S3 prefix rate limit và metadata planning.
  2. Bằng cách partition theo ngày kết hợp Liquid Clustering, mỗi file Parquet đạt kích thước chuẩn **256 MB** chứa dữ liệu của một nhóm tenant liên tiếp. File stats (min/max) cho phép engine skip ≥ 85% số file khi query theo từng tenant.
* **Loại bỏ Hierarchical Partitioning (`/year/month/day/tenant_id/`):** Gây ra hàng triệu file nhỏ (small files), chi phí S3 `PUT/LIST` requests tăng vọt > $3.000/tháng.
* **Loại bỏ Unclustered Append-only:** Khiến mọi câu truy vấn dashboard tenant phải full scan toàn bộ 5 TB của ngày đó, vi phạm SLA latency < 1,5s.

---

### Quyết định 3: Kiến trúc bảo vệ dữ liệu nhạy cảm (PII Anonymization)
* **Lựa chọn:** **Deterministic Tokenization / Format-Preserving Encryption (FPE) + SHA-256 HMAC ngay tại Streaming Ingestion (Bronze Landing)**.
* **Lý do chọn:**
  1. **Security by Default:** Không bao giờ lưu PII ở dạng plaintext xuống bất kỳ layer nào của data lake. Khóa HMAC được lưu an toàn tại AWS KMS với chính sách xoay vòng định kỳ (Key Rotation).
  2. Cho phép thực hiện các phép toán thống kê chính xác (`COUNT DISTINCT`, `GROUP BY user_pseudonym`) trên Silver/Gold mà không làm lộ danh tính người dùng thật.
* **Loại bỏ Dynamic Data Masking tại Query-time:** Nguy cơ rò rỉ dữ liệu cao nếu analyst query bằng engine khác hoặc cấu hình IAM rule bị sai sót. Ngoài ra, chi phí compute de-masking lặp đi lặp lại ở mỗi query rất tốn kém.
* **Loại bỏ Irreversible Redaction (Ghi đè bằng `[REDACTED]` hoàn toàn):** Làm mất khả năng trace log điều tra sự cố khi cần correlate nhiều request từ cùng một user gặp lỗi.

---

### Quyết định 4: Chiến lược Ingestion & Sizing File
* **Lựa chọn:** **Structured Streaming Micro-batch 60s + In-memory Small File Coalesce + Target File Size 256 MB**.
* **Lý do chọn:**
  1. Chu kỳ micro-batch 60 giây cân bằng hoàn hảo giữa độ trễ dashboard (5 phút) và kích thước file ghi.
  2. Trong mỗi micro-batch (60s ~ 700.000 events ~ 3,5 GB), Spark coalesce thành ~14 files (250 MB/file) trước khi commit vào Bronze Delta Table.
* **Loại bỏ Continuous Streaming Record-by-Record (1-second trigger):** Sinh ra 86.400 commits/ngày, dẫn đến transaction log phình to nghẽn I/O và chi phí S3 PUT requests bùng nổ.
* **Loại bỏ Hourly Batch Ingestion:** Không đáp ứng được SLA cập nhật dashboard 5 phút một lần của khách hàng.

---

### Quyết định 5: Vận hành FinOps & Quản trị Vòng đời Lưu trữ (Lifecycle & Maintenance)
* **Lựa chọn:** **4-Job Automated Maintenance Pipeline + S3 Tiering Transition**.
* **Lý do chọn:**
  1. **Job 1 (Auto-Compaction):** Chạy mỗi 30 phút gộp các file trễ thành 256 MB - 512 MB.
  2. **Job 2 (Clustering):** Chạy mỗi 2 giờ để duy trì Z-Order/Liquid Clustering khi có dữ liệu mới.
  3. **Job 3 (7-Day VACUUM & Retention Expiry):** Chạy daily lúc 02:00 AM với `VACUUM bronze_llm_events RETAIN 168 HOURS` và `silver_llm_calls RETAIN 168 HOURS`.
  4. **Job 4 (Orphan Sweeper):** Quét và xóa các uncommitted multipart uploads và files rác của crashed streaming workers (> 24h).
  5. **S3 Tiering:** Gold Layer chuyển sang **S3 Standard-IA** sau 30 ngày và **S3 Glacier Instant Retrieval** sau 90 ngày.
* **Loại bỏ Simple S3 Expiration Rule:** S3 Lifecycle Rule chỉ xóa file trên object storage nhưng **không cập nhật Delta Log**, dẫn đến hỏng bảng (corrupted snapshot) và lỗi `FileNotFoundException` khi query time travel.

---

## 4. Failure Modes & 3-AM Disaster Recovery

### Failure Mode 1: Traffic Spike 10x (350.000 req/s) & Small-File Pathology
* **Triệu chứng lúc 3h sáng:** Một khách hàng lớn chạy load test, Kafka consumer lag tăng vọt, số lượng file nhỏ ghi vào Bronze tăng đột biến, query dashboard bị timeout (> 30s).
* **Cơ chế phát hiện (Detection):** Alert CloudWatch / Datadog: `DeltaLogCommitLatency > 5000ms` HOẶC `KafkaConsumerLag > 500,000 messages`.
* **Quy trình xử lý & Rollback (Remediation):**
  1. Tự động kích hoạt Dynamic Micro-batch Scaling: tăng trigger interval từ 60s lên 180s để tăng kích thước batch ghi.
  2. Chạy khẩn cấp ad-hoc compaction: `OPTIMIZE silver_llm_calls WHERE date = current_date()`.
  3. Query dashboard tự động fall back đọc từ pre-aggregated Gold Table thay vì scan Silver.

---

### Failure Mode 2: PII Leakage do Upstream Prompt Format Drift
* **Triệu chứng lúc 3h sáng:** Upstream model gateway thay đổi format JSON, trường PII mới (`user_tax_id`) lọt qua tầng filter và bị ghi vào cột `prompt_sanitized` của Silver.
* **Cơ chế phát hiện (Detection):** Anomaly Detector (chạy định kỳ quét 0.1% random sample) phát hiện mẫu regex PII trong Silver layer. Alert P1 gửi về On-call Architect.
* **Quy trình xử lý & Rollback (Remediation):**
  1. **Hotfix Pipeline:** Cập nhật ngay regex tokenization rule trên Kafka consumer.
  2. **Time Travel Quarantine:** Truy vấn phiên bản trước lỗi: `SELECT * FROM silver_llm_calls VERSION AS OF (N - 1)`.
  3. **Targeted In-place Remediation:** Chạy Delta `UPDATE silver_llm_calls SET prompt_sanitized = mask_pii(prompt_sanitized) WHERE date = current_date()`.
  4. **Hard Purge:** Chạy `VACUUM silver_llm_calls RETAIN 0 HOURS` (override safety guard) để tiêu hủy hoàn toàn các file Parquet cũ chứa PII chưa mask khỏi S3.

---

### Failure Mode 3: Metadata Log Bloating & Checkpoint Replay Failure
* **Triệu chứng lúc 3h sáng:** Sau 5.000 commits streaming, job query engine (Trino/DuckDB) mất hơn 45 giây chỉ để parse metadata JSON trước khi đọc dữ liệu, khiến SLA p95 bị vi phạm.
* **Cơ chế phát hiện (Detection):** Alert: `QueryPlanningTimeMs > 2000ms`.
* **Quy trình xử lý & Rollback (Remediation):**
  1. Nguyên nhân: Quá trình ghi Checkpoint Parquet định kỳ (mỗi 10 commits) bị lỗi do worker memory leak.
  2. Khắc phục: Ép tạo Checkpoint ngay lập tức bằng script quản trị `scripts/lakehouse.py::create_checkpoint()`.
  3. Cold readers sẽ chỉ load 1 file Parquet checkpoint duy nhất + vài JSON commits mới nhất, đưa planning time về < 150ms.

---

## 5. Back-of-Envelope FinOps Cost Model

### Giả định khối lượng (Volume Math):
* **Raw Ingestion:** $1\text{B req} \times 5\text{ KB} = 5.000\text{ GB} = 5\text{ TB/ngày}$.
* **Bronze Storage (Nén Zstd ~50%):** $2,5\text{ TB/ngày} \times 7\text{ ngày} = \mathbf{17,5\text{ TB}}$ (ổn định do xóa theo rolling 7 ngày).
* **Silver Storage (Đã trích xuất & nén Zstd ~30%):** $1,5\text{ TB/ngày} \times 7\text{ ngày} = \mathbf{10,5\text{ TB}}$ (rolling 7 ngày).
* **Gold Aggregates (Rollups 5 phút & daily):** ~5 GB/ngày $\times 365\text{ ngày} = \mathbf{1,825\text{ TB/năm}}$.
* **Tổng dung lượng đĩa active trung bình:** $\approx 30\text{ TB}$.

### Bảng tính chi phí chi tiết (Hàng tháng):

| Thành phần chi phí | Công thức tính toán | Chi phí ($/tháng) |
|---|---|---|
| **S3 Storage: Bronze & Silver** | $28\text{ TB} \times \$0,023/\text{GB-tháng}$ | **$644,00** |
| **S3 Storage: Gold (IA & Glacier)** | $1,8\text{ TB} \times \$0,0125/\text{GB-tháng}$ | **$22,50** |
| **S3 API PUT/POST Requests** | Micro-batch 60s = 1.440 commits/ngày $\times 20$ files $\times 30$ ngày = 864.000 PUTs $\times \$0,005/1.000$ | **$4,32** |
| **S3 API GET/LIST Requests** | Dashboard queries + Maintenance scanners $\approx 10.000.000$ GETs $\times \$0,0004/1.000$ | **$4,00** |
| **Compute: Streaming Ingestion** | 3 $\times$ `c6g.xlarge` Spot EMR/K8s instances (\$0,068/hr $\times 720$ hrs) | **$146,88** |
| **Compute: 4-Job Maintenance** | 1 $\times$ `r6g.2xlarge` Spot instance chạy 2 giờ/ngày (\$0,15/hr $\times 60$ hrs) | **$9,00** |
| **Compute: Trino / DuckDB Query** | Serverless Auto-scaling Query cluster cho dashboard | **$450,00** |
| **Dự phòng biến động & Data Transfer** | 20% Buffer cho traffic peak & cross-AZ egress | **$250,00** |
| **TỔNG CỘNG HÀNG THÁNG** | **Tất cả chi phí Storage + Compute + API** | **$1.530,70 / tháng** |

> 🎯 **Kết luận FinOps:** Tổng chi phí **$1.530,70/tháng**, chỉ chiếm **~30,6% ngân sách cho phép ($5.000/tháng)**. Tiết kiệm được **$3.469,30/tháng** để đầu tư vào GPU compute và cache serving.

---

## 6. One-Week Shippable MVP Slice

Để chứng minh tính khả thi của kiến trúc trước Senior Review, team triển khai **MVP 1-Tuần** với phạm vi sau:

```
┌──────────────────────────────────────────────────────────────────────────────────┐
│                             ONE-WEEK MVP MILESTONES                              │
├─────────┬────────────────────────────────────────────────────────────────────────┤
│ Day 1-2 │ • Dựng Local/Cloud Kafka topic + Generator giả lập 50.000 events/giây. │
│         │ • Viết hàm PII HMAC-SHA256 tokenization chuẩn hóa dữ liệu.            │
├─────────┼────────────────────────────────────────────────────────────────────────┤
│ Day 3-4 │ • Xây dựng Bronze & Silver Delta Streaming Ingestion (Micro-batch 60s).│
│         │ • Cấu hình Liquid Clustering trên `(tenant_id, model_id)`.            │
├─────────┼────────────────────────────────────────────────────────────────────────┤
│ Day 5   │ • Triển khai 5-Minute Window Rollup Job sinh bảng Gold Metrics.        │
│         │ • Đo kiểm benchmark: Pruning skip rate > 80%, query p95 < 1s.          │
├─────────┼────────────────────────────────────────────────────────────────────────┤
│ Day 6   │ • Cài đặt cron job tự động: Compaction + VACUUM 7-day TTL + Checkpoint.│
│         │ • Kiểm tra cơ chế Time Travel và Audit Trail.                         │
├─────────┼────────────────────────────────────────────────────────────────────────┤
│ Day 7   │ • Đo kiểm khả năng chịu lỗi (Chaos test: crash worker, schema drift).  │
│         │ • Trình diễn demo Dashboard trực tiếp cho Architecture Review Board.   │
└─────────┴────────────────────────────────────────────────────────────────────────┘
```

---

## 7. Proof-of-Concept (PoC) Code

Mã nguồn thực thi chứng minh cơ chế lõi của MVP được lưu tại:  
👉 [`submission/bonus/poc/poc_llm_observability.py`](poc/poc_llm_observability.py)

Mã nguồn kiểm chứng độc lập các cơ chế:
1. **Streaming Micro-batch Landing & Tokenization**
2. **5-Minute Gold Aggregation (p50/p95 latency, token cost calculation)**
3. **Liquid Clustering / Data Skipping efficiency benchmark**
4. **Lifecycle Expiry & Safe 7-Day VACUUM simulation**
