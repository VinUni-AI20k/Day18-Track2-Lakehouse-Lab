# Architecture Decision Record: LLM Observability at 1B Requests/Day Scale

- **Topic:** A — LLM Observability ở quy mô 1B requests/ngày
- **Author:** Data Architecture Team
- **Status:** APPROVED / FOR DESIGN REVIEW
- **Target Budget Cap:** $\le \$5,000$ / month across all compute & storage tiers

---

## 1. Problem Statement

Hệ thống phục vụ nền tảng Gateway cho các mô hình Foundation LLM xử lý **1 tỷ (1.000.000.000) requests/ngày**. Với kích thước trung bình 5 KB/request (payload prompt, response, metadata, token usage, latency), hệ thống phát sinh **5 TB dữ liệu thô (raw JSON) mỗi ngày** (~150 TB/tháng).

### Ràng buộc kỹ thuật & nghiệp vụ:
1. **SLA Dashboard:** Cập nhật chi phí (USD) và độ trễ ($p50, p95, p99$) theo từng Tenant/Model với chu kỳ **5 phút/lần** (ad-hoc query $p95 < 2$ giây).
2. **Lifecycle & Retention:** Giữ toàn bộ prompt/response thô trong **7 ngày** để phục vụ Incident Review và AI Red-teaming; sau 7 ngày chỉ giữ lại số liệu tổng hợp (Aggregates) trong **1 năm (365 ngày)**.
3. **Bảo mật & Tuân thủ PII:** Mã hóa/che giấu (redact/tokenize) toàn bộ dữ liệu định danh cá nhân (PII, email, API key) ngay tại cửa ngõ trước khi lưu trữ xuống các tầng tiếp theo.
4. **FinOps:** Tổng ngân sách bao gồm lưu trữ (Storage), tính toán (Compute), và cước phí API Network không vượt quá **\$5.000 / tháng**.

---

## 2. Architecture Diagram

```
 [1B req/day] ➔ [API Gateway / LLM Proxy]
                       │
                       ▼ (Buffered Streaming, 60s trigger)
         ┌─────────────────────────────┐
         │ Apache Kafka / AWS Kinesis  │
         └─────────────┬───────────────┘
                       │
                       ▼ (Spark Structured Streaming / Flink Engine)
 ┌────────────────────────────────────────────────────────────────────────┐
 │ 🥉 BRONZE LAYER (S3 Standard, Retention = 7 days)                     │
 │ • Path: s3://lakehouse-storage/bronze/llm_raw_events/                 │
 │ • Format: Delta Lake / Snappy Parquet                                  │
 │ • Pipeline: PII Salted-Hashing Tokenizer + Append-only                 │
 └─────────────────────────────┬──────────────────────────────────────────┘
                               │ (Continuous Micro-batch / Deduplication)
                               ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ 🥈 SILVER LAYER (S3 Standard ➔ Glacier Instant, Retention = 7 days)   │
 │ • Path: s3://lakehouse-storage/silver/llm_calls_cleansed/             │
 │ • Format: Delta Lake Partitioned by [date, tenant_id]                  │
 │ • Optimization: Z-ORDER BY (user_id, model) + Deletion Vectors         │
 └─────────────────────────────┬──────────────────────────────────────────┘
                               │ (Aggregated Rollups every 5 mins)
                               ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │ 🥇 GOLD LAYER (S3 Standard-IA, Retention = 365 days)                  │
 │ • Path: s3://lakehouse-storage/gold/tenant_daily_metrics/              │
 │ • Format: Delta Lake Partitioned by [year, month]                      │
 │ • Metrics: p50/p95/p99 latency, cost_usd, error_rate, token_counts    │
 └─────────────────────────────┬──────────────────────────────────────────┘
                               │
               ┌───────────────┴───────────────┐
               ▼                               ▼
 ┌───────────────────────────┐   ┌───────────────────────────┐
 │ Trino / DuckDB Serverless │   │ Grafana / BI Dashboard    │
 │ (Tenant Analytics Engine) │   │ (Cost & Latency Monitors) │
 └───────────────────────────┘   └───────────────────────────┘
```

---

## 3. Key Architectural Decisions & Alternatives Rejected

### Quyết định 1: Chọn Delta Lake làm Table Format trung tâm
* **Quyết định:** Chọn **Delta Lake 3.x / 4.x** với cơ chế *Deletion Vectors* và *Liquid Clustering / Z-Order*.
* **Lựa chọn bị loại 1 (ClickHouse / Elasticsearch):** 
  - *Lý do loại:* Elasticsearch ở quy mô 150 TB/tháng yêu cầu cụm server RAM/SSD đắt đỏ tiêu tốn ít nhất \$15.000–\$25.000/tháng (vượt 300%–500% trần ngân sách FinOps).
* **Lựa chọn bị loại 2 (Apache Hudi):** 
  - *Lý do loại:* Hudi có chi phí overhead metadata và độ phức tạp vận hành cao hơn đáng kể so với Delta Lake trên hạ tầng Cloud Object Storage.

---

### Quyết định 2: Chiến lược phân tầng lưu trữ S3 Lifecycle Tiering
* **Quyết định:** 
  - **Bronze & Silver:** Lưu tại *S3 Standard* trong 3 ngày đầu, tự động chuyển sang *S3 Glacier Instant Retrieval* từ ngày thứ 4 đến ngày thứ 7, sau đó **xóa vĩnh viễn (Hard Expiry)** bằng S3 Lifecycle Rule.
  - **Gold:** Lưu tại *S3 Standard-Infrequent Access (Standard-IA)* trong 365 ngày.
* **Lựa chọn bị loại (Single-tier S3 Standard cố định):**
  - *Lý do loại:* Lưu trữ toàn bộ 150 TB trên S3 Standard ($0.023/GB) ngốn \$3.450/tháng chỉ riêng tiền đĩa cứng, không còn dư ngân sách cho compute và network.

---

### Quyết định 3: Khử PII bằng kỹ thuật Tokenization tại cửa ngõ Bronze
* **Quyết định:** Bóc tách các trường nhạy cảm (prompt, IP, user_id) và băm một chiều (Salted HMAC-SHA-256) ngay tại lúc ingest vào Bronze. Bảng ánh xạ khóa (Key Vault) được cô lập tại AWS KMS / HashiCorp Vault.
* **Lựa chọn bị loại (Dynamic Data Masking lúc query):**
  - *Lý do loại:* Masking lúc query vẫn lưu trữ PII thô dưới đĩa Parquet. Nếu hệ thống lưu trữ bị rò rỉ hoặc kiểm toán theo chuẩn GDPR / Nghị định 13/2023/NĐ-CP, việc lưu PII thô trong 7 ngày không qua mã hóa là vi phạm tuân thủ nghiêm trọng.

---

### Quyết định 4: Sử dụng REST Catalog chuẩn mở (Apache Polaris)
* **Quyết định:** Triển khai **Apache Polaris (REST Catalog Spec)** để phân quyền dữ liệu (RBAC) và quản lý metadata tập trung cho cả Spark, Trino và DuckDB.
* **Lựa chọn bị loại (AWS Glue Data Catalog / Hive Metastore):**
  - *Lý do loại:* Hive Metastore gặp nghẽn cổ chai nghiêm trọng khi số lượng file vượt quá 10 triệu object; AWS Glue tính phí \$1 cho mỗi 100.000 partition scan requests, gây đội chi phí khi query tần suất cao.

---

### Quyết định 5: Chiến lược Micro-batch Ingestion kết hợp Asynchronous Compaction
* **Quyết định:** Ingestion engine ghi batch định kỳ mỗi **60 giây** (tránh small-file tức thời) kết hợp một cron job độc lập chạy `OPTIMIZE compact` mỗi 1 giờ để gom file về kích thước tối ưu 128 MB – 256 MB.
* **Lựa chọn bị loại (Sync Ingestion Compaction):**
  - *Lý do loại:* Ép gom file ngay trong luồng ghi streaming làm tăng latency của ingestion pipeline từ 60 giây lên hơn 5 phút và dễ gây ra tranh chấp ghi (write conflict / concurrency collision).

---

## 4. Failure Modes (Kịch bản sự cố lúc 3 giờ sáng)

### 💥 Sự cố 1: Writer crash bỏ lại hàng triệu file mồ côi (Orphan Storage Leak)
* **Hiện tượng:** Cụm node streaming gặp sự cố Out-Of-Memory (OOM) khởi động lại liên tục, sinh ra hàng triệu file Parquet chưa kịp commit vào transaction log.
* **Phát hiện:** CloudWatch Alarm cảnh báo dung lượng S3 thực tế tăng lệch >20% so với dung lượng metadata báo cáo từ `history()`.
* **Kế hoạch Rollback & Xử lý:** Chạy script dọn rác tự động tính hiệu tập hợp:
  $$\text{Files trên S3} - \text{Files live trong Delta Log}$$
  kèm tham số `min_age_hours=24` (Age Guard) để xóa dứt điểm các file rác mà không gây race-condition với writer đang chạy.

---

### 💥 Sự cố 2: Schema Drift do Provider LLM bổ sung trường dữ liệu mới
* **Hiện tượng:** OpenAI/Anthropic cập nhật API payload, trả về thêm trường `reasoning_tokens` khiến pipeline parse JSON bị lỗi.
* **Phát hiện:** Alert tỷ lệ bản ghi rơi vào Dead Letter Queue (DLQ) vượt ngưỡng $0.1\%$.
* **Kế hoạch Rollback & Xử lý:** 
  1. Pipeline Silver kích hoạt chế độ `schema_mode="merge"` để tự động mở rộng schema mà không làm gián đoạn luồng ghi.
  2. Bật replay job để nạp lại dữ liệu từ DLQ vào Silver.

---

### 💥 Sự cố 3: Dữ liệu đến muộn (Late-arriving Events do mạng đối tác)
* **Hiện tượng:** Một cluster đối tác bị mất kết nối mạng và xả dồn 50 triệu events của 6 tiếng trước vào hệ thống.
* **Phát hiện:** Metric độ trễ `event_time vs processing_time` tăng vọt.
* **Kế hoạch Xử lý:** Sử dụng cú pháp `MERGE INTO` với điều kiện kiểm tra timestamp idempotent:
  ```sql
  MERGE INTO silver.llm_calls t
  USING incoming_batch s
  ON t.request_id = s.request_id
  WHEN MATCHED AND s.ts > t.ts THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  ```
  Sau đó kích hoạt re-aggregate cho đúng phân vùng ngày bị ảnh hưởng ở tầng Gold.

---

## 5. Back-of-the-Envelope Cost Calculation (Toán FinOps)

### 💰 1. Chi phí Lưu trữ (Storage Math):
* **Bronze Layer:** 5 TB/ngày $\times$ 7 ngày retention = **35 TB raw**.
  * Sau khi nén Parquet Snappy (tỷ lệ 3:1) $\rightarrow$ **11.6 TB**.
  * Chi phí S3 Standard: $11.6 \text{ TB} \times \$23/\text{TB} = \mathbf{\$266.80 / \text{tháng}}$.
* **Silver Layer:** Sau khi lọc và dedup $\rightarrow$ **7 TB**.
  * Chuyển S3 Glacier Instant sau ngày thứ 3: $(3 \text{ ngày } \times 7\text{TB}/7 \times \$23) + (4 \text{ ngày } \times 7\text{TB}/7 \times \$4) = \mathbf{\$94.00 / \text{tháng}}$.
* **Gold Layer (Aggregates 365 ngày):** 50 MB/ngày $\times$ 365 ngày = **18.25 GB** $\rightarrow \mathbf{\$0.40 / \text{tháng}}$.

### ⚡ 2. Chi phí Compute (Spark & Compaction):
* **Streaming Ingestion:** Chạy cụm EMR / Spot Instances (2 node `c6g.xlarge` ARM64 spot @ \$0.068/hr):
  $$2 \times \$0.068 \times 24 \times 30 = \mathbf{\$97.92 / \text{tháng}}$$
* **Compaction & Rollup Cronjob:** Chạy 1 giờ/lần (mỗi lần 5 phút trên Spot Instances):
  $$4 \text{ node } \times \$0.136 \times (24 \times 0.083 \times 30) = \mathbf{\$32.50 / \text{tháng}}$$
* **Ad-hoc & Dashboard Queries (Trino/DuckDB Serverless):** $\approx \mathbf{\$1.200.00 / \text{tháng}}$.

### 📡 3. Chi phí API Requests (S3 PUT/GET):
* Ingest 60s/lần = 1.440 PUT/ngày $\rightarrow$ Không đáng kể ($\approx \$15/\text{tháng}$).
* Query scan planning qua Metadata: $\approx \$180/\text{tháng}$.

$$\sum \text{Total Estimated Cost} = \$266.80 + \$94.00 + \$0.40 + \$97.92 + \$32.50 + \$1,200.00 + \$195.00 \approx \mathbf{\$1,886.62 / \text{tháng}}$$

> ✅ **Kết luận FinOps:** Chi phí ước tính **\$1.886 / tháng** nằm hoàn toàn dưới trần ngân sách **\$5.000 / tháng** (Dư địa an toàn 62% cho đột biến tải).

---

## 6. MVP Slice trong 1 Tuần Đầu Tiên

Để chứng minh tính khả thi của kiến trúc trước ban giám đốc, kế hoạch triển khai MVP trong 5 ngày làm việc:

* **Ngày 1:** Dựng luồng nạp mẫu Kinesis $\rightarrow$ Delta Bronze với cơ chế Tokenization băm PII.
* **Ngày 2:** Viết Spark Streaming job khử trùng lặp (Dedup) và ghi phân vùng tầng Silver theo `[date, tenant_id]`.
* **Ngày 3:** Thiết lập cron job `OPTIMIZE compact` + `Z-ORDER` và benchmark tốc độ query point tenant.
* **Ngày 4:** Tạo bảng tổng hợp Gold tính toán $p50, p95$ latency và chi phí USD, kết nối Grafana dashboard.
* **Ngày 5:** Chạy kịch bản giả lập sự cố writer OOM để kiểm tra khả năng phục hồi và đo đạc chi phí thực tế.
