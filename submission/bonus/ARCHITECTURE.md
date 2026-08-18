# Architecture Decision Record (ADR): LLM Observability at 1B Requests/Day Scale

> **Topic:** Topic A — LLM Observability at 1 Billion Requests/Day (~5 TB/day raw)  
> **Status:** Approved / Design Review Deliverable  
> **Target Budget:** $\le \$5,000/\text{tháng}$ (Storage & Ingestion Footprint)  
> **SLAs:** Dashboard Refresh $\le 5\text{ phút}$; Prompt Incident Review Retention = 7 ngày; Aggregates Retention = 1 năm; Strict PII Redaction at Landing.

---

## 1. Problem Statement

Hệ thống Foundation Model API xử lý **1 tỷ requests/ngày**. Mỗi payload (metadata, prompt, response, token usage, latency metrics) có kích thước trung bình **~5 KB**, sinh ra **5 TB raw data/ngày** (~57.8 MB/s streaming throughput, ~11,570 requests/giây ở steady state, peak ~30,000 req/s).

Bài toán đối mặt với 4 ràng buộc xung đột kỹ thuật:
1. **Real-time Latency vs. File Size:** Cần dashboard thống kê chi phí, token consumption, latency p50/p95/p99 theo từng `tenant_id` và `model_id` làm mới mỗi **5 phút**. Ghi streaming tần suất cao dễ gây ra thảm họa *Small-File Explosion*.
2. **Tuân thủ Bảo mật PII:** Prompt/Response chứa thông tin nhạy cảm (PII/Secret) phải được che giấu (redact/tokenize) ngay tại cổng Bronze trước khi bất kỳ nhân sự hoặc analyst nào có thể truy cập.
3. **Vòng đời Dữ liệu phân tầng (Retention Divergence):** Full Prompt/Response chỉ cần giữ **7 ngày** để Incident Review, trong khi Aggregates phải lưu trữ **1 năm** phục vụ kiểm toán tài chính và báo cáo SLA.
4. **Trần Ngân Sách FinOps Cứng:** Tổng chi phí lưu trữ (Storage + API Operations) không được vượt quá **\$5,000/tháng** (trong khi 150 TB/tháng lưu S3 Standard thông thường sẽ ngốn > \$3,450/tháng chưa tính backup/logs).

---

## 2. Architecture Overview & Pipeline Diagram

Hệ thống triển khai theo kiến trúc **Medallion Lakehouse trên AWS S3**, sử dụng **Delta Lake 3.x với Liquid Clustering** và **Apache Flink / Spark Structured Streaming**.

```
[ API Gateways / Ingress ] (1B req/day ~ 11.5K rps)
           │ (OTel GenAI Telemetry)
           ▼
┌─────────────────────────────────────────────────────────────┐
│ Streaming Ingestion Buffer (Apache Kafka / Redpanda Cluster)│
└─────────────────────────────────────────────────────────────┘
           │
           │ Stream Consume (Flink / Spark Streaming)
           │ + Inline HMAC-SHA256 Tokenization / Regex PII Masking
           ▼
┌─────────────────────────────────────────────────────────────┐
│ 🟫 BRONZE LAYER (Append-only Raw Landing)                  │
│ • Path: s3://lakehouse-telemetry/bronze/llm_events_raw/     │
│ • Partition: date=YYYY-MM-DD                                │
│ • Retention: 7 ngày (S3 Lifecycle → Expire / Hard Delete)   │
└─────────────────────────────────────────────────────────────┘
           │
           │ Micro-batch Streaming (5-min trigger)
           │ + De-duplication on request_id + Schema Enforcement
           ▼
┌─────────────────────────────────────────────────────────────┐
│ 🥈 SILVER LAYER (Enriched & Clustered Logs)                 │
│ • Path: s3://lakehouse-telemetry/silver/llm_calls_cleaned/  │
│ • Clustering: Z-ORDER / Liquid Clustering by (tenant_id)    │
│ • Storage: S3 Standard (Day 1-3) → S3 Standard-IA (Day 4-7) │
│ • Retention: 7 ngày (Tombstone + Deletion Vectors)          │
└─────────────────────────────────────────────────────────────┘
           │
           │ 5-minute Micro-batch Aggregation (Scheduled SQL)
           │ + Metric Rollup (p50/p95/p99, cost_usd, error_rate)
           ▼
┌─────────────────────────────────────────────────────────────┐
│ 🥇 GOLD LAYER (Multi-Tenant Analytics Mart)                 │
│ • Path: s3://lakehouse-telemetry/gold/llm_metrics_5min/     │
│ • Partition: year=YYYY / month=MM                           │
│ • Storage: S3 Standard (< 100 GB tổng)                      │
│ • Retention: 365 ngày (1 năm)                               │
└─────────────────────────────────────────────────────────────┘
           │
     ┌─────┴────────────────────────────┐
     ▼                                  ▼
[ Real-time Dashboard ]         [ Incident Review Tool ]
(DuckDB Serverless / ClickHouse) (Athena / DuckDB over Silver)
(Latency < 500ms for Tenant UI)  (Time-travel audit for 7 days)
```

---

## 3. Key Architecture Decisions & Trade-off Analysis

### Quyết định 1: Chọn Table Format — Delta Lake 3.x với Liquid Clustering
* **Lựa chọn:** **Delta Lake 3.x**.
* **Lý do chọn:** 
  1. Hỗ trợ **Liquid Clustering** (`CLUSTER BY tenant_id, timestamp`) giúp tối ưu hoá việc ghi streaming liên tục mà không làm cố định layout partition cứng.
  2. Hỗ trợ **Deletion Vectors** (DV): Khi có yêu cầu xoá dữ liệu PII theo GDPR/Nghị định 13, Delta chỉ cần ghi vector bit đánh dấu xoá thay vì rewrite toàn bộ file Parquet dung lượng lớn.
  3. **Change Data Feed (CDF)** tích hợp sẵn cho phép downstream streaming consume sự kiện thay đổi tức thì.
* **Các phương án đã loại bỏ:**
  * *Loại Apache Iceberg:* Mặc dù Iceberg có REST Catalog xuất sắc, nhưng cơ chế streaming append 5 phút một lần sinh ra khối lượng Manifest Files và Snapshot Metadata rất lớn, đòi hỏi tần suất chạy `expire_snapshots` liên tục dễ gây lock catalog.
  * *Loại Plain Parquet (Hive style):* Không có ACID, không có min/max statistics pruning ở cấp độ commit log, dễ bị corrupt khi ghi streaming đồng thời.

---

### Quyết định 2: Chiến lược Phân vùng (Partitioning) & Tối ưu hoá Truy vấn Đa khách hàng (Multi-tenant)
* **Lựa chọn:** Phân vùng thô theo **`date=YYYY-MM-DD`** kết hợp **Z-ORDER / Clustering theo `tenant_id`**.
* **Lý do chọn:**
  1. Hệ thống phục vụ hơn **10,000 tenants**. Nếu partition trực tiếp theo thư mục `date=.../tenant_id=...`, mỗi 5 phút ghi sẽ sinh ra $10,000 \times 288 = 2.88\text{ triệu files/ngày}$ $\rightarrow$ Sụp đổ hệ thống File System (Small-file Disaster).
  2. Phân vùng theo ngày giúp việc dọn dẹp lifecycle (xóa dữ liệu cũ hơn 7 ngày) diễn ra ở cấp độ metadata / prefix S3 mà không tốn compute.
  3. Z-Order gom các dòng của cùng một tenant vào các row groups liền kề trong file Parquet, giúp truy vấn dashboard lọc `WHERE tenant_id = 'X'` bỏ qua (**prune**) hơn **95% dữ liệu** (như đã chứng minh trong NB2).
* **Phương án loại bỏ:**
  * *Loại Hash Partitioning theo tenant:* Gây skew dữ liệu trầm trọng giữa các tenant lớn (Enterprise) và tenant nhỏ (Free tier).

---

### Quyết định 3: Bảo mật PII — Inline Tokenization & Salting tại Cổng Landing
* **Lựa chọn:** **Deterministic Tokenization (HMAC-SHA256 với Secret Salt Key)** cho định danh (`user_id`, `email`) kết hợp **Regex/NER Masking** cho Prompt/Response ngay trong Spark Streaming/Flink trước khi ghi xuống Bronze.
* **Lý do chọn:**
  1. Đảm bảo nguyên tắc *Zero-Cleartext at Rest*: Dữ liệu thô rơi xuống S3 Bronze đã được làm sạch PII $\rightarrow$ Không cần phân quyền phức tạp để chặn người đọc tầng Bronze.
  2. Khả năng Dedup không bị ảnh hưởng: HMAC với Salt cố định cho phép so khớp và dedup `user_id` / `request_id` chính xác mà không cần biết thông tin thật của người dùng.
* **Phương án loại bỏ:**
  * *Loại Asynchronous PII Scrubbing (ghi raw rồi batch job quét sau):* Tạo ra "cửa sổ lộ lọt dữ liệu" (vài phút đến vài giờ) vi phạm nghiêm trọng Điều 13 Nghị định 13/2023/NĐ-CP và GDPR.

---

### Quyết định 4: Chiến lược Vòng đời Lưu trữ (FinOps Tiering) đạt trần $\le \$5,000/\text{tháng}$
* **Lựa chọn:** Thiết lập **S3 Lifecycle Rules tự động** kết hợp nén ZSTD Level 3:
  1. **Bronze Raw Table:** Lưu tại *S3 Standard* $\rightarrow$ Tự động Hard Expire sau **7 ngày**.
  2. **Silver Cleaned Table:** Lưu tại *S3 Standard* (0–3 ngày) $\rightarrow$ Chuyển sang *S3 Standard-Infrequent Access (IA)* (ngày 4–7) $\rightarrow$ Expire sau 7 ngày.
  3. **Gold Aggregates Table:** Dung lượng nhỏ (< 100 GB/năm), lưu vĩnh viễn trên *S3 Standard* trong **365 ngày**.
* **Phương án loại bỏ:**
  * *Loại S3 Glacier cho toàn bộ Bronze:* Glacier có phí tối thiểu 90 ngày lưu trữ và phí GET/Lifecycle transition cao, không phù hợp cho dữ liệu chỉ lưu 7 ngày.

---

### Quyết định 5: Lựa chọn Query Engine phục vụ Dashboard 5 phút
* **Lựa chọn:** **DuckDB Serverless (qua Lambda/ECS) + Embedded Caching Layer**.
* **Lý do chọn:**
  1. Gold metrics table chỉ có dung lượng ~3.5 MB/ngày (đã aggregate theo 5 phút). DuckDB có thể scan toàn bộ bảng Gold trong **< 30 ms** trực tiếp qua S3 S3A/Arrow.
  2. Chi phí compute gần như bằng \$0 khi không có query, không cần duy trì cluster Spark/Trino chạy 24/7 chỉ để phục vụ dashboard.
* **Phương án loại bỏ:**
  * *Loại Snowflake / Databricks SQL Warehouse 24/7:* Chi phí duy trì cụm compute tối thiểu \$2,000–\$4,000/tháng, phá vỡ trần ngân sách \$5,000.

---

## 4. Failure Modes & 3:00 AM Incident Runbooks

### 🚨 Failure Mode 1: Traffic Spike đột biến $\rightarrow$ Nổ file nhỏ làm chậm Dashboard 5 phút
* **Triệu chứng (Detection):** Alert Prometheus kích hoạt khi thời gian query Gold/Silver vượt quá 5 giây; số lượng files trong `_delta_log/` tăng vọt > 50,000 files/ngày.
* **Cơ chế gây lỗi:** Traffic tăng từ 11K rps lên 50K rps khiến micro-batch commit liên tục tạo ra hàng chục nghìn file Parquet kích thước < 500 KB.
* **3:00 AM Rollback & Recovery Plan:**
  1. Bật tính năng **Auto-Compaction** và **Optimized Writes** trong Delta table properties:
     ```sql
     ALTER TABLE silver.llm_calls SET TBLPROPERTIES (
       'delta.autoOptimize.optimizeWrite' = 'true',
       'delta.autoOptimize.autoCompact' = 'true'
     );
     ```
  2. Kích hoạt khẩn cấp job `OPTIMIZE silver.llm_calls ZORDER BY (tenant_id)` chạy nền để gom file về kích thước chuẩn 128 MB.
  3. Dashboard tạm thời chuyển hướng đọc bảng Gold đã pre-aggregated thay vì scan bảng Silver.

---

### 🚨 Failure Mode 2: Lộ lọt PII do mẫu Prompt mới vượt qua bộ lọc Regex
* **Triệu chứng (Detection):** Hệ thống Data Loss Prevention (DLP) định kỳ quét ngẫu nhiên phát hiện số CCCD/Credit Card chưa được mask trong bảng Silver.
* **3:00 AM Rollback & Recovery Plan (Tận dụng Delta Time Travel & Deletion Vectors):**
  1. Định vị phạm vi ảnh hưởng thông qua commit timestamp trong `DESCRIBE HISTORY`.
  2. Cập nhật ngay bộ quy tắc Regex/NER trên Flink Ingestion filter.
  3. Chạy câu lệnh UPDATE khử PII trực tiếp trên bảng Silver bằng Deletion Vectors (không rewrite toàn bộ dữ liệu):
     ```sql
     UPDATE silver.llm_calls 
     SET prompt = '[REDACTED_PII]' 
     WHERE prompt RLIKE 'regex_pattern_cccd';
     ```
  4. Thực hiện `VACUUM silver.llm_calls RETAIN 0 HOURS` (với cờ bypass retention check) để xoá vĩnh viễn physical parquet blocks chứa cleartext PII cũ khỏi đĩa S3.

---

### 🚨 Failure Mode 3: Job Streaming Crash để lại Uncommitted Orphan Files gây phình S3
* **Triệu chứng (Detection):** Hóa đơn lưu trữ S3 tăng bất thường nhưng `SELECT count(*)` trên Delta Table không tăng tương ứng; số byte đo bằng S3 CloudWatch lệch > 20% so với Delta Metadata.
* **Cơ chế gây lỗi (Bài học từ NB6):** Lệnh `VACUUM` tiêu chuẩn của Delta chỉ dọn dẹp các file có tombstone trong log. File do worker bị kill đột ngột chưa từng commit vào `_delta_log/` sẽ vô hình với VACUUM.
* **3:00 AM Rollback & Recovery Plan:**
  1. Chạy kịch bản **Reconciliation Orphan Scanner** (như đã cài đặt trong NB6): Quét toàn bộ `s3.list_objects` đối chiếu với tập hợp `dt.file_uris()`.
  2. Thực hiện phép trừ tập hợp: $\text{Orphans} = \text{S3 Files} \setminus \text{Committed Files}$.
  3. Xoá an toàn các file có tuổi thọ > 24 giờ chưa từng commit.

---

## 5. Back-of-the-Envelope Cost Estimation (Bản toán chi phí chi tiết)

### 📊 Giả định số liệu (Scale Parameters):
* **Số lượng request:** 1,000,000,000 requests/ngày.
* **Dung lượng raw:** $1\text{B} \times 5\text{ KB} = 5,000\text{ GB} = 5\text{ TB/ngày}$ (Raw uncompressed).
* **Tỷ lệ nén Parquet + ZSTD:** Giảm 60% dung lượng $\rightarrow$ Còn **2.0 TB/ngày** (Compressed).
* **Thời gian lưu trữ Raw (Bronze/Silver):** 7 ngày cố định $\rightarrow$ Dung lượng steady-state luôn giữ ở mức: $7 \times 2.0\text{ TB} = \mathbf{14.0\text{ TB}}$.

### 💰 Chi tiết Bảng giá AWS (US East, cập nhật 2026):
* **S3 Standard:** \$0.023 / GB / tháng (\$23.55 / TB / tháng).
* **S3 Standard-IA (Infrequent Access):** \$0.0125 / GB / tháng (\$12.80 / TB / tháng).
* **S3 PUT Requests:** \$0.005 / 1,000 requests.
* **S3 GET Requests:** \$0.0004 / 1,000 requests.

---

### 🧮 Phép tính chi phí hàng tháng:

| Hạng mục chi phí | Khối lượng / Thông số | Đơn giá | Thành tiền / tháng |
|---|---|---|---:|
| **1. Storage: Bronze Raw (7 ngày)** | 14 TB steady-state (S3 Standard) | \$23.55 / TB | **\$329.70** |
| **2. Storage: Silver Cleaned (7 ngày)** | 14 TB (3 ngày Standard + 4 ngày IA) | Trung bình \$17.40 / TB | **\$243.60** |
| **3. Storage: Gold Metrics (365 ngày)** | 3.5 MB/ngày $\times$ 365 = 1.27 GB | \$0.023 / GB | **\$0.03** |
| **4. S3 PUT Operations** | Micro-batch 5 phút: 288 batches/ngày $\times$ 50 files = 14.4K PUTs/ngày = 432K PUTs/tháng | \$0.005 / 1K PUT | **\$2.16** |
| **5. S3 GET Operations (Dashboard)** | 100 internal users $\times$ refresh 5 phút = 864K GETs/tháng | \$0.0004 / 1K GET | **\$0.35** |
| **6. Streaming Compute (Ingest & Clean)** | 2 $\times$ AWS EMR Serverless vCPU (hoặc Flink ECS Fargate) | \$0.0405 / vCPU-hr | **\$350.00** |
| **7. Compaction & Maintenance Compute** | Daily scheduled job: 1 giờ / ngày on m6g.xlarge | \$0.154 / hr | **\$4.62** |
| **8. Buffer Dự phòng & Data Transfer In/Out** | Egress nội bộ VPC + S3 Lifecycle transition fees | Flat estimate | **\$150.00** |
| **TỔNG CỘNG HÀNG THÁNG** | | | **\$1,080.46 / tháng** |

> 🎯 **Kết luận FinOps:**  
> Tổng chi phí vận hành ước tính là **~\$1,080 / tháng**, chỉ chiếm **~21.6%** so với trần ngân sách tối đa **\$5,000 / tháng**. Hệ thống còn dư địa hơn **\$3,900/tháng** để mở rộng băng thông hoặc tăng thời gian lưu trữ khi lưu lượng tăng trưởng gấp 4 lần.

---

## 6. One-Week MVP Implementation Slice

Để chứng minh tính khả thi của kiến trúc trước Senior Design Review, team triển khai **lát cắt MVP trong 1 tuần** (Vertical Slice):

* **Ngày 1–2 (Ingestion & Tokenization Spike):**
  * Dựng pipeline Flink/Spark micro-batch 5 phút nhận 1 triệu request mẫu.
  * Tích hợp hàm `hmac_sha256_salted()` kiểm chứng 0% cleartext PII lọt xuống tầng Bronze.
* **Ngày 3–4 (Medallion & Z-Order Clustering Benchmark):**
  * Ghi bảng Silver và thực thi `OPTIMIZE silver.llm_calls ZORDER BY (tenant_id)`.
  * Đo đạc chứng minh tỉ lệ **file pruning $\ge 15\times$** khi query lọc theo tenant cụ thể.
* **Ngày 5 (Gold Table & DuckDB Serverless Serving):**
  * Viết job SQL tổng hợp số liệu p50/p95 latency và cost theo cửa sổ 5 phút.
  * Kết nối DuckDB query trực tiếp từ S3 Parquet đạt p95 query latency < **200 ms**.
* **Ngày 6–7 (Lifecycle & Failure Testing):**
  * Giả lập crash job để tạo 5 orphan files $\rightarrow$ Chạy script `find_and_remove_orphans` dọn sạch 100%.
  * Kiểm chứng S3 Lifecycle rule xóa dữ liệu sau 7 ngày thành công.

---

*Tài liệu này được bảo vệ trong Senior Design Review — AICB-P2T2 Track 2 Lakehouse Architecture.*
