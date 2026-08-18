# Kiến Trúc Data Lakehouse Cho Hệ Thống Ride-Hailing Việt Nam Tuân Thủ Nghị Định 13/2023/NĐ-CP

> **Author:** Tran Duc Thien

> **Student ID:** 2A202602032

> **Topic:** C — CDC từ Ride-Hailing Việt Nam $\rightarrow$ Lakehouse (Tuân thủ Nghị định 13)  
> **Deliverable:** Architecture Decision Record & Design Review Document

---

## 1. Problem Statement

Hệ thống đặt xe công nghệ tại Việt Nam xử lý **100 triệu chuyến xe/năm**, với lưu lượng ghi đạt đỉnh **30.000 writes/giây (peak WPS)** từ cụm Oracle OLTP. Dữ liệu chứa thông tin cá nhân nhạy cảm (PII) theo **Nghị định 13/2023/NĐ-CP** (SĐT, CCCD/CMND, tọa độ GPS thời gian thực, thông tin tài khoản ngân hàng).

**Thách thức cốt lõi:**
1. **SLA thời gian thực:** Dashboard phân tích kinh doanh và điều phối xe cần cập nhật trong vòng **60 giây** kể từ khi commit tại OLTP; các truy vấn phân tích ad-hoc yêu cầu độ trễ $p95 < 1\text{ giây}$.
2. **Xử lý sự kiện đến muộn (Late-arriving & Out-of-order data):** Sự cố mất sóng viễn thông 4G tại các tỉnh ngoại thành khiến các gói tin cập nhật trạng thái chuyến xe (`COMPLETED`, `CANCELLED`, `RATING`) gửi trễ hàng giờ, có nguy cơ ghi đè làm sai lệch trạng thái mới hơn.
3. **Tuân thủ pháp lý nghiêm ngặt (Nghị định 13 & Quyền xóa dữ liệu):** Phải đảm bảo mã hóa/tokenization PII ngay từ tầng tiếp nhận (Bronze landing), lưu vết kiểm toán (audit log) 100% lượt truy cập PII, và đáp ứng yêu cầu xóa dữ liệu cá nhân (Right-to-Erasure) trong 72 giờ mà không làm gián đoạn các pipeline phân tích tổng hợp.

---

## 2. Architecture Diagram

```
+---------------------------------------------------------------------------------------------------------------+
| INGESTION & CAPTURE LAYER                                                                                     |
|  +------------------+         +------------------+         +-------------------+                              |
|  | Oracle OLTP DB   | ------> | Debezium CDC     | ------> | Apache Kafka      |                              |
|  | (100M trips/yr)  |  (logs) | (Schema Registry)|         | (trip_events topic|                              |
|  +------------------+         +------------------+         +---------+---------+                              |
+----------------------------------------------------------------------|----------------------------------------+
                                                                       | Spark Structured Streaming (micro-batch 30s)
                                                                       v
+---------------------------------------------------------------------------------------------------------------+
| MEDALLION STORAGE LAYER (Object Storage / S3 / MinIO)                                                         |
|                                                                                                               |
|  +---------------------------------------------------------------------------------------------------------+  |
|  | BRONZE LAYER: `bronze_trips_cdc` (Raw CDC Stream + Salted HMAC Tokenization)                           |  |
|  | - Append-only, Raw Payload + Kafka Metadata (_op, _ts_ms, _source_scn).                                 |  |
|  | - Deterministic HMAC-SHA256 Tokenization cho SĐT & CCCD; Vault lưu Key riêng biệt.                      |  |
|  | - Format: Delta Lake (Enable CDF = true, Retention = 14 days, Z-Order: _ts_ms).                        |  |
|  +----------------------------------------------------+----------------------------------------------------+  |
|                                                       |                                                       |
|                                                       | MERGE INTO (WHEN MATCHED AND src.ts > tgt.ts)         |
|                                                       | Deletion Vectors enabled, Daily Compaction            |
|                                                       v                                                       |
|  +---------------------------------------------------------------------------------------------------------+  |
|  | SILVER LAYER: `silver_trips` (Curated Single-Source-of-Truth + SCD Type 2)                              |  |
|  | - Clean Schema, Validated Geo-coordinates, De-duplicated trip records.                                  |  |
|  | - Partition: `date(trip_start)` | Z-Order: `[customer_token, driver_token, service_type]`.             |  |
|  | - Deletion Vectors: Xóa PII không cần rewrite toàn bộ Parquet file.                                     |  |
|  +----------------------------------------------------+----------------------------------------------------+  |
|                                                       |                                                       |
|                                                       | Scheduled ETL / Continuous Aggregation (Hourly/Daily) |
|                                                       v                                                       |
|  +---------------------------------------------------------------------------------------------------------+  |
|  | GOLD LAYER: `gold_trip_metrics_hourly`, `gold_driver_performance`, `gold_privacy_audit`                 |  |
|  | - Aggregates hoàn toàn không chứa PII: Doanh thu, Tỷ lệ hoàn thành, Nhiệt độ nhu cầu (Demand Heatmap).|  |
|  | - Partition: `date`, `h3_hex_resolution_8` | Optimized for sub-second BI dashboard querying.            |  |
|  +---------------------------------------------------------------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------------------+
                                                       |
+------------------------------------------------------v--------------------------------------------------------+
| CONSUMPTION & CONTROL PLANE (Governance, Security, Query Engines)                                             |
|  +------------------------------------------------+  +-----------------------------------------------------+  |
|  | Apache Polaris (REST Catalog) & Apache Ranger  |  | Trino / DuckDB / Spark SQL Engines                  |  |
|  | - Column-level RBAC & ABAC Dynamic Masking     |  | - Sub-second operational BI & Real-time Dispatch    |  |
|  | - Audit Trail & OpenLineage Metadata Lineage   |  | - Machine Learning Feature Store (ETA, Pricing ML)  |  |
|  +------------------------------------------------+  +-----------------------------------------------------+  |
+---------------------------------------------------------------------------------------------------------------+
```

---

## 3. Quyết định Kiến trúc & Trade-offs (Major Architectural Decisions)

### Quyết định 1: Định dạng bảng (Table Format) — Chọn Delta Lake 3.1
* **Tôi chọn:** **Delta Lake 3.1**.
* **Tôi loại Apache Iceberg vì:** Dù Iceberg có chuẩn REST Catalog xuất sắc và Hidden Partitioning tốt, việc hỗ trợ cơ chế Deletion Vectors và hiệu năng xử lý các lệnh `MERGE INTO` tần suất cao (30s/batch từ CDC) của Delta Lake qua Rust engine (`delta-rs`) và Spark native engine đạt thông lượng (throughput) cao hơn ~35% trong các pipeline streaming CDC ghi đè liên tục.
* **Tôi loại Apache Hudi vì:** Hudi có ưu thế về Merge-on-Read (MoR) nhưng cấu trúc metadata phức tạp, phụ thuộc nặng nề vào Spark/JVM runtime, gây khó khăn cho việc tích hợp các engine truy vấn embedded siêu nhẹ như DuckDB/Polars phục vụ API Gateway.

### Quyết định 2: Xử lý CDC & Cập nhật trễ (Late-Arriving Data) — Chọn Delta MERGE với Version Predicate
* **Tôi chọn:** **`MERGE INTO` với điều kiện `WHEN MATCHED AND src.event_timestamp > tgt.event_timestamp` kết hợp Deletion Vectors**.
* **Tôi loại Copy-on-Write (CoW) truyền thống vì:** Ghi lại toàn bộ file Parquet cho mỗi dòng thay đổi sẽ dẫn đến tình trạng "Write Amplification" trầm trọng ở mức 30.000 writes/giây, làm bùng nổ chi phí I/O lưu trữ S3.
* **Tôi loại Append-only View Deduplication (Query-time Deduplication) vì:** Việc quét toàn bộ lịch sử và dedup bằng `ROW_NUMBER()` tại thời điểm truy vấn làm latency truy vấn vượt quá 5 giây, vi phạm SLA $p95 < 1\text{s}$ của dashboard điều phối.

### Quyết định 3: Chiến lược Bảo vệ Dữ liệu PII (Nghị định 13) — Chọn Ingestion-Time HMAC Tokenization
* **Tôi chọn:** **Tokenization bằng Salted HMAC-SHA256 kết hợp HashiCorp Vault ngay tại tầng Ingestion (Spark Streaming $\rightarrow$ Bronze)**.
* **Tôi loại Dynamic Masking thuần túy tại Query Time vì:** Nếu PII thô được ghi trực tiếp vào Bronze/Silver storage ở dạng unencrypted text, bất kỳ lỗ hổng bảo mật nào ở tầng storage bucket (S3 IAM misconfiguration) hoặc audit dump đều trực tiếp vi phạm Điều 11 & Điều 26 Nghị định 13.
* **Tôi loại Asymmetric Public-Key Encryption (RSA/AES-GCM cho từng trường) vì:** Chi phí CPU để giải mã khi join dữ liệu lớn giữa bảng chuyến xe và bảng tài xế/khách hàng quá cao (tăng thời gian query lên gấp 4 lần); Tokenization cho phép join trực tiếp trên token mà không cần giải mã.

### Quyết định 4: Chiến lược Phân vùng & Tối ưu Layout (Partitioning & Clustering)
* **Tôi chọn:** **Phân vùng `date(trip_start)` kết hợp Z-Order trên `[customer_token, driver_token, service_type]`**.
* **Tôi loại Phân vùng theo Giờ (`year/month/day/hour`) vì:** Tạo ra hàng chục nghìn file nhỏ (Small-Files Problem) cho 100M chuyến/năm, gây nghẽn driver catalog khi scan metadata.
* **Tôi loại Phân vùng theo Địa lý Tỉnh/Thành (`province_id`) vì:** Gây lệch tải trầm trọng (Data Skewness) do TP.HCM và Hà Nội chiếm >80% lưu lượng, dẫn đến tình trạng partition quá lớn trong khi các tỉnh khác có quá nhiều micro-files.

### Quyết định 5: Control Plane & Governance — Chọn Apache Polaris (REST Catalog) + OpenLineage
* **Tôi chọn:** **Apache Polaris (mở, hỗ trợ REST spec) tích hợp OpenLineage**.
* **Tôi loại Databricks Unity Catalog vì:** Nguy cơ vendor lock-in; chi phí DBU đắt đỏ khi phục vụ hàng trăm microservices độc lập truy vấn ngoài hệ sinh thái Databricks.
* **Tôi loại Hive Metastore truyền thống vì:** HMS không hỗ trợ schema evolution an toàn (dễ crash khi rename column), không có multi-table ACID transaction, và không hỗ trợ metadata catalog federation cho các engine hiện đại (DuckDB/Trino).

---

## 4. Failure Modes & Kịch bản 3:00 AM (Disaster Recovery & Rollback)

### Kịch bản 1: Mất mạng diện rộng tại tỉnh xa $\rightarrow$ Dữ liệu sự kiện cũ dội về ghi đè trạng thái mới
* **Triệu chứng lúc 3:00 AM:** 50.000 chuyến xe đã hoàn thành (`COMPLETED`) bị chuyển ngược về trạng thái `ASSIGNED` do trạm BTS tỉnh khôi phục mạng và gửi dồn các event CDC cũ bị nghẽn từ 6 tiếng trước.
* **Cơ chế Phát hiện:** Alert giám sát metric: `count(status_retrograde_events) > 0` và `min(src.event_timestamp) < current_timestamp - 4 hours`.
* **Khắc phục & Rollback:** 
  1. Pipeline Silver được bảo vệ bởi logic `WHEN MATCHED AND src.event_timestamp > tgt.event_timestamp THEN UPDATE`.
  2. Nếu có lỗi cấu hình predicate, thực hiện **Delta Time Travel Rollback**:
     ```python
     dt = DeltaTable.forPath(spark, "s3://lakehouse/silver_trips")
     # Khôi phục bảng về commit trước thời điểm BTS dội dữ liệu (v1420)
     dt.restoreToVersion(1420)
     ```
  3. Chạy lại backfill micro-batch với predicate lọc thời gian chính xác.

### Kịch bản 2: Debezium đẩy Schema Breaking Change (Oracle đổi kiểu dữ liệu cột)
* **Triệu chứng lúc 3:00 AM:** DBA chạy migration thêm cột `tax_code VARCHAR2(50)` và đổi `fare_amount` từ `NUMBER(10,2)` sang `NUMBER(12,4)`, khiến consumer streaming bị crash (`SchemaMismatchException`).
* **Cơ chế Phát hiện:** Pipeline dead-letter queue (DLQ) alert: `streaming_consumer_lag > 50,000 records`.
* **Khắc phục:**
  1. Tầng Bronze sử dụng chế độ `schema_mode="merge"` (`spark.databricks.delta.schema.autoMerge.enabled = true`), tự động nạp cột mới vào Delta log mà không dừng stream.
  2. Tầng Silver áp dụng **Schema Evolution** an toàn của Delta/Iceberg, giữ nguyên `field_id` cho cột tiền tệ và ghi nhận schema log version mới, sau đó replay micro-batch từ DLQ.

### Kịch bản 3: Yêu cầu Xóa Dữ liệu Cá nhân Khẩn cấp (Right-to-Erasure — Nghị định 13 Art. 16)
* **Triệu chứng:** Khách hàng hoặc cơ quan quản lý yêu cầu xóa toàn bộ lịch sử định danh của `customer_id = 998811` trong vòng 24h.
* **Cơ chế Thực thi:**
  1. Xóa khóa salt/token mapping trong Key Management Vault $\rightarrow$ Toàn bộ token lịch sử trong Lakehouse ngay lập tức trở thành *irreversible anonymous hash* (ẩn danh vĩnh viễn theo chuẩn Điều 17).
  2. Kích hoạt lệnh xóa vật lý trên tầng Silver:
     ```sql
     DELETE FROM silver_trips WHERE customer_token = 'token_998811';
     ```
  3. Nhờ **Deletion Vectors**, lệnh `DELETE` hoàn tất trong $< 2\text{ giây}$ mà không cần viết lại Parquet data files.
  4. Chạy job bảo trì `VACUUM silver_trips RETAIN 0 HOURS` trong cửa sổ bảo trì để dọn sạch các tombstones trên đĩa vật lý.

---

## 5. Ước tính Chi phí Back-of-the-Envelope (FinOps Math)

### Quy mô Dữ liệu:
* **Lưu lượng:** $100.000.000\text{ chuyến/năm} \approx 274.000\text{ chuyến/ngày}$.
* **Số event CDC:** Mỗi chuyến sinh trung bình 15 trạng thái (Booked, Matching, Driver Accepted, Arrived, In-Transit GPS pings, Completed, Payment, Rating) $\rightarrow 4.100.000\text{ events/ngày} \approx 48\text{ events/giây}$ (Peak: $30.000\text{ writes/giây}$).
* **Dung lượng raw:** $2\text{ KB/event} \times 4.1\text{M events/ngày} \approx 8.2\text{ GB/ngày raw} \rightarrow \approx 2.5\text{ GB/ngày}$ sau nén Parquet Zstandard $\approx 0.9\text{ TB/năm}$.
* **Lưu trữ GPS Traces (Multimodal/Telematics):** $\approx 10\text{ TB/năm}$.

### Bảng Chi phí Hàng tháng (AWS Region ap-southeast-1 / Singapore):

| Hạng mục | Quy mô / Thông số tính toán | Đơn giá | Thành tiền / Tháng |
|---|---|---|---:|
| **S3 Storage (Hot Tier - Standard)** | 5 TB (Bronze 14d + Silver 90d + Gold 1yr) | \$0.025 / GB-tháng | **\$125 / tháng** |
| **S3 Storage (Cold Tier - Glacier Instant)**| 20 TB (Lịch sử Bronze & Silver cũ > 90 ngày) | \$0.004 / GB-tháng | **\$80 / tháng** |
| **S3 API Requests (PUT/LIST/GET)** | 50M PUT (micro-batches) + 200M GET/s | \$0.005 / 1K PUT, \$0.0004 / 1K GET | **\$330 / tháng** |
| **Ingestion & Streaming Compute** | 2 × m6i.xlarge (4 vCPU, 16 GB RAM) EMR/EKS 24/7 | \$0.192 / node-giờ × 2 × 730h | **\$280 / tháng** |
| **Compaction & Maintenance Jobs** | 1 × r6i.2xlarge chạy 2 giờ/ngày (Spot instance) | \$0.15 / giờ × 60h | **\$9 / tháng** |
| **Query Engine (Trino/DuckDB Cluster)** | 4 × c6i.2xlarge phục vụ BI & Dispatch APIs | \$0.34 / node-giờ × 4 × 730h (Savings Plan) | **\$992 / tháng** |
| **Network Egress & Kafka MSK** | 3-broker cluster kafka.m5.large | \$0.21 / broker-giờ × 3 × 730h | **\$460 / tháng** |
| **KMS / Vault & Security Audit Log** | 100M tokenization calls / tháng | \$0.03 / 10K requests | **\$300 / tháng** |
| **TỔNG CỘNG HÀNG THÁNG** | **Toàn bộ hạ tầng Lakehouse 100M trips/yr** | | **~\$2.576 / tháng** |

> **Nhận xét FinOps:** Tổng chi phí lưu trữ và xử lý toàn bộ bài toán 100 triệu chuyến xe chỉ tốn **~\$2.576 / tháng** (chưa tới \$31.000/năm), hoàn toàn nằm trong ngưỡng ngân sách cho phép ($< \$5.000/\text{tháng}$) nhờ cơ chế nén Zstandard, Deletion Vectors giảm I/O rewrite, và vòng đời lưu trữ tự động Hot $\rightarrow$ Cold.

---

## 6. Lộ trình Triển khai MVP 1 Tuần (1-Week Slice)

Không triển khai toàn bộ hệ thống đồ sộ cùng lúc. Đây là kế hoạch shippable MVP trong 5 ngày làm việc:

* **Ngày 1 (Ingestion & Tokenization Spike):** Dựng Debezium CDC ảo bắt sự kiện từ Oracle mock, cấu hình hàm Salted HMAC Tokenization tại Bronze để xác nhận không có plain-text PII nào lọt vào storage.
* **Ngày 2 (Delta MERGE & Late-Data Handling):** Xây dựng bảng Silver, viết câu lệnh `MERGE INTO` có predicate `src.event_timestamp > tgt.event_timestamp`, kiểm thử inject 10.000 sự kiện out-of-order và kiểm tra tính toàn vẹn trạng thái chuyến xe.
* **Ngày 3 (Deletion Vectors & GDPR Erasure):** Cấu hình bật Deletion Vectors (`delta.enableDeletionVectors = true`), kiểm chứng benchmark lệnh `DELETE` khách hàng hoàn tất dưới 2 giây và audit log ghi nhận đầy đủ.
* **Ngày 4 (Gold Aggregations & Query SLA):** Dựng pipeline tổng hợp Gold (doanh thu, heatmap chuyến đi theo H3 Geo-hex) và kết nối DuckDB/Trino, đo lường độ trễ $p95 < 1\text{s}$.
* **Ngày 5 (End-to-End Stress Test & Disaster Rollback):** Giả lập sự cố ngắt kết nối mạng tại 3 tỉnh thành, dồn tải 30.000 events/giây, thực hiện kiểm thử khôi phục Time Travel và đóng gói bàn giao PoC.

---

## 7. Mã Nguồn PoC Kiểm Chứng (Proof-of-Concept)

Toàn bộ cơ chế phức tạp nhất của kiến trúc (HMAC Tokenization, Late-arriving `MERGE`, Deletion Vectors, Change Data Feed, Right-to-Erasure) đã được lập trình và kiểm chứng độc lập tại file [`submission/bonus/poc/poc_decree13_cdc.py`](./poc/poc_decree13_cdc.py).
