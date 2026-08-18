# Bonus Challenge Architecture Brief: CDC Ride-Hailing Việt Nam → Lakehouse (Tuân Thủ Nghị Định 13/2023/NĐ-CP)

## 1. Problem Statement
Đội ngũ kỹ thuật vận hành hệ thống gọi xe công nghệ tại Việt Nam phục vụ **100 triệu chuyến xe/năm**, chịu tải đỉnh **30,000 writes/giây** trên cơ sở dữ liệu production (Oracle DB).
Dữ liệu sinh ra chứa thông tin định danh cá nhân (PII) nhạy cảm: Số điện thoại, Số CMND/CCCD, Tọa độ GPS thời gian thực, Tên người dùng và Thông tin thẻ thanh toán — chịu sự điều chỉnh nghiêm ngặt của **Nghị định 13/2023/NĐ-CP**.

**Thách thức chính:**
- **SLA Ingestion & Analytics:** Dashboard analytics phải cập nhật dữ liệu gần thời gian thực (SLA refresh $<60$ giây từ source commit), ad-hoc analytical queries đạt latency p95 $<1$ giây.
- **Xử lý Late-arriving Data:** Thiết bị tài xế/hành khách hay mất kết nối mạng ở các khu vực sóng yếu (tỉnh xa, đường hầm), dẫn tới sự kiện gửi bù muộn hàng giờ hoặc hàng ngày.
- **Tuân thủ Pháp lý (Decree 13 Compliance):** 100% PII phải được Mã hóa/Mã hóa một chiều (Tokenization/Pseudonymization) ngay tại tầng Landing (Bronze) trước khi bất kỳ nhà phân tích nào có quyền xem; phải duy trì Audit Log không thể sửa đổi (Immutable Audit Ledger) cho từng lượt truy vấn dữ liệu PII; hỗ trợ quyền xóa dữ liệu cá nhân (Right to be forgotten) với latency $<24$ giờ.

---

## 2. Architecture Diagram

```
+-----------------------------------------------------------------------------------+
|                            INGESTION & LANDING LAYER                              |
|                                                                                   |
|  [Oracle DB Peak 30k w/s] ---> [Debezium CDC] ---> [Kafka Event Streams]          |
|                                                           |                       |
|                                                           v                       |
|                                              +-------------------------+          |
|                                              | Bronze Layer (Delta)    |          |
|                                              | - Raw CDC Events        |          |
|                                              | - Tokenized PII Inline  |          |
|                                              | - Partition by day(ts)  |          |
|                                              +-------------------------+          |
+-----------------------------------------------------------|-----------------------+
                                                            |
                                                            v
+-----------------------------------------------------------------------------------+
|                           SILVER LAYER (CURATED & DEDUP)                          |
|                                                                                   |
|  +-----------------------------------------------------------------------------+  |
|  | Silver Layer (Delta CDF Enabled)                                            |  |
|  | - MERGE INTO table ON trip_id                                               |  |
|  | - Late Data Handling: WHEN MATCHED AND src.ts > tgt.ts                      |  |
|  | - De-pseudonymized PII restricted via Column-level Security (RLS/CLS)       |  |
|  +-----------------------------------------------------------------------------+  |
+-----------------------------------------------------------|-----------------------+
                                                            |
                                                            v
+-----------------------------------------------------------------------------------+
|                            GOLD LAYER & SERVING LAYER                             |
|                                                                                   |
|  +-------------------------------------+      +---------------------------------+ |
|  | Gold Aggregates (Iceberg + Polaris) |      | Access Audit Ledger (Delta CDF) | |
|  | - Real-time KPI Dashboards (<60s SLA) |      | - Log reader identity & reason  | |
|  | - Z-Ordered by (driver_id, city_id) |      | - Decree 13 Compliance Audit   | |
|  +-------------------------------------+      +---------------------------------+ |
+-----------------------------------------------------------------------------------+
```

---

## 3. Quyết Định Kiến Trúc Chính & Các Phương Án Đã Loại

### Quyết định 1: Chọn Delta Lake với Change Data Feed (CDF) làm Storage Format chính cho tầng Bronze & Silver
- **Tôi chọn:** Delta Lake v3.x (hỗ trợ Delta CDF và Deletion Vectors).
- **Tôi loại Iceberg cho tầng Bronze:** Vì Delta CDF ghi nhận trực tiếp bản tin `_change_type` (`insert`, `update_preimage`, `update_postimage`, `delete`) cực kỳ hiệu quả cho CDC stream ingestion từ Debezium với latency thấp.
- **Tôi loại Hudi:** Vì độ phức tạp vận hành cao và sinh nhiều file nhỏ đòi hỏi hạ tầng Spark cluster đắt đỏ duy trì liên tục.

### Quyết định 2: Chiến lược Tokenization / Pseudonymization PII ngay tại Bronze Landing
- **Tôi chọn:** Hash-based Tokenization kèm Pepper bí mật lưu trong AWS KMS / HashiCorp Vault (`HMAC-SHA256(PII + KMS_Pepper)`).
- **Tôi loại Plaintext Storage tại Bronze:** Vì nếu dữ liệu Bronze bị rò rỉ hoặc lộ bucket S3, PII chưa mã hóa sẽ vi phạm trực tiếp Điều 13 & Điều 17 Nghị định 13/2023/NĐ-CP.
- **Tôi loại Symmetric Encryption (AES-256) cho tất cả các trường:** Vì AES không bảo toàn tính truy vấn đồng nhất (deterministic equality joins) trên các cột thường xuyên join như `driver_phone`.

### Quyết định 3: Xử lý Late-Arriving Data bằng MERGE Conditional Logic
- **Tôi chọn:** Sử dụng `MERGE INTO silver.trips tgt USING bronze_stream src ON tgt.trip_id = src.trip_id WHEN MATCHED AND src.event_timestamp > tgt.event_timestamp THEN UPDATE SET *`.
- **Tôi loại Append-only với Query-time Deduplication (`ROW_NUMBER() OVER (...)`):** Vì truy vấn ad-hoc ở tầng Gold sẽ phải scan toàn bộ lịch sử để lấy trạng thái mới nhất, vi phạm SLA latency p95 $<1$ giây khi bảng đạt hàng trăm triệu dòng.
- **Tôi loại Last-Write-Wins tại Kafka Consumer:** Vì Kafka partition rebalance hoặc mạng chập chờn có thể làm sai lệch thứ tự sự kiện đến.

### Quyết định 4: Catalog & Governance sử dụng Apache Polaris (REST Catalog)
- **Tôi chọn:** Apache Polaris (Vendor-neutral REST Catalog).
- **Tôi loại Databricks Unity Catalog độc quyền:** Tránh vendor lock-in, cho phép các engine đa dạng (DuckDB, Trino, PySpark, Polars) truy vấn cùng một metastore thống nhất.
- **Tôi loại Hive Metastore truyền thống:** Hive metastore không hỗ trợ Iceberg REST spec, không có fine-grained Role-Based Access Control (RBAC) chuẩn hóa cấp cột/dòng.

### Quyết định 5: Định kỳ Maintenance & Partitioning Strategy
- **Tôi chọn:** Phân vùng Bronze/Silver theo `date(event_timestamp)`, Z-Ordering tầng Silver theo `(driver_id, geohash_zone)`.
- **Tôi loại Partition theo `hour` hoặc `driver_id`:** Tạo ra hiện tượng Small-File Problem nghiêm trọng ($>500,000$ files/ngày), làm sập Metastore và tăng chi phí scan S3 API get-object.

---

## 4. Kịch Bản Thất Bại (Failure Modes & Mitigation)

1. **Failure Mode 1: Out-of-order Late Events làm ghi đè dữ liệu mới hơn (Stale Overwrite)**
   - *Phát hiện:* Kiểm tra chỉ số `updated_at` trong Silver nhỏ hơn bản ghi hiện tại.
   - *Xử lý & Rollback:* Áp dụng điều kiện `WHEN MATCHED AND src.event_timestamp > tgt.event_timestamp`. Nếu có sự cố ghi lỗi tập thể, dùng Delta **Time Travel** `RESTORE TABLE silver.trips TO VERSION AS OF <version_before_incident>`.

2. **Failure Mode 2: Yêu cầu Xóa Dữ liệu Cá nhân (Right to be Forgotten - Decree 13 Art. 16)**
   - *Phát hiện:* Nhận request xóa dữ liệu từ khách hàng/tài xế qua cổng chăm sóc khách hàng.
   - *Xử lý & Rollback:* Thực hiện `DELETE FROM silver.trips WHERE customer_token = :token`. Delta Lake sử dụng **Deletion Vectors** để đánh dấu xóa mềm không tốn chi phí rewrite file Parquet ngay lập tức, sau đó job đêm chạy `VACUUM` với retention $0$ giờ (sau khi thu hồi log) để xóa triệt để file vật lý chứa PII trên S3.

3. **Failure Mode 3: Trùng lặp sự kiện do Kafka Consumer Retry (At-least-once Delivery)**
   - *Phát hiện:* Số dòng Bronze tăng đột biến nhưng số bản ghi duy nhất không đổi.
   - *Xử lý:* Tầng Silver thực hiện MERGE dedup theo `trip_id` độc nhất, đảm bảo tính Idempotency $100\%$.

---

## 5. Ước Lượng Chi Phí (Back-of-Envelope Cost Estimate)

### Giả định Quy mô:
- Chuyến xe: 100 triệu chuyến/năm $\approx 274,000$ chuyến/ngày.
- Kích thước dòng CDC: 2 KB/chuyến.
- Dung lượng dữ liệu thô: $274,000 \times 2\text{ KB} = 548\text{ MB/ngày} \approx 200\text{ GB/năm}$.
- Hệ số nhân log & indexes: $3\times \rightarrow 600\text{ GB/năm}$.

### Tính toán Chi phí Hàng tháng (AWS S3 & Serverless Compute):
1. **Storage (AWS S3 Standard & S3 Glacier Instant Retrieval):**
   - $600\text{ GB} \times \$0.023/\text{GB-tháng} \approx \$13.8/\text{tháng}$.
2. **Compute Ingestion & Compaction (EKS / Serverless Spark):**
   - Micro-batch Spark Ingestion & Maintenance (`OPTIMIZE/Z-ORDER` 2 lần/ngày): $4\text{ vCPU} \times 2\text{ giờ/ngày} \times \$0.04/\text{vCPU-giờ} \times 30\text{ ngày} \approx \$9.6/\text{tháng}$.
3. **Kafka & Debezium CDC Cluster:**
   - Managed Kafka (MSK) small cluster: $\approx \$150/\text{tháng}$.
4. **Tổng Chi phí Ước tính:** $\approx \mathbf{\$175 - \$200\text{ / tháng}}$ (cực kỳ tối ưu nhờ thiết kế Lakehouse gọn nhẹ).

---

## 6. Slices MVP Đầu Tiên (First Week Build)

Tập trung xây dựng slice nhỏ nhất có thể chạy end-to-end:
1. **Ngày 1-2:** Dựng `scripts/generate_cdc_data.py` mô phỏng luồng CDC 10,000 chuyến xe kèm PII giả định.
2. **Ngày 3-4:** Viết module `tokenization.py` mã hóa PII bằng HMAC-SHA256 và luồng ghi `write_deltalake` vào tầng Bronze.
3. **Ngày 5-6:** Thực thi pipeline `MERGE INTO` từ Bronze sang Silver xử lý dedup và late-arriving events.
4. **Ngày 7:** Viết test kiểm chứng tuân thủ: Thử nghiệm xóa PII một tài xế và chạy `VACUUM` chứng minh PII hoàn toàn biến mất trên đĩa.
