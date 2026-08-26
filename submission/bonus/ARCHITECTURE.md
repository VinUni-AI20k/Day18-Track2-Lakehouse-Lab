# Bonus Challenge: CDC từ ride-hailing Việt Nam → Lakehouse (tuân thủ Decree 13)

## 1. Problem Statement
Một ứng dụng gọi xe tại Việt Nam cần xây dựng pipeline CDC (Change Data Capture) từ Oracle DB (Hệ thống điều vận) sang Data Lakehouse để phục vụ phân tích. 
- **Scale:** 100 triệu chuyến/năm, peak 30K writes/giây. 
- **Constraints:** Dữ liệu chứa thông tin nhạy cảm (PII: số điện thoại, CMND, tọa độ GPS) phải tuân thủ Nghị định 13 bảo vệ Dữ liệu Cá nhân.
- **SLA:** Cập nhật Dashboard (Silver/Gold) trong <60 giây từ khi source commit. Truy vấn Ad-hoc (p95) < 1 giây.
- **Data Quality:** Xử lý sự kiện muộn (late-data) từ các cuốc xe ở vùng sóng yếu.
Đây là bài toán khó vì phải cân bằng giữa tốc độ Stream < 60s, chi phí xử lý (Streaming Ingestion), và yêu cầu Audit/Redact khắt khe của Nghị định 13.

## 2. Architecture Diagram

```ascii
[Oracle DB] --> (Debezium/Kafka) --> [Bronze Layer] 
                                      - Raw JSON/Avro 
                                      - Append Only
                                      - PII Tokenizer Service (Redaction)
                                      |
                                      V
                             [Silver Layer (Delta)]
                               - SCD Type 2 History
                               - MERGE WHEN MATCHED (Late Data Handling)
                               - Z-Order by (driver_id, date)
                                      |
                                      V
                              [Gold Layer (Delta)]
                               - Aggregated Views 
                               - RLS (Row-Level Security)
                               - Dashboards (Superset/Tableau)
```

## 3. Quyết định kiến trúc & Alternatives bị loại
1. **Lựa chọn Table Format:**
   - **Chọn:** Delta Lake.
   - **Loại:** Apache Iceberg vì Delta Lake hỗ trợ tính năng Change Data Feed (CDF) tốt hơn khi làm việc rảnh mạch với Structured Streaming của Spark cho bài toán CDC.
2. **Xử lý PII (Nghị định 13):**
   - **Chọn:** Mã hóa Tokenization động (AES/GCM) ngay khi data đáp xuống Bronze Landing zone. 
   - **Loại:** Dynamic Data Masking ở tầng View (Gold) vì nếu Bronze bị lộ, Data PII nguyên bản sẽ vi phạm luật ngay lập tức.
3. **Tiếp nhận Late-Data:**
   - **Chọn:** Dùng cú pháp `MERGE INTO ... WHEN MATCHED AND src.ts > target.ts THEN UPDATE`.
   - **Loại:** Xóa (Delete) và Insert lại Batch, vì thao tác này gây khóa bảng nặng nề và làm tăng độ trễ vượt qua SLA 60s.
4. **Chiến lược Partitioning ở Silver:**
   - **Chọn:** Partition theo `year_month` kết hợp `OPTIMIZE ZORDER BY (driver_id)`.
   - **Loại:** Partition theo ngày (`date`) vì 100 triệu chuyến/năm chia theo ngày sẽ tạo ra hiện tượng "Small files problem", nhất là khi chạy Streaming liên tục mỗi 60s.
5. **Catalog & Governance:**
   - **Chọn:** Unity Catalog (Databricks) hoặc Polaris Catalog để làm Data Lineage tập trung và cấp quyền Table ACL cho Audit log.
   - **Loại:** Hive Metastore cũ kỹ không hỗ trợ Fine-grained access control (Cấp quyền tới Column, Row).

## 4. Failure Modes & Recovery
1. **Lỗi mạng, Debezium nhồi 1 triệu Duplicate data cũ vào Bronze (3 giờ sáng):**
   - *Detect:* Spark Streaming job báo metric rows bất thường tăng vọt.
   - *Rollback:* Mặc dù Bronze bị dội data, truy vấn Silver dùng lệnh MERGE dựa trên `src.ts > target.ts` sẽ âm thầm bỏ qua (Skip) các data lỗi thời. Do vậy Silver không bị ảnh hưởng.
2. **Schema Drift (Oracle đổi tên cột từ `driver_phone` sang `phone_number`):**
   - *Detect:* Job failed tạị Bronze do Strict Schema.
   - *Recovery:* Dùng Delta Lake `schema_mode="merge"` để Auto-Evolve schema, cập nhật Data Contract. Chạy `RESTORE` về version trước đó nếu lây nhiễm xấu tới Silver.
3. **Thất thoát khóa Tokenization, lộ PII (Vi phạm Nghị Định 13):**
   - *Detect:* Alert qua Audit Logs báo có phiên truy cập bất thường đọc cột Tokenize trực tiếp từ Data Engineer nghỉ việc.
   - *Recovery:* `RESTORE` quyền KMS (Key Management), đồng thời xoay (rotate) key giải mã. Truy vế thông qua `history()` của Delta Table.

## 5. Ước lượng chi phí (Back-of-envelope) cho Cloud (AWS)
- **Data Size:** 100M trips/year * 500 Bytes/trip ≈ 50GB thô/năm. Thực tế nở ra do SCD type 2 và compaction => ~200GB/năm.
- **Storage:** S3 Standard (Bronze/Silver/Gold) = 200GB x 3 layers x $0.023/GB = ~$14/tháng (Rất rẻ).
- **Compute:** Spark Streaming cluster "always-on" nhỏ để đáp ứng SLA 60s (2 Nodes i3.xlarge) = ~$500/tháng.
- **Tổng cộng:** ~$515/tháng. (Đủ tiết kiệm và Scale tự nhiên).

## 6. Lộ trình Build MVP (Tuần 1)
Chỉ dựng duy nhất Pipeline từ CSV tĩnh -> Debezium Mock -> Bronze -> Silver Merge. Tập trung test kỹ nhất tính năng **Tokenize cột SĐT của khách hàng ở Bronze** và đảm bảo câu lệnh MERGE bỏ qua được Record cũ khi chạy lại (Idempotent). Chấp nhận query chưa nhanh nhưng dữ liệu phải Tuân thủ pháp luật (Compliance First).