# Kiến trúc Lakehouse cho Dữ liệu CDC Ride-hailing Việt Nam (Tuân thủ Nghị định 13)

## 1. Problem Statement
Ứng dụng gọi xe tạo ra **100 triệu chuyến xe/năm** với lưu lượng cập nhật đạt **30K writes/giây** vào giờ cao điểm. Dữ liệu gốc lưu tại Oracle DB. Bài toán đặt ra là chuyển luồng dữ liệu thay đổi (CDC) về Data Lakehouse phục vụ phân tích (analytics) với các ràng buộc:
- **Độ trễ dữ liệu (Freshness):** Dashboard được làm mới trong 60 giây kể từ khi có commit ở DB nguồn.
- **Hiệu năng truy vấn (Latency):** Ad-hoc query đạt p95 < 1 giây.
- **Tuân thủ (Compliance):** Dữ liệu chứa thông tin cá nhân PII của người dùng. Hệ thống phải tuân thủ **Nghị định 13/2023/NĐ-CP**, đáp ứng quyền xóa dữ liệu (Right to Erasure) và hỗ trợ audit quyền truy cập.
- **Dữ liệu đến muộn (Late Data):** Mạng chập chờn khiến event đến không theo thứ tự thời gian.

## 2. Architecture Diagram

```mermaid
flowchart LR
    subgraph Operational
        Oracle[(Oracle DB)]
    end
    
    subgraph Ingestion
        Debezium[Debezium CDC]
        Kafka[Kafka Cluster]
    end
    
    subgraph Lakehouse [Delta Lakehouse]
        Bronze[(Bronze\nRaw CDC, PII Tokenized)]
        Silver[(Silver\nSCD Type 2, Clean)]
        Gold[(Gold\nAggregated, Analytics)]
    end
    
    subgraph Governance & Security
        Vault[HashiCorp Vault\n(Tokenization)]
        UC[Unity Catalog\n(RBAC, Lineage)]
    end

    Oracle -->|Redo Logs| Debezium
    Debezium -->|JSON/Avro| Kafka
    Kafka -->|Spark Structured Streaming\nMicro-batch 30s| Bronze
    Bronze -->|Delta CDF\nLate data MERGE| Silver
    Silver -->|Batch/Stream| Gold
    
    Vault <..>|Shredding/Masking| Bronze
    UC -.-> Lakehouse
```

## 3. Quyết định Kiến trúc & Trade-offs

### 3.1. Định dạng lưu trữ (Table Format)
- **Quyết định: Delta Lake.**
- **Alternatives đã loại:** 
  - *Apache Iceberg:* Delta Lake hỗ trợ Change Data Feed (CDF) tích hợp sâu với Spark Structured Streaming, thuận lợi cho việc xử lý late data qua lệnh `MERGE INTO`.
  - *Apache Hudi:* Write amplification cao hơn Delta ở chế độ Copy-On-Write; quá trình bảo trì phức tạp. 

### 3.2. Quản trị PII (PII Governance)
- **Quyết định: Tokenization tại lớp Bronze kết hợp HashiCorp Vault.**
- **Alternatives đã loại:**
  - *Encryption-at-rest:* Chỉ chống lộ lọt ở mức vật lý, không ngăn chặn truy vấn vào cột chứa PII từ phía Data Analyst.
  - *Dynamic Data Masking:* Gặp khó khăn về chi phí tính toán và yêu cầu rewrite lịch sử khi thực thi quyền xóa dữ liệu (Right to Erasure). Crypto-shredding (xoá key mapping trong Vault) giải quyết triệt để yêu cầu này.

### 3.3. Xử lý Ingestion (Ingestion Path)
- **Quyết định: Spark Structured Streaming với Micro-batch 30s.**
- **Alternatives đã loại:**
  - *Apache Flink:* Không cần thiết do SLA là 60s; giảm độ phức tạp vận hành bằng cách tái sử dụng engine Spark cho cả batch và stream.
  - *Batch ETL (Airflow hourly):* Không đáp ứng được SLA 60s.

### 3.4. Layout & Partitioning lớp Silver
- **Quyết định: Partition theo `date(ts)` và `region_id`. Z-Order theo `driver_id`.**
- **Alternatives đã loại:**
  - *Partition theo `driver_id`:* Nguy cơ over-partitioning (>100K driver) gây lỗi Metastore. Z-order theo `driver_id` là phương án tối ưu để data skipping loại bỏ 90% dữ liệu không liên quan trong các truy vấn đặc thù.

### 3.5. Catalog & Data Lineage
- **Quyết định: Databricks Unity Catalog.**
- **Alternatives đã loại:**
  - *Hive Metastore:* Thiếu Fine-grained Access Control tập trung (Row/Column-level). Cần Unity Catalog để quản lý truy cập và xuất audit log tuân thủ Nghị định 13.

## 4. Kịch bản sự cố (Failure Modes)

1. **Sự cố 1: Dữ liệu cuốc xe từ khu vực xa đến trễ (Mất kết nối mạng).**
   - **Tác động:** Bản ghi trạng thái đến sau bản ghi tổng hợp, có nguy cơ ghi đè sai lệch dữ liệu.
   - **Detection/Rollback:** Logic `MERGE WHEN MATCHED AND src.ts > tgt.ts` tự động chỉ cập nhật khi event muộn có timestamp lớn hơn trạng thái hiện tại; không yêu cầu can thiệp thủ công.
2. **Sự cố 2: Lộ lọt PII (Thông tin khách hàng bị log nhầm vào cột ghi chú).**
   - **Tác động:** Vi phạm quy định bảo vệ dữ liệu cá nhân.
   - **Detection/Rollback:** Unity Catalog Lineage cảnh báo khi dữ liệu bị access sai luồng. Sử dụng **Time Travel** của Delta Lake (`RESTORE TABLE rides TO VERSION AS OF <timestamp>`) để khôi phục, sau đó backfill dữ liệu đã mask.
3. **Sự cố 3: Spark Streaming Job bị crash do Out Of Memory (OOM).**
   - **Tác động:** Dữ liệu CDC bị nghẽn ở Kafka, SLA 60s bị vi phạm.
   - **Detection/Rollback:** Cảnh báo từ Prometheus khi Kafka consumer lag vượt ngưỡng. Hệ thống tự động restart job và resume từ checkpoint S3 (Exactly-Once semantics), tránh trùng lặp bản ghi.

## 5. Ước lượng chi phí (FinOps)
- **Quy mô:** 30K peak writes/s $\approx$ 1 tỷ bản ghi/tháng. Dung lượng ~1TB raw/tháng.
- **Storage (S3 Standard):** 
  - Bronze + Silver + Gold $\approx$ 3TB/tháng x 12 tháng = 36TB.
  - S3 Standard ($0.023/GB): **$800/tháng**.
- **Compute (Spark Structured Streaming):**
  - Cluster 3 nodes m5.4xlarge chạy 24/7 (On-demand $0.76/h/node).
  - Compute: **$1,600/tháng** (có thể tối ưu 50% bằng Spot instances).
- **Tổng chi phí dự kiến:** **$2,400/tháng**.

## 6. Lộ trình triển khai MVP (Tuần 1)
- **Mục tiêu:** Xây dựng luồng dữ liệu cho bảng cốt lõi `rides_history`.
- **Phạm vi (Slice):** 
  1. Cấu hình Debezium đẩy CDC của bảng `rides_history` lên Kafka.
  2. Triển khai Spark Streaming job đọc Kafka, tích hợp API Vault (mock) để mask PII, ghi xuống Delta table (Bronze).
  3. Xây dựng Delta MERGE job (micro-batch) từ Bronze CDF cập nhật dữ liệu lên lớp Silver. 
  4. Đánh giá kiểm thử cơ chế late data update.
