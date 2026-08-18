# Bản Quyết Định Kiến Trúc (ADR): LLM Observability Lakehouse ở Quy Mô 1 Tỷ Requests/Ngày

**Trạng thái:** Đã phê duyệt (Approved)  
**Vai trò:** Data Architect On-Call  
**Phạm vi:** Nền tảng LLM Observability (5 TB/ngày raw, 1 tỷ request/ngày, Ngân sách lưu trữ $\le \$5.000$/tháng)

---

## 1. Bối cảnh & Yêu cầu bài toán

Hệ thống LLM Gateway của tổ chức xử lý **1 tỷ lượt gọi inference mỗi ngày** (~5 KB JSON/lượt $\rightarrow$ **5 TB/ngày dữ liệu thô**).
Các yêu cầu kỹ thuật và nghiệp vụ bắt buộc bao gồm:
1. **SLA Dashboard cho Tenant:** Theo dõi thời gian thực lượng token tiêu thụ, chi phí và độ trễ (p50/p95/p99) theo từng khách hàng (tenant), cập nhật mỗi 5 phút.
2. **Vòng đời dữ liệu & Tuân thủ:** Giữ đầy đủ prompt/completion thô trong đúng 7 ngày phục vụ truy vết sự cố (incident review); lưu trữ dữ liệu tổng hợp theo giờ/ngày trong 1 năm.
3. **Bảo mật & Quyền riêng tư:** Tự động ẩn danh / gắn token hóa dữ liệu định danh cá nhân (PII redaction) ngay tại biên trước khi lưu trữ.
4. **Trần ngân sách FinOps:** Chi phí lưu trữ không vượt quá **$\$5.000/\text{tháng}$**.

---

## 2. Thiết kế Kiến trúc Medallion Pipeline

```
+-------------------------------------------------------------------------------+
| Ingestion: Kafka / Kinesis Stream (5 TB/ngày, peak 12.000 req/s)              |
+---------------------------------------+---------------------------------------+
                                        |
                   [Stream Processing & Bộ lọc PII tại biên]
                                        |
+---------------------------------------v---------------------------------------+
| TẦNG BRONZE (Landing / Raw Log Bất biến)                                      |
| Định dạng: Delta Lake / Iceberg phân vùng theo (date, hour)                   |
| Lưu trữ: Cloud Object Storage (Standard Tier, Hard TTL 7 ngày)                |
| Trường: request_id, tenant_id, model, token_counts, latency_ms, blobs ẩn danh |
+---------------------------------------+---------------------------------------+
                                        |
                     [Micro-batch Structured Streaming 5 phút/lần]
                                        |
+---------------------------------------v---------------------------------------+
| TẦNG SILVER (Sạch, Đã Deduplicate, Có Cấu trúc Chuẩn)                         |
| Định dạng: Delta Lake phân vùng theo (date, tenant_id_prefix)                  |
| Tối ưu: Liquid Clustering / Z-Order theo (tenant_id, model)                   |
| Thời gian lưu giữ: 7 ngày                                                     |
+---------------------------------------+---------------------------------------+
                                        |
                     [Tác vụ Tổng hợp Định kỳ (Rollups)]
                                        |
+---------------------------------------v---------------------------------------+
| TẦNG GOLD (Data Mart Tổng hợp Chỉ số Observability)                           |
| Độ mịn: Tổng hợp 1 phút & 1 giờ theo (tenant_id, model, status_code)          |
| Chỉ số: p50/p95/p99 latency, prompt/completion tokens, cost_usd, error_rate   |
| Thời gian lưu giữ: 365 ngày (Chuyển sang Infrequent Access sau 30 ngày)       |
+-------------------------------------------------------------------------------+
```

---

## 3. Chiến lược Lưu trữ & Mô hình Chi phí FinOps

### 3.1. Dung lượng thực tế & Nén dữ liệu
* Dữ liệu JSON thô chưa nén: $5\text{ TB/ngày} \times 7\text{ ngày} = 35\text{ TB}$.
* Định dạng cột Parquet nén ZSTD Level 3 giảm dung lượng thô $\sim 4.5\times \rightarrow \mathbf{7.8\text{ TB}}$ lưu trữ thực tế tại Bronze/Silver.
* Dữ liệu tổng hợp tầng Gold: $10.000\text{ tenants} \times 5\text{ models} \times 24\text{ giờ} \times 365\text{ ngày} \approx \mathbf{1.2\text{ TB/năm}}$.

### 3.2. Bảng phân bổ chi phí lưu trữ hàng tháng (Đơn giá chuẩn AWS / GCP)
| Tầng / Hạng mục | Dung lượng hoạt động | Đơn giá | Chi phí hàng tháng |
| :--- | :--- | :--- | :--- |
| **Bronze + Silver (Cửa sổ 7 ngày)** | 7.8 TB (Standard S3/GCS) | \$0.023 / GB | **\$179.40** |
| **Gold (Ngày 1–30)** | 100 GB (Standard S3/GCS) | \$0.023 / GB | **\$2.30** |
| **Gold (Ngày 31–365)** | 1.1 TB (Infrequent Access) | \$0.0125 / GB | **\$13.75** |
| **Chi phí API & Tác vụ Compaction/Expiry** | Tác vụ PUT/GET & chuyển tier | Biểu giá API chuẩn | **\$45.00** |
| **Tổng chi phí lưu trữ hàng tháng** | | | **\$240.45 / tháng** |

> **Biên an toàn FinOps:** Chi phí lưu trữ thực tế chỉ hết **\$240.45/tháng**, chiếm chưa tới **$5\%$** mức trần ngân sách cho phép (\$5.000/tháng), dành hơn 95% ngân sách cho năng lực tính toán streaming (Compute).

---

## 4. Đánh giá & Quyết định Lựa chọn Giải pháp

### 4.1. Định dạng bảng: Delta Lake vs. Apache Iceberg vs. JSON thô trên S3
* **Loại bỏ: JSON thô trên S3 + Athena/Hive:** Không có giao dịch ACID, không thể prune file theo tenant, chi phí scan toàn bộ S3 rất đắt (\$5/TB scan $\approx \$25$ cho một câu truy vấn dashboard đơn lẻ).
* **Lựa chọn: Delta Lake với Z-Order / Liquid Clustering:** 
  * Tối ưu hóa truy vấn lọc theo từng khách hàng (`WHERE tenant_id = '...'`) nhờ bỏ qua $\ge 90\%$ số file thông qua min/max statistics trong transaction log.
  * Hỗ trợ sẵn Change Data Feed (CDF) để bắn cảnh báo bất thường sang hệ thống downstream.

### 4.2. Lưu trữ Đa phương tiện & Vector Embeddings
* Lưu trữ vector embedding **inline trực tiếp trong bảng Parquet** với định dạng nén lượng tử `int8` thay vì duy trì một cụm Vector DB ngoài tốn kém cho 1 tỷ req/ngày.
* Tính năng Projection Pushdown của Parquet đảm bảo các dashboard đọc `tenant_id, latency, cost` sẽ tự động bỏ qua cột vector/blob, hoàn toàn không tốn I/O.

### 4.3. Lịch trình Bảo trì Bảng Tự động
1. **Compaction:** Job định kỳ hàng giờ gom các file micro-batch về kích thước chuẩn 256 MB.
2. **Clustering:** Chạy Z-Order hàng đêm trên các phân vùng active theo cặp `(tenant_id, model)`.
3. **Snapshot Expiry & Vacuum:** Thực thi `VACUUM` hàng ngày với thời gian lưu giữ đúng 168 giờ (7 ngày).
4. **Orphan File Sweeps:** Quét tập hợp chênh lệch (Set-difference) để dọn các file rác mồ côi do job ghi bị lỗi.
