# Architecture Brief: Click-Stream Lifecycle (FinOps Cap)

## 1. Problem Statement
**Bối cảnh:** Team Consumer App Analytics tạo ra **10 TB raw click-events mỗi ngày**. Luật pháp yêu cầu lưu trữ dữ liệu trong đúng 365 ngày. 
**Ràng buộc:** 
- Ngân sách (Budget) bị CFO giới hạn cứng ở mức **$8,000/tháng** cho toàn bộ storage và compute.
- SLA cho tốc độ truy vấn (Query latency):
  - Dữ liệu 7 ngày gần nhất: p95 < 2s (Hot).
  - Dữ liệu từ 8 - 90 ngày: p95 < 30s (Warm).
  - Dữ liệu từ 91 - 365 ngày: Best effort, < 5 phút (Cold).

**Độ khó:** Giữ được hiệu năng truy vấn siêu tốc (2s) cho dữ liệu mới, trong khi vẫn phải lưu trữ khối lượng dữ liệu khổng lồ (vài PB một năm) mà không làm nổ quỹ $8K.

---

## 2. Architecture Diagram

```text
[Raw Events] -> (Kafka/Kinesis) 
                     |
            (Spark Structured Streaming - Bronze)
                     |
              [Delta Lake - Silver]
                     |
       +-------------+-------------+
       |             |             |
   [HOT TIER]   [WARM TIER]   [COLD TIER]
   S3 Standard    S3 IA       Glacier IR
   1-7 days      8-90 days    91-365 days
   p95 < 2s      p95 < 30s    p95 < 5 min
```
*Ghi chú: Dữ liệu được ingest thẳng vào Delta Lake. S3 Lifecycle Rules sẽ ngầm đẩy các Data files của Delta xuống các tier S3 rẻ hơn theo thời gian.*

---

## 3. Các quyết định kiến trúc cốt lõi & Alternatives đã loại

**Quyết định 1: Định dạng lưu trữ (Table Format)**
- **Chọn Delta Lake (với Z-ORDER).**
- **Loại Parquet thuần:** Thiếu tính năng Data Skipping (min/max stats) và Z-ORDER. Không có Transaction Log sẽ khiến việc quét (scan) thư mục chứa hàng triệu file click-stream tốn vài phút chỉ để list file, vi phạm SLA 2s.

**Quyết định 2: Chiến lược phân mảnh (Partitioning Strategy)**
- **Chọn:** Partition theo `date` (ngày) + Z-ORDER theo `user_id` và `session_id`.
- **Loại Partition theo `date` + `hour`:** Sẽ tạo ra quá nhiều thư mục nhỏ (over-partitioning). Việc Z-order bên trong partition ngày là đủ để tối ưu point-query cho specific user/session.

**Quyết định 3: Storage Tiering (Quản lý vòng đời lưu trữ)**
- **Chọn:** Tận dụng tính năng Transparent Lifecycle của AWS S3 để ngầm chuyển các file Parquet tĩnh của Delta sang tier rẻ hơn. Cụ thể: S3 Standard (ngày 1-7) -> S3 Standard-IA (ngày 8-90) -> S3 Glacier Instant Retrieval (ngày 91-365).
- **Loại việc tự viết script copy data:** Việc tự copy/move dữ liệu sang một S3 bucket khác để lưu trữ lạnh sẽ làm gãy (break) Transaction Log của Delta Lake, khiến người dùng không thể truy vấn chung một bảng mượt mà.

**Quyết định 4: Chiến lược Compaction (Gom file nhỏ)**
- **Chọn:** Chạy job `OPTIMIZE` mỗi 1 giờ cho các partition của ngày hôm nay và hôm qua.
- **Loại việc chạy OPTIMIZE trên toàn bộ bảng:** Việc quét lại các partition cũ (đã nằm ở Glacier) không những không cần thiết mà còn kích hoạt phí truy xuất (retrieval fee) của S3, gây nổ ngân sách.

**Quyết định 5: Xử lý dữ liệu hết hạn (Data Expiry)**
- **Chọn:** Cấu hình Delta Lake `VACUUM` retention và S3 Expiration Rule để tự động hard-delete các file quá 365 ngày.
- **Loại việc chạy câu lệnh DELETE FROM:** Câu lệnh DELETE sẽ sinh ra Deletion Vectors và tốn compute khổng lồ để quét toàn bộ lịch sử. S3 Expiration Rule làm việc này ở tầng storage với chi phí $0.

---

## 4. Failure Modes (Kịch bản sự cố lúc 3h sáng)

1. **Sự cố:** Job OPTIMIZE vô tình quét trúng partition 6 tháng trước, kéo dữ liệu từ Glacier lên, dự kiến bill S3 đội lên $2,000 trong đêm.
   - **Detection:** Billing alert tự động trigger khi chi phí S3 API Calls tăng đột biến.
   - **Rollback:** Lập tức kill Spark Job. Cập nhật lại logic của hàm chạy OPTIMIZE (chỉ filter `date >= current_date - 2`).

2. **Sự cố:** Kafka bị nghẽn, mất mạng diện rộng, dữ liệu click-stream ngày hôm qua đổ dồn về vào hôm nay (Late Data).
   - **Detection:** Dashboard Data Freshness báo đỏ (độ trễ > 1 tiếng).
   - **Rollback:** Chạy lại `OPTIMIZE` bổ sung đặc biệt cho partition của ngày hôm qua (chỉ định explicitly qua filter) để gom lại hàng triệu file nhỏ vừa bị late data xả xuống.

3. **Sự cố:** Lệnh VACUUM lỡ tay dọn sạch các file data đang được một báo cáo BI dài hạn đọc dở.
   - **Detection:** BI tools báo lỗi `FileNotFoundException` khi query bảng Delta.
   - **Rollback:** Tăng giá trị `deletedFileRetentionDuration` của VACUUM lên 7 ngày thay vì mặc định 7 ngày, đảm bảo có buffer an toàn cho các query chạy lâu.

---

## 5. Ước lượng chi phí (Cost Estimation)

*Giả định:* 10 TB raw data/ngày. Khi lưu xuống Delta Lake bằng Zstd compression, tỷ lệ nén là 1:5 -> Sinh ra **2 TB data/ngày**.

**Tính toán Storage Size ở trạng thái ổn định (đã đủ 365 ngày):**
- **Hot Tier (7 ngày):** 7 ngày × 2 TB = 14 TB (S3 Standard).
- **Warm Tier (83 ngày):** 83 ngày × 2 TB = 166 TB (S3 Standard-IA).
- **Cold Tier (275 ngày):** 275 ngày × 2 TB = 550 TB (S3 Glacier Instant Retrieval).

**Chi phí S3 Storage/Tháng (AWS us-east-1):**
- S3 Standard (~$23/TB): 14 TB × $23 = **$322**
- S3 IA (~$12.5/TB): 166 TB × $12.5 = **$2,075**
- S3 Glacier IR (~$4/TB): 550 TB × $4 = **$2,200**
- **Tổng Storage:** **~$4,597 / tháng**

**Chi phí Compute:**
- Dành khoảng **$3,000 / tháng** cho Databricks/Spark Compute (dành cho streaming ingestion và maintenance jobs).
- **Tổng TCO:** **~$7,597 / tháng** (Đạt mục tiêu an toàn dưới mức hard cap $8,000 của CFO).

---

## 6. MVP Slice (Cái gì làm trước)

Thay vì dựng toàn bộ kiến trúc trên, trong tuần đầu tiên, team sẽ xây dựng **Minimum Viable Product (MVP)**:
- Tạo một Delta Table nhỏ.
- Viết 1 script Python giả lập ghi 10GB raw events vào bảng, chạy liên tục trong 1 ngày.
- Cấu hình thử S3 Lifecycle Rule chuyển file sang S3 IA sau 1 ngày (dùng cờ test).
- Chứng minh rằng: Việc S3 ngầm đổi storage class của file không làm hỏng tính toàn vẹn của Transaction Log trong Delta Lake. Truy vấn bằng DuckDB vẫn đọc được trơn tru dữ liệu nằm ở IA tier.
