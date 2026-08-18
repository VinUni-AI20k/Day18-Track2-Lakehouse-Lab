# Phân Tích Anti-Pattern Hạ Tầng Lakehouse

Team chúng tôi dễ vướng phải **Căn bệnh File nhỏ (Small-File Problem) do Streaming thiếu Table Maintenance**.

### 1. Nguyên nhân Thực tế
* Pipeline streaming (Kafka $\rightarrow$ Lakehouse) ghi micro-batch liên tục mỗi 5–10s.
* Tích lũy hàng triệu file Parquet kích thước nhỏ (vài chục KB) mà không có cron job dọn dẹp.

### 2. Rủi ro Hạ tầng
* **Chi phí request tăng vọt:** S3/GCS tính tiền theo request (`GET`/`LIST`). Chi phí gọi API chiếm tới $25\%$ hóa đơn, vượt xa chi phí lưu trữ byte thuần túy.
* **Suy giảm hiệu năng:** Query planner nghẽn do phải đọc metadata footer của hàng vạn file nhỏ, vô hiệu hóa cơ chế cache.

### 3. Giải pháp Xử lý
* **Compaction tự động:** Lên lịch định kỳ gộp file về kích thước chuẩn 128–512 MB.
* **Clustering (Z-order):** Sắp xếp đa chiều để tăng tỉ lệ file-skipping $\ge 50\%$.
* **Expiry + Orphan Sweep:** Chạy Expiry song song quét file rác để thực sự giải phóng dung lượng đĩa.
