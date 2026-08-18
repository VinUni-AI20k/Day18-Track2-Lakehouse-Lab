# Lakehouse Reflection: Phân tích Anti-Pattern Hạ tầng Dữ liệu Thực tế

Hệ thống của chúng tôi dễ mắc phải nhất là **Small-Files Problem do Streaming Ingestion & Thiếu Quét Orphan Files (Anti-Pattern #1 & #3)**.

**1. Rủi ro thực tế:**
Ingestion liên tục từ Kafka với trigger ngắn sinh ra hàng nghìn file Parquet nhỏ (như đo ở NB2/NB6: 200 micro-batches $\rightarrow$ 200 files). Lỗi này làm bùng nổ metadata, suy giảm tốc độ truy vấn phi tuyến tính và đội chi phí S3. Đáng chú ý, Delta `VACUUM` bỏ qua file orphan uncommitted do job crash để lại; còn Iceberg `expire_snapshots` chỉ sửa metadata mà không xóa file manifest trên đĩa.

**2. Giải pháp khắc phục:**
* **Bảo trì định kỳ:** Lên lịch cron job chạy `OPTIMIZE` gom file về 128–512 MB kết hợp `Z-ORDER` clustering theo cột truy vấn thường xuyên.
* **Ghép đôi Dọn rác:** Luôn kết hợp `expire_snapshots` với quét tập hợp orphan (`storage_files - active_manifests`) để giải phóng dung lượng thật.
* **Đồng bộ Vòng đời:** Dùng Delta Change Data Feed (CDF) để đồng bộ xóa dữ liệu người dùng (Nghị định 13) sang các vector index ngoài.
