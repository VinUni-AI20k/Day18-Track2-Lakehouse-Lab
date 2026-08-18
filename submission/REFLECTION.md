# Reflection: Top Lakehouse Anti-Patterns

**Anti-Pattern team dễ vướng nhất:** *Small-File Pathology do Ingestion liên tục thiếu bảo trì định kỳ.*

### 1. Vì sao dễ vướng?
Khi xây dựng pipeline thu thập log LLM/microservices theo micro-batch (chu kỳ vài giây), hệ thống tạo ra hàng ngàn file Parquet siêu nhỏ (~50–100 KB). Do hệ thống ban đầu vẫn hoạt động bình thường, team thường bỏ qua khâu maintenance:
- **Bùng nổ chi phí request:** Phí `S3 GET` và I/O tăng phi tuyến tính theo số lượng file, chiếm phần lớn hóa đơn lưu trữ.
- **Nghẽn Query Planning:** Engine phải quét hàng triệu file nhỏ, vô hiệu hóa khả năng data skipping của min/max stats.

### 2. Giải pháp khắc phục (áp dụng từ Lab 18):
- **Tự động hóa Compaction & Clustering:** Lập cron job chạy `OPTIMIZE` gộp file về kích thước chuẩn (128–512 MB) và áp dụng `Z-ORDER` theo access path (`user_id`, `model`) để đạt skip rate ≥ 50%.
- **Chaining Snapshot Expiry & Orphan Cleanup:** Thiết lập `VACUUM` kèm age guard (≥ 24h) kết hợp quét set difference trên đĩa để dọn sạch rác phát sinh từ crashed writers mà log không tham chiếu.
