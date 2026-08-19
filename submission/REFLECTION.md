# Reflection: Top 5 Lakehouse Anti-Patterns

Trong 5 anti-patterns của Data Lakehouse, hệ thống dữ liệu của team chúng tôi dễ mắc phải nhất là **"Streaming Ingestion Without Scheduled Compaction (Small-Files Problem) và Bỏ Quên Dọn File Mồ Côi (Orphans)"**.

### 1. Nguyên nhân & Rủi ro:
- **Nghiệp vụ:** Luồng streaming/CDC đẩy liên tục micro-batches (5–10s) vào tầng Bronze. Thiếu cron job `OPTIMIZE` định kỳ khiến số lượng file Parquet nhỏ (vài chục KB) tăng lên hàng trăm nghìn file.
- **Hậu quả:** Chi phí lệnh `GET` trên S3 bùng nổ và làm nghẽn I/O khi query engine phải scan quá nhiều file.
- **Bẫy bảo trì ngầm:** Khi writer crash giữa chừng, các file rác chưa commit vào Transaction Log sẽ bị `VACUUM` mặc định bỏ sót.

### 2. Giải pháp khắc phục:
- **Compaction & Z-ORDER:** Chạy tự động `compact()` gộp file về 128–512 MB và `z_order()` theo cột hay lọc (`user_id`).
- **Orphan Sweeper:** Định kỳ chạy thuật toán hiệu tập hợp ($Files_{Disk} \setminus Files_{Log}$) kèm age guard để xóa sạch file mồ côi.
