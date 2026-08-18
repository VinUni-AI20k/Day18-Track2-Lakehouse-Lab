# Reflection: Lakehouse Anti-Patterns

Trong các Lakehouse Anti-Patterns, đội ngũ của chúng tôi dễ vướng phải lỗi **"The Small-File Problem & Bỏ qua Table Maintenance"** nhất.

### Lý do:
1. **Đặc thù dữ liệu Streaming**: Hệ thống liên tục nạp dữ liệu LLM calls và telemetry theo cơ chế micro-batch (5–10 giây/đợt). Nếu không cài đặt các scheduled cron jobs cho `COMPACTION` (`OPTIMIZE`), hàng ngàn file Parquet nhỏ (vài KB) sẽ tích tụ nhanh chóng qua đêm.
2. **Tác động chi phí & Hiệu năng**: Số lượng file phình to làm tăng số lượng API request (S3 `GET`) theo cấp số nhân, khiến thời gian truy vấn bị chậm non-linear và làm tăng hóa đơn dịch vụ đám mây.
3. **Bỏ qua Orphan Removal**: Khi các pipeline ghi dữ liệu bị ngắt giữa chừng (crash), các file mồ côi (orphans) không được commit vào transaction log vẫn nằm trên đĩa. Nếu chỉ chạy `VACUUM` hoặc `expire_snapshots` thông thường mà không quét orphan, dung lượng lưu trữ đĩa sẽ không bao giờ giảm.

### Giải pháp khắc phục:
Thiết lập tự động hóa 4 job bảo trì định kỳ (Compaction, Z-Order, Expiry, Orphan Sweep) trong hệ thống Orchestration (Airflow/Prefect) để đảm bảo kích thước file luôn duy trì ở mức chuẩn 128–512 MB.
