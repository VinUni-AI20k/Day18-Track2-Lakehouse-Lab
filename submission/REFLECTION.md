# Bài thu hoạch: Anti-Pattern Rủi Ro Nhất trong Lakehouse

**Anti-Pattern: Tích tụ file nhỏ (Small Files) do Ingestion Streaming**

Team chúng tôi chạy pipeline streaming (Kafka $\rightarrow$ Delta Lake batch 5s). Dù các commit đều thành công, việc tích tụ tạo ra hàng chục nghìn file Parquet sub-MB hàng tuần.

Từ kết quả đo đạc ở NB2 và NB6:
1. **Suy giảm truy vấn:** Engine phải đọc hàng nghìn file footer và manifest thay vì đọc dữ liệu cột, vô hiệu hóa tính năng file-pruning qua min/max stats.
2. **Chi phí FinOps:** Dịch vụ auto-compaction tính phí theo số lượng object. Số lượng file bùng nổ đẩy chi phí dọn dẹp tăng phi tuyến tính.
3. **Điểm mù của Vacuum:** Job crash để lại orphan files ngoài commit log, khiến `VACUUM` mặc định bỏ sót hoàn toàn.

**Giải pháp:** Lên lịch Compaction định kỳ (`OPTIMIZE` gom file về 128–512 MB), gom cụm theo Z-Order clustering, và thực hiện quy trình bảo trì kép: Snapshot Expiry đi liền với Orphan Sweep.
