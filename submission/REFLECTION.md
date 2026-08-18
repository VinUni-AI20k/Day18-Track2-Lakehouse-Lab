# Reflection: Lakehouse Anti-Patterns in Practice

Trong "Top 5 Lakehouse Anti-Patterns", team tôi dễ vướng phải **Small-File Problem do Streaming Ingestion kết hợp Ảo tưởng về VACUUM** nhất:

1. **Tích tụ file vụn**: Các luồng nạp (Kafka/Flink) ghi liên tục mỗi vài giây tạo ra hàng trăm nghìn file Parquet nhỏ (đã đo ở NB2: 200 batch sinh 200 file). Điều này làm tăng vọt chi phí GET request trên Object Storage và làm nghẽn scan planning.
2. **Cạm bẫy VACUUM không dọn orphan**: Thực nghiệm ở NB6 chứng minh `VACUUM` chỉ xóa file đã được tombstone trong transaction log. Các file rác do writer crash để lại chưa từng commit sẽ hoàn toàn vô hình trước `VACUUM`, gây lãng phí dung lượng lưu trữ dài hạn.

**Giải pháp**:
- Thiết lập cron job định kỳ chạy `OPTIMIZE` (Compaction) kết hợp `Z-ORDER` trên cột truy vấn chính.
- Triển khai thuật toán hiệu tập hợp: `Files đĩa − Files Catalog` (kèm age-guard $\ge 24$h) để quét sạch orphan files.
