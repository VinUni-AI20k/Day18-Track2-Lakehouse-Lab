# Reflection: Top Lakehouse Anti-Pattern

**Anti-Pattern có nguy cơ cao nhất:** *Bỏ qua các tác vụ bảo trì định kỳ (Compaction, Expiry & Orphan Cleanup).*

### Lý do:
Trong các hệ thống xử lý streaming và ingestion liên tục (như log LLM, CDC), dữ liệu thường được ghi dưới dạng các file nhỏ (micro-batches). Nếu không thiết lập pipeline tự động chạy **Compaction** và **Z-ORDER**, hệ thống sẽ nhanh chóng rơi vào tình trạng hàng trăm nghìn small files, làm suy giảm nghiêm trọng hiệu năng đọc (query latency tăng vọt do metadata bottleneck).

Đặc biệt, qua bài lab (NB6), chúng tôi nhận thấy `VACUUM` thông thường chỉ xóa các file đã được ghi nhận trong transaction log mà bỏ sót các file rác do job crash để lại (**uncommitted orphans**), còn `expire_snapshots` của Iceberg nếu không đi kèm orphan cleanup sẽ không giải phóng dung lượng đĩa thực tế. Việc thiếu chiến lược bảo trì đồng bộ sẽ gây lãng phí chi phí lưu trữ đám mây và làm chậm tốc độ truy vấn phân tích.
