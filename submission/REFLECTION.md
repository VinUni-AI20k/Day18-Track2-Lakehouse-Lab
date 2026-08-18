# Lakehouse Anti-Pattern Reflection

Trong 5 Lakehouse Anti-Patterns, hệ thống dữ liệu của team chúng tôi dễ mắc phải nhất là **Small-Files Problem do Streaming Ingestion** (bỏ qua chiến lược bảo trì định kỳ).

### Nguyên nhân & Rủi ro
Khi nạp dữ liệu telemetry/LLM events liên tục qua micro-batches theo thời gian thực, hàng chục nghìn file Parquet cỡ vài KB được sinh ra mỗi ngày. Điều này làm bùng nổ metadata log (`_delta_log`/Iceberg manifests), tăng số lượng IOPS/S3 GET requests, và triệt tiêu khả năng data-skipping, khiến latency các truy vấn phân tích downstream suy giảm nghiêm trọng.

### Giải pháp khắc phục
1. **Auto-Compaction & Bin-packing**: Kích hoạt auto-compaction khi ghi hoặc gom file định kỳ với `dt.optimize.compact(target_size=128*1024*1024)` nhằm duy trì kích thước file mục tiêu 128MB–256MB.
2. **Multi-dimensional Clustering**: Định kỳ chạy `z_order(["tenant_id", "timestamp"])` trên các cột thường xuyên lọc để tối ưu hóa min/max stats skipping.
3. **Automated Lifecycle Maintenance**: Lập lịch orchestrator chạy `VACUUM` dọn snapshot cũ và uncommitted orphan files, đồng thời tạo Delta checkpoint định kỳ để nén log state.
