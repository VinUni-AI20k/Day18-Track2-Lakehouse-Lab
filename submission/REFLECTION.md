# Reflection: Top Lakehouse Anti-Patterns

Trong 5 anti-patterns của Lakehouse, rủi ro lớn nhất mà hệ thống của team chúng tôi dễ vướng phải nhất là **The Small-File Problem do thiếu vắng Table Maintenance định kỳ (Compaction & Vacuum)**.

### Lý do và Phân tích:
1. **Streaming Ingestion & Micro-batching**: Team thu thập log sự kiện, LLM prompts/responses và telemetry liên tục với chu kỳ vài giây. Mỗi batch nhỏ tạo ra một commit và một file Parquet riêng biệt, nhanh chóng tích tụ hàng trăm nghìn small files chỉ sau vài ngày.
2. **Hậu quả chi phí & hiệu năng**: Số lượng file quá lớn làm bùng nổ chi phí S3 `GET` request (tính theo số lượt gọi, không chỉ dung lượng byte) và làm suy giảm nghiêm trọng tốc độ query do overhead quét file metadata thay vì đọc dữ liệu nén liên tục.
3. **Giải pháp khắc phục**: Thiết lập lịch tự động (cron/orchestrator) cho **4 Jobs bắt buộc**:
   - **Job 1 (Compaction)**: `OPTIMIZE` gộp các small files về target 128–512 MB.
   - **Job 2 (Clustering)**: `Z-ORDER` theo key truy vấn thường xuyên (`tenant_id`, `model`) để file-skipping tối ưu.
   - **Job 3 & 4 (Expiry & Orphan Cleanup)**: `VACUUM` và quét orphan files để giải phóng dung lượng rác và tombstone metadata.
