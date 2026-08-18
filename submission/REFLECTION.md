# Lab18 Reflection

## Anti-Pattern: Small-Files Problem từ Multi-Agent Streaming

Trong 5 Anti-Patterns của Lakehouse, hệ thống **ContentForge AI** của team 4 người dễ mắc nhất là **Small-Files Problem** — do kiến trúc multi-agent streaming ingestion.

### Lý do
Hệ thống có 7 agents chạy song song (Trend, Planner, Writer, Media, Compliance, Publishing, RAG), mỗi agent ghi dữ liệu vào PostgreSQL theo thời gian thực qua 12-state pipeline. Observer Layer thu thập metrics mỗi 5 phút. Khi chuyển sang Lakehouse (Delta/Iceberg), nếu giữ nguyên pattern "agent ghi ngay khi xong" → hàng trăm micro-commits/giờ → hàng nghìn small Parquet files.

### Giải pháp
1. **Batch writes**: Thu thập events trong buffer 5-10 phút, ghi một lần thay vì mỗi agent ghi riêng.
2. **OPTIMIZE schedule**: Chạy `OPTIMIZE` mỗi đêm hoặc sau mỗi batch để gom files.
3. **Z-ORDER**: Cluster theo `job_id`, `project_id` để prune hiệu quả khi query theo job.
4. **Retention policy**: Archive data > 30 ngày, xóa orphans sau mỗi OPTIMIZE.

Cách tiếp cận này giữ được real-time observability mà khônghy sinh query performance.
