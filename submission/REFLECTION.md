# Reflection: Top Lakehouse Anti-Pattern Risk

**Anti-Pattern nguy cơ cao nhất:** *Small-Files Problem do Streaming Ingestion & Micro-batching.*

### Lý do & Phân tích rủi ro:
Trong kiến trúc LLM Observability và Event Tracking thực tế, các dịch vụ AI sinh ra hàng nghìn log requests mỗi phút. Nếu ingest trực tiếp vào Data Lakehouse theo từng micro-batch nhỏ mà không có chiến lược compaction định kỳ:
1. **Bùng nổ Metadata & Suy giảm hiệu năng:** Mỗi micro-batch tạo ra một file Parquet nhỏ (~vài KB) kèm một commit JSON trong `_delta_log`. Khi số lượng file lên tới hàng trăm nghìn, chi phí metadata scan của query engine (DuckDB/Spark) tăng vọt, gây nghẽn I/O và suy giảm nghiêm trọng tốc độ point query.
2. **Giải pháp khắc phục:** 
   - Triển khai **Job 1 (Compaction)** định kỳ với `dt.optimize.compact()` để gom các file nhỏ về kích thước tối ưu (128MB–256MB).
   - Áp dụng **Z-ORDER** trên các trường lọc thường xuyên (`user_id`, `model`) để tận dụng cơ chế file-level min/max statistics skipping, loại bỏ tới 90%+ số files không liên quan khi truy vấn.
