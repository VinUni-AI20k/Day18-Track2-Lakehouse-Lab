# Lakehouse Anti-Pattern Reflection

**Anti-Pattern dễ mắc phải nhất:** *Small-Files Problem do Streaming Ingestion thiếu Compaction*.

**Thực trạng & Rủi ro:**
Hệ thống dữ liệu của team liên tục ghi nhận streaming theo thời gian thực (LLM observability logs, CDC events) với tần suất micro-batch vài giây. Việc ghi liên tục tạo ra hàng nghìn file Parquet siêu nhỏ (vài KB), dẫn đến bùng nổ I/O metadata, giảm hiệu năng truy vấn nghiêm trọng (read amplification) và tăng đột biến chi phí scan/list trên object storage.

**Giải pháp khắc phục:**
1. **Compaction tự động (Bin-packing):** Lập lịch định kỳ chạy job `OPTIMIZE` (hoặc bật auto-compaction) nhằm gộp các file nhỏ thành kích thước tối ưu (128MB–512MB).
2. **Clustering với Z-ORDER:** Áp dụng `Z-ORDER BY (timestamp, model_id)` tại tầng Silver/Gold để thu hẹp min/max statistics, tối ưu data skipping khi truy vấn.
3. **Bảo trì vòng đời (Maintenance):** Thiết lập pipeline định kỳ chạy cặp đôi `VACUUM` (thu hồi dữ liệu sau retention period) và quét dọn **Orphan files** (dọn các file rác sinh ra do uncommitted/failed writes) để giảm tải metadata và tối ưu chi phí lưu trữ.
