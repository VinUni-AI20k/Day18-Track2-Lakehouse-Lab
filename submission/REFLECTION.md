# Lakehouse Anti-Pattern Reflection

**Anti-pattern team dễ vướng nhất:** *Small Files Problem (Thiếu Compaction định kỳ) & Bỏ quên Orphan Files.*

### Lý do và Phân tích:
1. **Streaming/Micro-batch Ingestion:** Khi ingest log/events theo chu kỳ ngắn, mỗi batch sinh nhiều file Parquet chỉ vài KB. Nếu thiếu job `compaction`, số lượng file bùng nổ làm phình to `_delta_log` và tăng I/O latency khi query (chậm hơn 3×–10×).
2. **Ảo tưởng về `VACUUM`:** Nhóm thường nghĩ `VACUUM` dọn sạch mọi file rác. Thực tế `VACUUM` chỉ dọn file có tombstone trong transaction log; các uncommitted orphan files do job crash bị bỏ lại hoàn toàn vô hình với `VACUUM`, làm đội chi phí storage.

### Giải pháp khắc phục:
1. **Tự động hóa Compaction:** Lên lịch định kỳ chạy `OPTIMIZE` / `compact()` gộp small files thành file kích thước tối ưu (128MB–1GB) kết hợp Z-ORDER.
2. **Quét Orphan Files định kỳ:** Thiết lập maintenance job định kỳ so khớp danh sách file vật lý trên storage với manifest log để sweep các orphan files chưa commit.
