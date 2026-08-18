# Reflection: Lakehouse Anti-Patterns

**Anti-pattern nguy cơ cao nhất:** *Small Files do Streaming Ingestion và thiếu Table Maintenance định kỳ.*

### Lý do kỹ thuật & Rủi ro:
1. **Chi phí S3 & Tắc nghẽn Scan:** Khi ghi streaming LLM logs hoặc Agent traces liên tục, hệ thống sinh ra hàng nghìn file Parquet nhỏ (<1 MB). Chi phí GET requests tăng phi tuyến tính và metadata phình to làm chậm quá trình scan planning.
2. **Metadata Bloat:** Không chạy log checkpoint (Job 5) khiến reader phải nạp hàng trăm file JSON để xác định trạng thái bảng.
3. **Orphan Files vô hình:** Khi tác vụ ghi gặp sự cố (crashed writer), file rác tồn tại trên đĩa mà không có commit trong log. VACUUM mặc định bỏ qua các file chưa tombstone này, gây thất thoát chi phí lưu trữ âm thầm.

### Giải pháp phòng ngừa:
Thiết lập cron job tự động chạy 4 jobs bảo trì: OPTIMIZE (compaction gom file 128–512 MB) kèm Z-ORDER, VACUUM/expire_snapshots định kỳ, và quét hiệu tập hợp (set-difference) để dọn sạch orphan files.