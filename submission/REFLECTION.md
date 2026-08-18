# Reflection: Lakehouse Anti-Patterns in Practice

Trong **5 Anti-Patterns của Lakehouse**, hệ thống của team tôi dễ mắc phải nhất là **Bỏ qua OPTIMIZE dẫn đến Small-Files Problem do Streaming Ingestion**, đi kèm lầm tưởng về **`VACUUM`**.

### 1. Nguyên nhân & Rủi ro:
* **Small-Files Problem**: Các pipeline streaming telemetry/LLM append liên tục micro-batch nhỏ, sinh ra hàng trăm nghìn file Parquet rải rác. Điều này làm phình metadata, làm chậm query scan planning và tăng chi phí S3 GET/LIST API.
* **Lỗ hổng Orphan Files**: Khi worker crash giữa chừng, các file uncommitted bị bỏ rơi trên storage. Lầm tưởng rằng `VACUUM` sẽ dọn sạch chúng khiến dung lượng lưu trữ tăng âm thầm, vì `VACUUM` chỉ xóa file có tombstone trong log.

### 2. Giải pháp khắc phục:
1. **Compaction & Z-Order tự động**: Lập lịch chạy Job 1 & 2 gộp các file nhỏ thành file chuẩn ~128–256 MB và đánh chỉ mục Z-Order theo `tenant_id`/`user_id` để tối ưu file-pruning.
2. **Differential Orphan Sweep**: Chạy bảo trì Job 4 định kỳ (`filesystem_files - active_log_files`) để dọn sạch file uncommitted, tối ưu chi phí FinOps.
