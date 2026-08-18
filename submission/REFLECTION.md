# Lakehouse Anti-Pattern Reflection

**Anti-Pattern Dễ Mắc Phải:**
Trong hệ thống AI Observability và lưu trữ Agent Traces của nhóm, lỗi dễ mắc phải nhất là **Small-Files Problem do Ingestion liên tục** (streaming/micro-batching) mà bỏ quên bước bảo trì định kỳ.

**Rủi Ro Kỹ Thuật:**
* Ghi nhận lượt gọi LLM và telemetry thời gian thực tạo ra hàng ngàn file Parquet siêu nhỏ (vài KB).
* Gây quá tải driver/catalog khi liệt kê metadata, làm suy giảm hiệu năng truy vấn phân tích và tăng chi phí I/O đọc file.

**Giải Pháp Khắc Phục:**
* **Compaction định kỳ:** Lập lịch tự động chạy `OPTIMIZE ... COMPACT` (kích thước mục tiêu 128MB–256MB) để gom các file nhỏ.
* **Đa chiều hoá chỉ mục (Z-ORDER):** Áp dụng `Z-ORDER BY (model, timestamp)` nhằm nâng cao hiệu quả Data Skipping và Partition Pruning.
* **Bảo trì Snapshot:** Thực thi `VACUUM` và `expire_snapshots()` định kỳ để dọn dẹp các tệp mồ côi và thu gọn transaction log.