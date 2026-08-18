# Reflection

Phân tích Anti-Pattern: Trong hệ thống dữ liệu Lakehouse, đặc biệt là với streaming ingestion (ghi dữ liệu liên tục theo lô nhỏ), team thường dễ mắc phải lỗi **Small-Files Problem** nhất (quên chạy OPTIMIZE). Khi các event hay log được đổ về Lakehouse ở tần suất cao, mỗi vi lô (micro-batch) lại sinh ra các file Parquet rất nhỏ (chỉ vài KB). Dần dần thư mục bảng sẽ chứa hàng chục nghìn file rác nhỏ lẻ. Điều này làm tăng độ trễ truy vấn (I/O overhead) do phải mở quá nhiều file, và làm phình to kích thước của transaction log (Delta Log/Iceberg Metadata), dẫn đến thời gian lập kế hoạch truy vấn (query planning) bị chậm đi nghiêm trọng.

**Giải pháp khắc phục:** 
1. Thiết lập các job bảo trì tự động (Maintenance Jobs) chạy ngầm định kỳ (ví dụ: chạy lệnh `OPTIMIZE` kết hợp `Z-ORDER` mỗi giờ hoặc mỗi ngày) để gom cụm (compact) các file nhỏ thành các file lớn (target_size ~ 256MB - 1GB) và tối ưu hóa index. 
2. Đối với Delta, kết hợp thêm lệnh `VACUUM` để dọn dẹp (Orphan Removal) các file cũ không còn được tham chiếu để giải phóng không gian lưu trữ và tối ưu hiệu suất đọc.
