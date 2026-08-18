Hệ thống dữ liệu của chúng tôi dễ mắc phải lỗi **Small-Files Problem (Vấn đề tệp nhỏ)** nhất. Trong kiến trúc Data Lakehouse, khi thực hiện Ingestion dữ liệu theo các batch nhỏ liên tục hoặc streaming, hệ thống rất dễ sinh ra hàng nghìn file Parquet dung lượng nhỏ trong Delta Lake. 

Điều này làm giảm hiệu suất truy vấn nghiêm trọng (Overhead) do engine tính toán phải tiêu tốn nhiều tài nguyên và thời gian để đọc I/O metadata cũng như mở từng file riêng lẻ.

**Giải pháp khắc phục:**
1. **Thực thi lệnh OPTIMIZE định kỳ:** Lập lịch (ví dụ: chạy hàng đêm) cho các job chạy lệnh `OPTIMIZE` trên các bảng Delta để gộp (compact) các file nhỏ thành các file lớn có kích thước tối ưu hơn (thường từ 128MB - 1GB).
2. **Bật Auto Compaction & Optimized Writes:** Cấu hình tự động gộp file trong quá trình ghi dữ liệu bằng cách thiết lập `delta.autoOptimize.autoCompact = true` và `delta.autoOptimize.optimizeWrite = true`.
3. **Áp dụng Z-Ordering:** Kết hợp `OPTIMIZE` với `ZORDER BY` trên các cột thường xuyên được dùng làm điều kiện lọc (WHERE clause) để hỗ trợ Data Skipping, giúp tăng tốc độ truy vấn đáng kể.
