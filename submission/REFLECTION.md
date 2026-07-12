# Reflection - Day 18 Lakehouse Lab

**Anti-pattern mà team dễ vướng phải nhất:** "The Small File Problem" (Vấn đề tập tin nhỏ).

**Lý do:** 
Trong dự án LLM Observability (NB4), dữ liệu được đẩy về liên tục theo từng cuộc gọi API (streaming hoặc high-frequency batch). Nếu không có cơ chế quản lý, hệ thống sẽ tạo ra hàng triệu file Parquet nhỏ trên S3/Data Lake. Điều này làm tăng chi phí lưu trữ và khiến tốc độ truy vấn Dashboard trở nên cực kỳ chậm do overhead khi mở/đóng file.

**Giải pháp từ Lab:**
Thông qua bài Lab, tôi nhận thấy tính năng `OPTIMIZE` và `Z-ORDER` của Delta Lake là "cứu cánh" cho vấn đề này. Nó cho phép team duy trì tốc độ ghi nhanh (ghi file nhỏ) nhưng vẫn đảm bảo hiệu năng đọc xuất sắc bằng cách định kỳ gom file (compaction) và sắp xếp lại dữ liệu để tối ưu hóa việc bỏ qua dữ liệu không cần thiết (Data Skipping).
