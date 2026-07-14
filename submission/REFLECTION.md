# Reflection on Slide §5 Anti-Patterns

Dựa trên nội dung của Slide §5 về các anti-pattern trong Data Lakehouse, anti-pattern mà cá nhân tôi dễ vướng phải nhất là **"The Small File Problem"** (Vấn đề tệp nhỏ).

**Lý do:**
1. **Đặc thù dữ liệu:** Hệ thống dự án của tôi thường xuyên phải tiếp nhận dữ liệu streaming với tần suất cao nhưng dung lượng mỗi payload rất nhỏ (ví dụ: log events, sensor data, user clicks).
2. **Hậu quả:** Nếu không thực hiện việc `OPTIMIZE` (compact files) thường xuyên, số lượng tệp nhỏ sẽ tăng lên theo cấp số nhân trong thư mục `_delta_log` và thư mục data. Điều này làm tăng độ trễ (latency) đáng kể khi truy vấn do engine phải mất quá nhiều thời gian để thực hiện file listing và mở từng tệp.
3. **Giải pháp:** Để khắc phục, tôi cần thiết lập các job chạy `OPTIMIZE` định kỳ (ví dụ: mỗi giờ hoặc mỗi ngày) để gom các tệp nhỏ thành các tệp lớn (khoảng 256MB - 1GB) và áp dụng `Z-ORDER` cho các cột thường xuyên được filter để tăng cường tốc độ truy vấn thông qua tính năng file skipping.
