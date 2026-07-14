# Lab Reflection

**Anti-pattern: The Small File Problem (Vấn đề tệp tin nhỏ)**

Trong kiến trúc Lakehouse của team, tôi nhận thấy hệ thống dễ gặp rủi ro nhất với lỗi **Small File Problem**.

**Lý do:**
1. **Quy trình nạp dữ liệu:** Các luồng dữ liệu thực tế (như LLM logs trong bài lab này) thường được ghi theo thời gian thực hoặc các batch rất nhỏ. Nếu không kiểm soát, mỗi lần ghi sẽ tạo ra một file Parquet mới, dẫn đến hàng ngàn file chỉ dung lượng vài KB.
2. **Hiệu năng suy giảm:** Qua Notebook 2, tôi đã chứng minh được rằng việc đọc 200 file nhỏ chưa tối ưu chậm hơn nhiều so với dữ liệu đã được gộp lại. Chi phí để hệ thống quản lý metadata và thực hiện I/O cho quá nhiều file nhỏ sẽ làm tê liệt khả năng truy vấn nhanh.

**Giải pháp từ Delta Lake:**
Bằng cách sử dụng lệnh `OPTIMIZE` định kỳ để thực hiện compaction (gộp file) và `Z-ORDER` để sắp xếp dữ liệu theo các cột thường xuyên truy vấn (như `user_id` hoặc `model`), chúng ta có thể giảm số lượng file và tận dụng tính năng *file skipping* để tăng tốc độ truy vấn lên gấp nhiều lần (như kết quả benchmark đạt được mức cải thiện ~7.9x).
