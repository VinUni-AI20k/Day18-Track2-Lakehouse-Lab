# Reflection: Lakehouse Architecture Anti-patterns

**Anti-pattern: The Small File Problem (Vấn đề tệp tin nhỏ)**

Trong kiến trúc Lakehouse, đội ngũ của chúng tôi dễ vướng vào lỗi **"Small File Problem"** nhất, đặc biệt là khi xây dựng các pipeline quan sát (observability) cho LLM như trong bài Lab này. 

**Lý do:**
1. **Dữ liệu streaming:** Dữ liệu từ các cuộc gọi API LLM thường được đẩy về theo thời gian thực hoặc các batch rất nhỏ. Nếu không có cơ chế quản lý, mỗi yêu cầu ghi sẽ tạo ra một tệp tin Parquet riêng biệt.
2. **Hệ quả:** Khi số lượng tệp lên đến hàng triệu, hiệu năng truy vấn của tầng Gold sẽ bị tê liệt do chi phí mở tệp và quét metadata quá lớn (như đã chứng minh trong NB2, tốc độ chậm hơn gấp 10 lần khi chưa tối ưu).

**Giải pháp rút ra:**
Qua bài Lab, tôi nhận thấy việc thiết lập các tiến trình bảo trì định kỳ sử dụng lệnh `OPTIMIZE` để gộp tệp (Compaction) và `Z-ORDER` để sắp xếp dữ liệu là yếu tố sống còn để duy trì hiệu năng của hệ thống khi dữ liệu tăng trưởng theo thời gian.
