# Reflection

**Anti-pattern rủi ro nhất: quá nhiều small files.**

Trong bài lab này, dữ liệu tôi xử lý có nguy cơ cao gặp anti-pattern này vì log LLM và agent trace được ghi liên tục theo micro-batch. Nếu mỗi worker ghi một file cho từng batch nhỏ, số object sẽ tăng nhanh hơn dung lượng dữ liệu. Kết quả NB2 và NB6 cho thấy tác hại không chỉ nằm ở tốc độ đọc: engine còn phải list object, mở footer, lập kế hoạch scan và duy trì metadata cho từng file. Vì vậy chi phí tăng gần theo số file, ngay cả khi tổng dữ liệu chưa lớn.

Tôi sẽ đặt mục tiêu kích thước file khoảng 128–512 MB, chạy compaction theo ngưỡng số file thay vì lịch cố định, và cluster theo khóa truy vấn thực tế như `user_id` hoặc thời gian. Tôi cũng sẽ theo dõi số file trên mỗi partition, kích thước trung vị và pruning ratio. Tôi không chọn giải pháp chỉ tăng kích thước micro-batch vì nó làm tăng độ trễ ingestion nhưng không xử lý các file nhỏ đã tồn tại.
