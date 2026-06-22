# Reflection

Anti-pattern mà dữ liệu của nhóm có nguy cơ gặp nhiều nhất là **quá nhiều file nhỏ**. Dữ liệu quan sát LLM thường được ghi liên tục theo từng batch nhỏ hoặc từng micro-batch streaming. Cách ghi này giúp dữ liệu xuất hiện nhanh, nhưng nếu không có chiến lược compact định kỳ thì số lượng file tăng rất nhanh, làm chi phí đọc metadata và lập kế hoạch truy vấn lớn hơn chi phí xử lý dữ liệu thực tế.

Kết quả trong notebook 02 cho thấy vấn đề khá rõ: bảng ban đầu có 200 file và truy vấn điểm mất khoảng 100.5 ms. Sau OPTIMIZE và Z-order, bảng còn 55 file, truy vấn còn khoảng 11.7 ms, nhanh hơn 8.6 lần; chỉ 1 trong 55 file cần được đọc cho `user_id` mục tiêu.

Vì vậy, nhóm nên theo dõi kích thước và số file theo partition, đặt lịch compaction, chọn partition có cardinality hợp lý, và dùng Z-order cho các cột lọc thường xuyên thay vì partition theo cột cardinality cao.
