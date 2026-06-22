# Reflection

Anti-pattern mà team tôi dễ gặp nhất là **small-file problem**. Trong thực tế, dữ liệu thường được ghi liên tục từ nhiều tác vụ streaming hoặc micro-batch. Nếu mỗi batch tạo ra một file riêng, số lượng file sẽ tăng rất nhanh dù tổng dung lượng dữ liệu chưa lớn. Spark sau đó phải tốn nhiều thời gian đọc metadata, lập kế hoạch và mở từng file, làm truy vấn chậm hơn và tăng chi phí compute.

Kết quả NB2 giúp tôi thấy rõ tác động này: bảng ban đầu có 1.600 file; sau khi chạy OPTIMIZE và Z-ORDER, chỉ còn 1 file và truy vấn nhanh hơn khoảng 13 lần. Vì vậy, team cần theo dõi số lượng và kích thước file, điều chỉnh kích thước micro-batch, đồng thời lên lịch OPTIMIZE phù hợp. Z-ORDER cũng chỉ nên áp dụng cho những cột thường xuyên được dùng để lọc, tránh phát sinh thêm chi phí tối ưu không cần thiết.
