# Reflection

Anti-pattern mà nhóm tôi có nguy cơ gặp nhiều nhất là **small files**. Dữ liệu quan sát hệ thống LLM được ghi liên tục theo từng request và thường được đẩy xuống lakehouse bằng các micro-batch nhỏ. Nếu mỗi batch tạo một file mới, số object sẽ tăng nhanh dù tổng dung lượng dữ liệu chưa lớn. Kết quả ở NB2 và NB6 cho thấy vấn đề này không chỉ làm truy vấn chậm mà còn tăng chi phí lập kế hoạch, đọc metadata và vận hành compaction.

Rủi ro lớn nhất của nhóm là chỉ nhìn vào dung lượng lưu trữ mà bỏ qua số lượng file. Hướng khắc phục là đặt kích thước file mục tiêu, điều chỉnh chu kỳ flush của writer, theo dõi đồng thời số file và kích thước trung bình, rồi chạy compaction theo ngưỡng thay vì theo lịch cố định. Với các truy vấn thường lọc theo thời gian, model hoặc user, nhóm cũng cần chọn partition hợp lý và clustering/Z-order dựa trên workload thực tế. Compaction chỉ xử lý hậu quả; sửa cấu hình ghi mới là giải pháp lâu dài và tiết kiệm hơn.
