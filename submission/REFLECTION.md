Họ và tên: Đoàn Văn Tuấn
Mã học viên: 2A202600046

Reflection: Data Anti-Pattern Risk
Dữ liệu của tôi có nguy cơ cao nhất rơi vào anti-pattern thứ nhất: "Đổ tất cả vào S3" (raw JSON, no schema).

Lý do: Đặc thù dữ liệu ML/LLM: Các dự án hiện tại sử dụng nhiều dữ liệu phi cấu trúc và đa phương thức (như video stream cho nhận diện buồn ngủ hay ảnh biển số xe). Việc thu thập dữ liệu thô từ các pipeline này rất dễ dẫn đến tình trạng lưu trữ tùy tiện mà không áp dụng schema ngay từ đầu.
=> Chúng nhanh chóng trở thành một đầm dữ liệu, khiến việc truy xuất và huấn luyện mô hình rất là khó khăn.
=> Cần thực hiện kiểm soát schema chặt chẽ và sử dụng các công nghệ quản lý bảng hiện đại để tránh biến hệ thống thành một kho chứa dữ liệu hỗn loạn.