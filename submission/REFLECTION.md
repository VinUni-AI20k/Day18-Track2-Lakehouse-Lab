# REFLECTION

Anti-pattern mà mình dễ gặp nhất là tạo quá nhiều small files do ingest liên tục với batch nhỏ. Với dữ liệu của team, luồng ghi thường đến từ nhiều nguồn và tần suất cao, nên nếu mỗi lần chỉ append một lượng nhỏ thì bảng Delta sẽ nhanh chóng bị phân mảnh. Khi đó, query chậm hơn, metadata nặng hơn, và các tối ưu như OPTIMIZE/Z-ORDER cũng phải làm việc nhiều hơn để bù lại.

Mình nghĩ rủi ro này cao hơn các anti-pattern khác vì nó xuất hiện rất tự nhiên trong quá trình vận hành hằng ngày. Nếu không chủ động gom batch, compact định kỳ, và thiết kế partition hợp lý, hệ thống sẽ chậm dần theo thời gian dù dữ liệu không tăng đột biến. Vì vậy, team mình cần ưu tiên kiểm soát kích thước file ngay từ đầu thay vì đợi đến lúc hiệu năng giảm mới xử lý.
