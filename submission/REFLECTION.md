# Reflection

Trong Top 5 Lakehouse Anti-Patterns, em nghĩ dễ mắc sai lần nhất là coi metadata và lifecycle maintenance như công việc phụ. Delta VACUUM không tự tìm orphan chưa từng commit, còn Iceberg expire snapshots không đồng nghĩa với việc đã dọn hết manifest/orphan files. Nếu chỉ nhìn số snapshot hoặc query vẫn chạy, ta có thể tưởng rằng storage đã giảm trong khi metadata và file rác vẫn tăng.

Các notebook cho thấy cách phòng tránh là đo trước/sau cho từng job, giữ catalog làm control plane, kiểm tra file statistics để xác nhận pruning, và pin table version cho training/replay. Lỗi khác dễ mắc phải là external vector index mà không quản lý lifecycle cùng bảng dữ liệu; khi row bị xoá, index cũ vẫn trả kết quả. Vì vậy embedding và trạng thái cần được truy vết trong bảng, còn index ngoài phải có cơ chế refresh hoặc kiểm tra version.
