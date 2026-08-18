# Reflection — Top 5 Lakehouse Anti-Patterns

Em dễ vướng nhất ở anti-pattern coi các file Parquet là bảng dữ liệu hoàn chỉnh, thay vì bảng được quản lý bởi transaction log và metadata.

Khi pipeline lỗi giữa chừng, Parquet có thể đã được tạo nhưng chưa xuất hiện trong `_delta_log`. File vẫn tồn tại và có thể đọc được, nhưng không phải Delta table hợp lệ; VACUUM cũng không nhất thiết dọn được orphan file chưa từng commit. Cần kiểm tra log, schema, snapshot và trạng thái commit trước khi đọc hoặc xóa dữ liệu.

Trong thực tế, em sẽ tách vùng tạm khỏi vùng dữ liệu chính, chọn filesystem phù hợp, thiết kế job idempotent và có quy trình dọn orphan file an toàn.