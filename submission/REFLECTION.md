# REFLECTION

Anti-pattern nhóm tôi dễ vướng nhất: **derived index đồng bộ một chiều,
không theo dõi vòng đời dữ liệu** — sync drift giữa lakehouse và vector
index bên ngoài.

NB7 tái hiện đúng lỗi: xoá 8 dòng của `user_042` khỏi lakehouse, nhưng
external vector index — đồng bộ một lần bằng upsert — vẫn trả về đủ 8
kết quả khi truy vấn. Lỗi không nằm ở việc quên xoá, mà ở kiến trúc:
pipeline sync chỉ biết "thêm mới", không có cơ chế lan truyền sự kiện
xoá.

Đây là rủi ro thực tế vì hệ thống RAG nào cũng tách riêng vector store
để tối ưu tốc độ — và một khi tách ra, nó dễ bị bỏ quên trong luồng
"xoá/sửa" dù được nhớ kỹ trong luồng "ghi mới". Hậu quả không chỉ là
dữ liệu cũ, mà còn vi phạm nghĩa vụ tuân thủ (right-to-erasure) nếu
index vẫn phục vụ nội dung đáng lẽ đã bị xoá.

Cách phòng tránh: dùng Change Data Feed để index subscribe vào sự kiện
xoá thay vì đoán, hoặc giữ embedding ngay trong bảng lakehouse để vòng
đời vector tự theo dòng dữ liệu gốc.
