# Reflection

Anti-pattern mà dữ liệu của nhóm tôi dễ mắc phải nhất là **small-file problem**.
Khi hệ thống ghi dữ liệu theo từng micro-batch, số lượng file tăng rất nhanh dù
tổng dung lượng chưa lớn. Truy vấn phải mở nhiều file và đọc nhiều metadata,
nên độ trễ tăng, chi phí quét cũng tăng. Đây là vấn đề dễ bị bỏ qua vì pipeline
vẫn chạy đúng và dữ liệu vẫn đầy đủ.

Trong NB2, tôi thấy rõ sự khác biệt trước và sau khi compaction kết hợp
Z-ORDER: số file giảm mạnh và bộ máy có thể bỏ qua các file không chứa
`user_id` cần tìm. NB6 cho thấy việc sửa hậu quả không nên là giải pháp duy
nhất. Cần đặt kích thước batch hợp lý, theo dõi số file mỗi partition, rồi
lập lịch compaction và clustering định kỳ. Đồng thời phải có job dọn orphan
file sau các lần job lỗi; `VACUUM` không tự phát hiện những file chưa từng được
ghi vào transaction log.

Vì vậy, trong môi trường thật tôi sẽ coi file-count và metadata-size là chỉ số
vận hành bắt buộc, cùng với latency và chi phí truy vấn.
