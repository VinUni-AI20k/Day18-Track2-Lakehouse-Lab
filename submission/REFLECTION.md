# Reflection — Top 5 Lakehouse Anti-Patterns

Trong số các anti-pattern phổ biến của Lakehouse, anti-pattern team tôi dễ vướng nhất là
**"Không expire snapshot kết hợp với orphan file cleanup"**.

Trước khi làm lab, tôi nghĩ chỉ cần `expire_snapshots()` là dữ liệu cũ sẽ bị xóa và
chi phí lưu trữ giảm. Thực tế NB6 chứng minh ngược lại: sau khi expire từ 20 xuống còn
3 snapshot, **0 file Avro bị xóa** — thậm chí metadata còn phình ra thêm. Phải chạy
thêm `delete_orphan_files()` sau đó mới thực sự giải phóng storage.

Lý do team dễ vướng là vì trong môi trường dev, volume dữ liệu nhỏ nên hóa đơn S3
không thay đổi rõ ràng — bug âm thầm tồn tại. Đến khi lên production với hàng triệu
snapshot/ngày, chi phí tăng đột biến mà không rõ nguyên nhân. Job 3 và Job 4 phải luôn
chạy thành **một cặp**, đây là bài học trực tiếp từ lab này.

Anti-pattern thứ hai đáng chú ý là **không dedup ở tầng Silver**: NB4 cho thấy ~5%
request_id bị trùng do retry pattern — nếu aggregate thẳng từ Bronze lên Gold sẽ ra số
liệu sai, p95 latency bị inflate.
