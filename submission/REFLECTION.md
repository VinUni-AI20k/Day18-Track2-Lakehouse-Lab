Chúng tôi dễ rơi vào anti-pattern “bỏ qua tối ưu hoá (no OPTIMIZE / no Z‑ORDER)”, cụ thể là để nhiều file nhỏ không được compact.

Tại sao: luồng ingest nhanh tạo ra hàng trăm file nhỏ (như trong NB2), dẫn đến thời gian truy vấn cao do engine phải mở nhiều file; điều này làm tăng chi phí I/O và làm suy yếu khả năng prune của Z‑order. Trong môi trường nhẹ (không Spark, chạy delta‑rs), thiếu nhiệm vụ định kỳ để compact và z‑order là nguyên nhân dễ thấy nhất vì nó không cần thay đổi mã nghiệp vụ, chỉ cần cấu hình vận hành.

Hành động khắc phục ngắn hạn: lên lịch `OPTIMIZE/compact()` định kỳ sau tải lớn và chạy `Z‑ORDER BY` theo cột truy vấn phổ biến (ví dụ `user_id`, `date`).

Hành động dài hạn: thêm kiểm tra CI cho `file count` và `file size` trên đường dẫn Delta; tự động hoá compaction khi file count vượt ngưỡng; giám sát latency truy vấn để phát hiện regressions.
