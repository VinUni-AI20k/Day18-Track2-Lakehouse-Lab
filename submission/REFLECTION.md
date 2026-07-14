# Reflection — Day 18 Lab: Anti-pattern Risk Assessment

**Student:** Phan Nguyen Viet Nhan (2A202600279)

## Anti-pattern most at risk: Small-file proliferation (slide §5)

Anti-pattern (mẫu thiết kế lỗi) mà nhóm của tôi dễ gặp phải nhất là tích tụ các tệp nhỏ không kiểm soát (unbounded small-file accumulation) trong các đường ống nạp dữ liệu dạng streaming.

Trong bài kiểm tra NB2, chúng tôi đã mô phỏng chính xác tình trạng này: 200 micro-batches truyền dữ liệu, mỗi lô ghi một tệp riêng biệt, khiến số lượng tệp tăng vọt từ ~0 lên 200 mà không qua quá trình nén (compaction). Trước khi dùng lệnh OPTIMIZE, mọi truy vấn điểm (point-query) đều phải quét qua toàn bộ 200 tệp này. Sau khi thực hiện OPTIMIZE kết hợp với Z-ORDER, cùng truy vấn đó chỉ cần đọc 1 trong tổng số 55 tệp — đạt tỷ lệ lọc tệp (pruning ratio) gấp 55 lần và tốc độ thực tế (wall-clock speedup) nhanh hơn 8,4 lần.

Trong môi trường sản xuất thực tế, mẫu lỗi này tích tụ rất nhanh. Một bộ Kafka consumer ghi trực tiếp mọi micro-batch vào Delta Lake mà không có lịch trình nén dữ liệu có thể tích lũy hàng chục nghìn tệp chỉ trong vài giờ, làm suy giảm hiệu suất truy vấn và gây quá tải metadata cho bộ điều khiển.

Nguyên nhân gốc rễ là do việc tối ưu hóa đang được coi là tùy chọn thay vì là một hoạt động bảo trì định kỳ. Cách khắc phục rất đơn giản: lập lịch chạy OPTIMIZE + ZORDER vào các khung giờ thấp điểm, thiết lập target_size từ 128–256 MB và theo dõi số lượng tệp trong transaction log của Delta.

Lỗi này rất dễ bị bỏ qua ở giai đoạn đầu của dự án khi khối lượng dữ liệu còn ít và các truy vấn vẫn mang lại cảm giác nhanh nhạy.