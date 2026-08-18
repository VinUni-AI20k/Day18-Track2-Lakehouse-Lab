# Reflection

Anti-pattern mà dữ liệu của nhóm dễ gặp nhất là **bỏ quên small-file problem**. Nguồn dữ liệu quan sát LLM và agent trajectory đều có tính streaming, nên mỗi micro-batch riêng lẻ hoàn toàn hợp lệ nhưng có thể tạo ra hàng triệu file nhỏ theo thời gian. Hậu quả không chỉ là truy vấn chậm: catalog phải lập kế hoạch trên nhiều metadata hơn, object storage phát sinh nhiều GET request và compaction tự động trở nên đắt vì bị tính phí theo cả dung lượng lẫn số object.

NB2 và NB6 cho thấy cách xử lý phải mang tính vận hành, không phải sửa truy vấn một lần. Nhóm cần đặt lịch compaction, clustering và checkpoint; theo dõi file count, kích thước file trung bình và tỷ lệ file-skipping; đồng thời điều chỉnh trigger interval của writer. Chúng tôi cũng cần chạy snapshot expiry cùng orphan-file removal, vì VACUUM/expiry riêng lẻ không bảo đảm thu hồi mọi file do job lỗi để lại. Retention phải được đặt có chủ đích để cân bằng rollback, chi phí và quyền xóa dữ liệu.
