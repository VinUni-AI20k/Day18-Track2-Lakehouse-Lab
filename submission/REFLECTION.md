
# Reflection — Top 5 Lakehouse Anti-Patterns

Anti-pattern mà nhóm tôi có nguy cơ gặp phải nhất là **“xem Lakehouse chỉ như một tập hợp các file thay vì một bảng dữ liệu được quản lý bằng transaction”**.

Qua bài lab, Delta Lake không chỉ lưu dữ liệu dưới dạng Parquet mà còn sử dụng transaction log để quản lý schema, lịch sử phiên bản và khả năng khôi phục. Ở NB1, schema enforcement ngăn dữ liệu sai kiểu được ghi vào bảng, trong khi schema evolution phải được bật một cách có chủ đích. NB3 cho thấy Time Travel và RESTORE phụ thuộc vào lịch sử transaction thay vì chỉ thao tác trực tiếp trên các file dữ liệu.

NB6 giúp hiểu rõ hơn về vấn đề vận hành: VACUUM không tự động xử lý mọi file mồ côi chưa từng được ghi nhận trong Delta log. Vì vậy cần có cơ chế phát hiện và dọn dẹp riêng. Tương tự, việc hết hạn snapshot của Iceberg không đồng nghĩa với việc tất cả file vật lý đã được thu hồi.

Bài học quan trọng nhất là cần xem Lakehouse như một hệ thống có transaction, lifecycle và cơ chế bảo trì rõ ràng. Schema validation, theo dõi lịch sử, compaction, cleanup, snapshot expiration và recovery phải được thiết kế ngay từ đầu thay vì xử lý như các công việc phát sinh sau này.
