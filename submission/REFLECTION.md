# Reflection

Anti-pattern dễ gặp nhất trong lab là small-file problem, xảy ra khi hệ thống ghi dữ liệu liên tục nhưng không có job maintenance chạy định kỳ. Đây là vấn đề thuộc nhóm Storage Optimization và Anti-Patterns.

Trong NB2, ghi 200 batch liên tục theo kiểu streaming tạo ra 200 file nhỏ. Sau khi chạy OPTIMIZE và Z-ORDER, số file giảm còn 55, tốc độ point-query tăng 14,4 lần và truy vấn `user_id=4242` chỉ cần đọc 1 file. NB6 cho thấy đây không phải lỗi code mà là hệ quả của ingest theo micro-batch, chẳng hạn Kafka trigger 5 giây, khi thiếu compact định kỳ. Small-file problem không chỉ ảnh hưởng hiệu năng mà còn tăng chi phí, vì object storage tính phí theo số request: 200 file mỗi query với 50 nghìn query mỗi ngày tốn hơn nhiều so với 4 file sau khi compact. Ngoài ra, VACUUM không dọn được orphan file chưa ghi vào transaction log, ví dụ file sót lại khi job crash.

Vì vậy nên thiết lập Job 1 compaction và Job 4 orphan sweep chạy định kỳ, thay vì chỉ xử lý thủ công khi query bắt đầu chậm.
