# Reflection

Anti-pattern dễ gặp nhất trong hệ thống này là **Small Files**.

Nguyên nhân là dữ liệu AI/LLM thường được ghi liên tục theo micro-batch hoặc streaming. Trong lab, 200 lần ghi nhỏ tạo ra 200 file cho 100.000 dòng dữ liệu. Khi số lượng file tăng, hệ thống phải xử lý nhiều metadata và mở nhiều file hơn, từ đó làm query chậm và tăng chi phí.

Ở NB6, sau khi compaction, số file giảm từ 200 xuống 11, tương đương khoảng 18 lần. Sau khi clustering theo `user_id`, một point query chỉ cần đọc 1/10 file thay vì 11/11 file.

Kết quả này cho thấy small-file problem có thể xuất hiện ngay cả khi pipeline vẫn hoạt động đúng. Vì vậy, khi triển khai Lakehouse thực tế cần có các job bảo trì định kỳ như compaction, clustering, vacuum/orphan removal và checkpoint, đồng thời nên thiết kế chu kỳ ghi dữ liệu hợp lý ngay từ đầu để hạn chế tạo quá nhiều file nhỏ.