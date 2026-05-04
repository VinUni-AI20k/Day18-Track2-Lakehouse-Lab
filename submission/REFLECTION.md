# Reflection

**Rủi ro theo Slide §5 dễ vướng phải nhất tại team:** Anti-pattern "Đổ tất cả vào S3" dưới dạng raw JSON, không có schema rõ ràng.

**Vì sao?**
Trong thực tế, khi cần triển khai nhanh, team rất dễ đẩy toàn bộ dữ liệu từ API hoặc Kafka lên object storage dưới dạng JSON thô mà chưa chốt schema và data contract. Cách làm này ban đầu có vẻ linh hoạt, nhưng về lâu dài rất dễ biến Data Lake thành Data Swamp: cùng một trường có thể đổi kiểu dữ liệu, thiếu cột, hoặc đổi tên giữa các đợt ingest. Khi đó các bước đọc dữ liệu ở Silver/Gold sẽ dễ hỏng, khó debug, và tốn nhiều thời gian làm sạch thủ công.

**Giải pháp:** Dùng Delta Lake để áp dụng schema enforcement ngay từ Bronze, chỉ cho phép schema evolution khi đã được kiểm soát. Đồng thời chuẩn hóa naming, partitioning, và metadata sớm để dữ liệu ở các lớp sau vẫn truy vết và tái sử dụng được.
