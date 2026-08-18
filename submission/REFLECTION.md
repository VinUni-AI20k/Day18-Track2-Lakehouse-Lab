# Reflection

**Câu hỏi:** Trong "Top 5 Lakehouse Anti-Patterns", team bạn dễ vướng cái nào nhất, vì sao?

Trong số các Lakehouse Anti-Patterns, team mình dễ vướng phải lỗi **"The Small-File Problem" (Vấn đề file nhỏ)** nhất. 

Lý do là vì trong quá trình xây dựng pipeline thu thập dữ liệu (ingestion), team thường có xu hướng stream dữ liệu thành các micro-batch nhỏ liên tục để dữ liệu được cập nhật nhanh nhất (giảm latency). Tuy nhiên, nếu quên thiết lập các job bảo trì chạy ngầm định kỳ, việc này sẽ nhanh chóng tạo ra hàng vạn file Parquet vụn vặt. 

Như đã thấy ở Notebook 02, điều này làm phình to transaction log và khiến các câu truy vấn bị chậm đi rất nhiều do engine phải tốn IO để mở từng file nhỏ và phân tích metadata. 

Để khắc phục triệt để, team sẽ cần thiết lập lịch trình tự động chạy lệnh `OPTIMIZE` (compaction) kết hợp với `Z-ORDER` định kỳ để gom gọn file, giúp hệ thống đọc nhanh hơn đáng kể.
