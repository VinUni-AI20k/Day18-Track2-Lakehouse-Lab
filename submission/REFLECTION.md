**Họ và tên:** Lương Anh Tuấn  
**MSSV:** 2A202600113

**Which anti-pattern from slide §5 would your team's data be most at risk of, and why?**
Dữ liệu của nhóm em có nguy cơ cao nhất rơi vào anti-pattern #3: bỏ qua OPTIMIZE và tạo quá nhiều file nhỏ. Trong pipeline lakehouse, các lần ghi incremental từ bước ingest, làm sạch dữ liệu hoặc thử nghiệm bằng notebook rất dễ sinh ra nhiều file Parquet/Delta nhỏ, đặc biệt khi job chạy theo batch nhỏ hoặc bị chạy lại nhiều lần trong lúc phát triển. Dữ liệu vẫn có thể đúng về mặt nội dung, nên vấn đề này dễ bị bỏ qua cho đến khi truy vấn chậm rõ rệt vì overhead đọc metadata và liệt kê file tăng lên.

Rủi ro này đáng lo hơn over-partitioning hoặc dùng Spark cho query nhỏ vì small-file problem có thể tích tụ dần ngay cả khi thiết kế bảng ban đầu khá hợp lý. Cách giảm rủi ro là lên lịch OPTIMIZE/compaction sau ingest, theo dõi số lượng file và kích thước file trung bình, đồng thời tránh ghi quá nhiều micro-batch nếu không thật sự cần cập nhật gần realtime.
