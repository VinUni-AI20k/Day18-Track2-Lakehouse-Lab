# Lab 18 Reflection

**Anti-pattern risk:** Bỏ qua OPTIMIZE (#3) và Dùng Spark cho dữ liệu nhỏ (#5).

**Reasoning:** 
1. Trong hệ thống LLM Observability, dữ liệu nạp vào liên tục tạo ra rất nhiều tệp nhỏ (**small-file problem**). Nếu không chạy `OPTIMIZE`, tốc độ truy vấn sẽ chậm đi 10 lần. Ở NB2, chúng ta đã thấy việc dùng `Z-ORDER` thay vì partition theo `user_id` (lỗi #2) giúp tránh tạo ra "triệu partition nhỏ" mà vẫn đảm bảo tốc độ.
2. Với tập dữ liệu Lab này (khoảng vài trăm MB đến vài GB), việc sử dụng Spark cluster là cực kỳ lãng phí (lỗi #5). Việc chọn **Lightweight path** với DuckDB và Polars giúp xử lý dữ liệu nhanh hơn và tiết kiệm chi phí gấp 10 lần mà vẫn đảm bảo đầy đủ tính năng của Lakehouse.

**Solution:** Thiết lập lịch `OPTIMIZE` định kỳ và ưu tiên sử dụng các công cụ query gọn nhẹ như DuckDB cho các tập dữ liệu dưới 100GB để tối ưu chi phí và hiệu năng.
