# Lab 18 Reflection

**Anti-pattern: The "Small-File Problem" (Streaming/Append-only without Optimization)**

Trong bối cảnh dữ liệu LLM observability như trong bài Lab này, đội ngũ phát triển thường có xu hướng đẩy dữ liệu log theo thời gian thực (streaming ingestion). Nếu không có cơ chế quản lý Delta Lake tốt, việc mỗi request hoặc mỗi batch nhỏ được ghi thành một file Parquet riêng lẻ sẽ dẫn đến hàng triệu file nhỏ sau một thời gian ngắn.

Vấn đề này cực kỳ nguy hiểm vì nó làm tăng đáng kể overhead cho metadata layer và khiến việc đọc dữ liệu trở nên chậm chạp do phải mở/đóng quá nhiều file. Trong bài Lab này, chúng ta đã thấy NB2 minh họa rõ rệt việc 200 file nhỏ làm chậm truy vấn như thế nào, và cách `OPTIMIZE` cùng `Z-ORDER` giải quyết triệt để vấn đề đó bằng cách gộp file và tối ưu hóa việc bỏ qua file (file skipping). Nếu không chú ý, đây là lỗi phổ biến nhất mà các team mới làm quen với Lakehouse dễ vướng phải.
