Dựa trên các bài học, anti-pattern mà hệ thống dữ liệu của chúng tôi dễ vướng phải nhất là **"The Small-File Problem" (Vấn đề nhiều file nhỏ)**.

Do đặc thù ingestion từ các nguồn dữ liệu streaming (như log hệ thống, telemetry data, IoT) thường diễn ra liên tục với các batch nhỏ, nếu ghi trực tiếp vào Delta Lake mà không có cơ chế quản lý, bảng sẽ bị phân mảnh thành hàng ngàn, thậm chí hàng triệu file Parquet nhỏ (giống như bài tập ở NB2).

Điều này gây ra 2 rủi ro chính:
1. **Hiệu suất truy vấn giảm sút nghiêm trọng:** Do chi phí I/O khi mở quá nhiều file và thao tác đọc metadata trở nên quá tải.
2. **Chi phí lưu trữ và tính toán:** Các thao tác đọc, list metadata mất nhiều thời gian hơn.

**Giải pháp:** Để khắc phục, chúng tôi cần thiết lập các batch job chạy định kỳ lệnh `OPTIMIZE` kết hợp với `Z-ORDER` (như đã làm ở Silver/Gold layer) để gộp file, giúp Delta Engine loại bỏ (prune) file hiệu quả và tối ưu thời gian truy vấn.
