Hệ thống dữ liệu của team chúng tôi dễ mắc phải lỗi **"Bỏ qua OPTIMIZE/Z-ORDER dẫn đến Small-Files Problem"** nhất.

Nguyên nhân là do đặc thù hệ thống thường xuyên tiếp nhận dữ liệu từ các luồng streaming (ví dụ: log sự kiện từ API, dữ liệu CDC) liên tục đổ về Lakehouse dưới dạng micro-batch. Quá trình này sẽ nhanh chóng sản sinh ra hàng ngàn file Parquet có kích thước cực nhỏ (chỉ vài KB) mỗi ngày trên S3. Nếu bỏ quên việc bảo trì, số lượng file nhỏ khổng lồ sẽ gây nghẽn I/O khi hệ thống phải tốn quá nhiều thời gian để quét metadata, làm suy giảm nghiêm trọng hiệu năng truy vấn trên các bảng Silver và Gold.

**Giải pháp khắc phục:**
1. **Lên lịch bảo trì tự động định kỳ:** Thiết lập một cron job hoặc luồng Airflow chạy lệnh `OPTIMIZE` kết hợp `Z-ORDER BY` (trên các cột thường xuyên bị filter như `tenant_id` hoặc `timestamp`) vào khung giờ thấp điểm.
2. **Kiểm soát kích thước file:** Cấu hình property `delta.targetFileSize` (thường từ 128MB - 256MB) cho các bảng Delta để hệ thống tự động gộp file đạt mức tối ưu nhất khi đọc.
