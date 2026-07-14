# Reflection: Lakehouse Architecture

**Anti-pattern nào từ slide §5 mà dữ liệu của team bạn có nguy cơ gặp phải nhất, và tại sao?**

Dữ liệu của tôi sẽ có nguy cơ cao nhất gặp phải anti-pattern **"Small-File Problem"**. Tôi thu thập dữ liệu telemetry và clickstream liên tục từ hàng ngàn người dùng ứng dụng di động đồng thời. Vì cần analytics gần như real-time, tôi ingest dữ liệu này vào Lakehouse theo các micro-batches nhỏ và thường xuyên (ví dụ: mỗi 1-2 phút). 

Mặc dù điều này đáp ứng yêu cầu về ingestion latency, nhưng nó tạo ra hàng ngàn file Parquet/JSON nhỏ trong các bảng Bronze và Silver Delta. Số lượng file khổng lồ này làm tê liệt read performance của các truy vấn downstream reporting vì các query engines (như DuckDB, Spark, hoặc Trino) dành nhiều thời gian hơn để liệt kê các file và đọc metadata hơn là xử lý dữ liệu thực tế. 

Để khắc phục điều này, cần phải chạy các lệnh `OPTIMIZE` định kỳ để nén các file nhỏ thành các chunks lớn hơn (ví dụ: 128MB - 256MB) và áp dụng `Z-ORDER` clustering trên các cột thường xuyên được lọc, khôi phục lại query performance tối ưu mà không làm ảnh hưởng đến ingestion speed.
