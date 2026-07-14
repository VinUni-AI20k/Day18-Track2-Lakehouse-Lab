# Reflection: Lakehouse Anti-patterns

- Họ tên: Lê Công Thành
- Mã HV: 2A202600091 

Trong 5 anti-patterns của Lakehouse, team chúng tôi đánh giá rủi ro dễ vướng nhất là **#3: Bỏ qua OPTIMIZE → small-file problem**.

**Lý do:**
Khi xây dựng các pipeline streaming hoặc ingestion liên tục (như log LLM API), dữ liệu thường được ghi vào Bronze layer dưới dạng các batch rất nhỏ (vài KB đến vài MB mỗi phút). Nếu không có cơ chế lập lịch (cron job) chạy lệnh `OPTIMIZE` định kỳ, số lượng file `.parquet` sẽ bùng nổ lên con số hàng trăm ngàn rất nhanh. 

Hậu quả là khi query lớp Silver hoặc Gold, engine (như DuckDB hay Spark) phải tốn phần lớn thời gian chỉ để mở/đọc metadata của hàng vạn file nhỏ thay vì thực sự xử lý dữ liệu, làm giảm hiệu năng system nghiêm trọng và làm chậm toàn bộ các downstream tasks. Việc thiết lập `OPTIMIZE` và `Z-ORDER` (như đã test ở NB2) là quan trọng nhưng lại dễ bị bỏ quên khi team chỉ tập trung vào việc đẩy được dữ liệu vào hệ thống.
