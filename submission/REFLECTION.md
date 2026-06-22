# REFLECTION — Top 5 Lakehouse Anti-Patterns
**Mã SV:** 2A202600665

Anti-pattern team dễ vướng nhất: **"Small-file problem"** (tích tụ hàng nghìn file nhỏ từ streaming ingestion mà không OPTIMIZE định kỳ).

Trong lab này, NB2 đã mô phỏng rõ: 200 batch appends → 200 files nhỏ, query chậm 11.6× so với sau khi compact + Z-order. Ở production, team thường chỉ tập trng vào logic ETL mà quên mất chi phí đọc hàng trăm file nhỏ mỗi lần query. Nếu không có job OPTIMIZE scheduled (ví dụ hourly compact + daily Z-order), performance sẽ degrade dần, nhất là với dashboard real-time cần scan nhiều partitions.

Anti-pattern khác cũng đáng quan tâm là "No schema enforcement" — cho phép ghi dữ liệu với schema linh tinh, dẫn đến silent corruption ở Silver/Gold layer.
