# Reflection

Anti-pattern dễ vướng nhất là biến data lake thành “data swamp”: ingest nhanh Bronze nhưng thiếu hợp đồng schema, ownership và kiểm tra chất lượng trước khi downstream dùng. Với dữ liệu LLM observability, rủi ro này càng cao vì log đến từ nhiều gateway/model/provider, schema thay đổi liên tục, retry tạo duplicate request_id, và một phần payload có thể lỗi hoặc thiếu field.

Lab này cho thấy cách giảm rủi ro bằng medallion pipeline: Bronze giữ raw event để audit, Silver parse/dedup/chuẩn hóa schema, Gold chỉ expose metric đã aggregate như p50/p95 latency, cost_usd và error_rate. Nếu áp dụng cho team thật, mình sẽ bắt buộc mỗi bảng Silver có quality check tối thiểu, data owner rõ ràng, và dashboard chỉ đọc từ Gold thay vì query trực tiếp raw logs.