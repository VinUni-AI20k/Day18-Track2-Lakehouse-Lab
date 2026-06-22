# Reflection — Top 5 Lakehouse Anti-Patterns

Họ và tên: Hồ Thành Tiến

MSSV: ***2A202600868***

Anti-pattern dễ vướng nhất với team tôi: **"Overwriting instead of appending"** (ghi đè thay vì append).

Trong thực tế, khi pipeline LLM observability chạy mỗi giờ, rất dễ mặc định dùng `mode="overwrite"` cho Bronze layer vì code đơn giản hơn. Hậu quả là toàn bộ lịch sử raw data bị xóa — không còn khả năng audit, replay, hay rollback khi có lỗi upstream. Delta Lake sinh ra để giải quyết đúng vấn đề này: dùng `mode="append"` + transaction log cho phép time travel về bất kỳ version nào (như đã thực hành trong NB3 với `restore()`). Nếu không có ACID guarantees, một bug nhỏ trong generator có thể silently corrupt toàn bộ Bronze và không có cách nào recover. Bài học: Bronze layer phải luôn append-only; overwrite chỉ chấp nhận ở Gold khi rebuild aggregation từ Silver.
