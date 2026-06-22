# Reflection — Top 5 Lakehouse Anti-Patterns

Anti-pattern dễ vướng nhất: **"Schema drift silently breaks downstream"** — ghi dữ liệu mới với schema thay đổi mà không enforce, khiến pipeline Silver/Gold nhận NULL hoặc crash mà không có cảnh báo rõ ràng.

Trong thực tế khi xây LLM observability pipeline (NB4), payload JSON từ các model khác nhau rất dễ thay đổi field names theo thời gian — ví dụ `usage.input` đổi thành `usage.prompt_tokens`. Nếu Bronze chỉ lưu raw string mà không có schema enforcement ở lớp Silver, lỗi sẽ âm thầm tạo ra Gold metrics với giá trị NULL, và team chỉ phát hiện khi dashboard báo cost_usd = 0 sau nhiều ngày.

Delta Lake giải quyết bằng hai cơ chế bổ sung nhau: schema enforcement chặn write không tương thích ngay tại Bronze, và `schema_mode="merge"` cho phép opt-in evolution có kiểm soát. NB1 minh họa rõ — bad write bị block ngay lập tức thay vì tạo ra corrupt data lan xuống toàn bộ medallion.

Bài học: **enforce schema càng sớm càng tốt trong pipeline**, không để schema drift tích lũy đến Gold layer mới phát hiện.

---
*Dương Quang Khải — AICB-P2T2 Day 18 Lab*
