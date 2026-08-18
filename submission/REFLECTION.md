# Reflection — Top 5 Lakehouse Anti-Patterns

**Question:** Trong "Top 5 Lakehouse Anti-Patterns", team bạn dễ vướng cái nào nhất, vì sao?

## Chọn: #2 — The Small-File Problem (Tumbling Tables)

Streaming ingestion viết liên tục mà không có compaction schedule. Kết quả: hàng triệu micro-files → query chậm, billing tăng phi tuyến tính.

**Tại sao dễ vướng:**

1. **Tưởng không sao** — mỗi batch đều "đúng" (append, <1ms). Không lỗi, không alert.

2. **Hậu quả trì hoãn** — sau 1 tháng, dashboard timeout. Incident 3 AM: "đã chạy 6 tháng."

3. **Fix đắt** — compaction cần compute + storage tạm. Team né.

**Bài học từ lab:** NB2: 200 file → 55 file (11×). NB6: 200 → 10 file (20×). Đặt lịch compaction từ ngày 1.

**Word count: 165**
