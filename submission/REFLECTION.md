# Reflection

Trong 5 anti-pattern, nhóm chúng tôi dễ vướng nhất vào **#3 — "Bỏ qua OPTIMIZE → small-file problem"**. Đây không phải lỗi code mà là hệ quả tự nhiên của ingestion dạng streaming/micro-batch: chỉ cần thiếu một cron job compaction, dữ liệu tự "vỡ vụn" theo thời gian.

Số liệu tự đo trong lab cho thấy mức độ nghiêm trọng. NB2: sau 200 lần ghi nhỏ, point-query trước OPTIMIZE+Z-ORDER chậm hơn 8,5× so với sau (target ≥3×), số file giảm 200→55. NB6: cùng baseline 200 file, chi phí GET request cho full-scan là $4,00/ngày; nếu gộp về 4 file, chỉ còn $0,08/ngày — chênh 50×, dù dữ liệu vẫn nguyên 100.000 dòng (~10–16MB). Compaction thật trong NB6 đưa 200 file về 11 (~18× ít hơn), vượt xa ngưỡng ≥10× của rubric.

Điều nguy hiểm nhất: bảng vỡ vụn vẫn trả kết quả đúng, nên đội vận hành thường chỉ phát hiện khi bill storage tăng phi tuyến hoặc dashboard chậm bất thường, chứ không phải qua lỗi rõ ràng. Vì vậy fix theo slide — lên lịch `OPTIMIZE` định kỳ (cron) — là hướng đúng và rẻ hơn nhiều so với việc dọn dẹp thủ công về sau.
