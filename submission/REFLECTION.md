# Reflection

**Anti-pattern nhóm dễ vướng nhất: bỏ qua job bảo trì định kỳ (compaction / orphan sweep), dẫn tới small-file accumulation.**

NB6 chứng minh đây không phải lỗi do code sai — 200 micro-batch ghi đúng logic vẫn tạo ra 200 file nhỏ (~51 KB/file), khiến chi phí GET tăng phi tuyến: ~$4/ngày so với ~$0.08/ngày nếu đã compact — chênh 50×, chỉ vì thiếu một cron job.

Nhóm dùng pattern ingest micro-batch (giống Kafka trigger 5s) ở nhiều bài lab khác, nên đây là kịch bản thực tế nhất: viết code đúng, nhưng quên lên lịch `OPTIMIZE`/`compact()` sau triển khai — lỗi chỉ lộ ra sau vài tuần khi file tích luỹ đủ lớn để chậm query hoặc đội chi phí, lúc đó khó truy lại nguyên nhân gốc.

Bài học cụ thể nhất: **Job 3 và Job 4 phải chạy thành cặp** (NB6) — `expire_snapshots`/`VACUUM` chỉ đụng metadata, không tự xoá file mồ côi. Chỉ chạy expiry mà quên sweep, storage bill không giảm dù "đã dọn dẹp" — cái bẫy dễ bỏ sót khi tự động hoá pipeline maintenance.
