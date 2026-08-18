# Reflection — Top 5 Lakehouse Anti-Patterns

Anti-pattern nguy hiểm nhất với team tôi là **Small Files** (NB2/NB6).

Hệ thống của team thường ingest data qua streaming với trigger ngắn (mỗi vài giây), dẫn đến hàng nghìn file nhỏ tích tụ mỗi ngày. Lab NB6 đã đo: 200 micro-batch tạo ra 200 file, query chậm phi tuyến tính. Nếu không có compaction job chạy định kỳ, metadata overhead tăng và file-skipping (Z-order) mất tác dụng vì quá nhiều file cần scan.

NB6 cũng làm lộ ra điều không ai nói: VACUUM Delta không xoá orphan chưa từng commit — file từ job crash biến mất khỏi log nhưng vẫn chiếm disk. Job 3 và Job 4 phải chạy thành cặp; expire_snapshots một mình không giảm được hoá đơn S3.
