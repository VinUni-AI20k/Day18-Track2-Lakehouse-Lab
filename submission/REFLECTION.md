# REFLECTION — Day 18 Lakehouse Lab

**Nguyễn Tuấn Khánh · 2A202601139**

## Anti-pattern dễ vướng nhất: small files (thiếu job compaction)

Tôi chọn nó vì nó **không đến từ code sai**. NB6 dựng lại đúng thứ một job ingest
Kafka trigger 5 giây làm qua đêm: 200 commit, mỗi commit hợp lệ, kết quả là 200 file
trung bình **51.5 KB** — xa mức 128–512 MB của production. Không dòng code nào cần
sửa; thứ thiếu là một cron job.

Con số làm tôi đổi cách nghĩ là chi phí. Với bảng 500 GB / 2 triệu file, managed
compaction tốn $750/tháng cho thành phần per-GB nhưng **$240/tháng cho thành phần
per-object** — 24% hoá đơn do *số lượng file*, không phải khối lượng dữ liệu. Nên
"để dịch vụ managed dọn hộ" không rẻ; sửa trigger interval của writer mới là.

Điều tôi không đoán trước: `expire_snapshots` hạ 20 → 3 snapshot nhưng **xoá 0 file
avro**. Job 3 và Job 4 là một *cặp* — chạy expiry mà không quét orphan chính là lý do
"đã expire mà hoá đơn S3 không giảm".
