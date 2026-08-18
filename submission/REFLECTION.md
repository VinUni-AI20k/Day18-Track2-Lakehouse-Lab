# Phản biện Thực tế Vận hành Lakehouse (Reflection)

* **Học viên:** Nguyễn Trọng Dũng (Mã HV: 01965)
* **Anti-Pattern đối mặt rủi ro cao nhất:** #1 — Small-File Crisis do Stream Ingestion thiếu Scheduled Compaction.

**1. Bối cảnh rủi ro:**
Hệ thống nạp CDC và agent traces bằng micro-batches liên tục. Mỗi batch tạo một file Parquet riêng, dù commit đúng ACID nhưng không có bảo trì thì tầng lưu trữ sẽ phình lên hàng trăm nghìn file siêu nhỏ (vài KB).

**2. Bằng chứng thực nghiệm (NB2 & NB6):**
200 micro-batches liên tiếp tạo ra 200 files nhỏ, query planning tăng phi mã và chi phí S3 `GET`/`LIST` chiếm >90% hoá đơn. Sau khi chạy `compact()` + `z_order(["user_id"])`, file giảm >10x và tốc độ point-query cải thiện rõ rệt.

**3. Ngộ nhận về VACUUM:**
Nhiều đội nhầm rằng `VACUUM` dọn sạch mọi file rác. Thực tế, `VACUUM` chỉ xoá file đã tombstone trong metadata. File rác do writer crash bỏ lại (chưa từng commit) sẽ vô hình trước `VACUUM` — cần Orphan Sweeper riêng (NB6 Job 4).

**4. Giải pháp:**
1. Cron Job Compaction gom file về 128–512 MB kèm Z-ORDER theo cột truy vấn chính.
2. Orphan Sweeper: hiệu tập hợp Disk \ Log với age guard 24h để tránh xoá file đang ghi.
