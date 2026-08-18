# Phản biện Thực tế Vận hành Lakehouse (Reflection)

* **Học viên:** Nguyễn Trọng Dũng (Mã HV: 01965)
* **Anti-Pattern rủi ro cao nhất:** #1 — Small-File Crisis do Stream Ingestion thiếu Scheduled Compaction.

**1. Bối cảnh:**
Nạp CDC và agent traces bằng micro-batches liên tục. Mỗi batch tạo một file Parquet riêng; thiếu bảo trì thì tầng lưu trữ phình lên hàng trăm nghìn file siêu nhỏ.

**2. Bằng chứng (NB2 & NB6):**
200 micro-batches tạo 200 files nhỏ, chi phí S3 `GET`/`LIST` chiếm >90% hoá đơn. Sau `compact()` + `z_order(["user_id"])`, file giảm >10x và point-query nhanh hơn rõ rệt.

**3. Ngộ nhận về VACUUM:**
`VACUUM` chỉ xoá file đã tombstone trong metadata. File rác do writer crash (chưa commit) vô hình trước `VACUUM` — cần Orphan Sweeper riêng (NB6 Job 4).

**4. Giải pháp:**
1. Cron Job Compaction gom file về 128–512 MB kèm Z-ORDER.
2. Orphan Sweeper: hiệu tập hợp Disk \ Log với age guard 24h.
