# Phản biện Thực tế Vận hành Lakehouse (Reflection)

* **Học viên:** Mai Việt Anh (Mã HV: 2A202601083 | @VietAnhETE16)
* **Anti-Pattern đối mặt rủi ro cao nhất:** #1 — Small-File Crisis & Stream Ingestion Without Scheduled Compaction.

**1. Bối cảnh rủi ro:**
Hệ thống nạp streaming CDC và agent traces theo micro-batches vài giây/lần. Dù commit chuẩn ACID, việc thiếu bảo trì khiến tầng lưu trữ phình to hàng trăm nghìn file Parquet siêu nhỏ (vài KB).

**2. Bằng chứng thực nghiệm (NB2 & NB6):**
200 micro-batches liên tiếp tạo 200 files nhỏ, đẩy chi phí S3 request `GET`/`LIST` áp đảo ($>90\%$ hóa đơn) và làm thời gian query planning tăng phi mã.

**3. Ngộ nhận về VACUUM:**
Đội ngũ lầm tưởng `VACUUM` dọn sạch mọi file rác. Thực tế, `VACUUM` chỉ xóa file đã *tombstone* trong metadata. File rác do writer crash bỏ lại chưa từng commit sẽ vô hình trước `VACUUM`, âm thầm gây rò rỉ dung lượng.

**4. Giải pháp khắc phục:**
1. Lập lịch Cron Job Compaction gom file về ngưỡng $128\text{--}512\text{ MB}$ kèm Z-ORDER.
2. Triển khai Orphan Sweeper tính hiệu tập hợp giữa disk và metadata (kèm age guard 24h).
