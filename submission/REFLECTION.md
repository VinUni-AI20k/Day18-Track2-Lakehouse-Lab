# REFLECTION

**Anti-pattern team tôi dễ vướng nhất: bỏ qua job maintenance — cụ thể là
chạy expiry mà không quét orphan.**

Không phải vì nó khó, mà vì nó *im lặng*. NB6 cho tôi hai con số:

- `VACUUM` của delta-rs không thấy 3 file orphan tôi cố tình tạo. Chúng chưa
  từng vào transaction log nên không có tombstone để thu hồi — vô hình ở mọi
  retention. Bảng vẫn báo đúng 100.000 dòng trong khi 5 file rác nằm trên đĩa.
- `expire_snapshots` của Iceberg đưa 20 → 3 snapshot nhưng xoá **0 file avro**,
  metadata còn phình 321,6 → 328,6 KB. Phải chạy tiếp orphan sweep mới thu hồi
  được 36,4 KB.

Pipeline team tôi ingest micro-batch và job hay bị kill giữa chừng — đúng điều
kiện sinh orphan chưa commit. Chúng tôi có cron cho compaction và expiry, không
có gì cho orphan. Dashboard "đã dọn xong" vẫn xanh trong khi hoá đơn lưu trữ
không giảm, vì con số duy nhất được theo dõi — số snapshot — giảm đúng kỳ vọng.

Hành động: thêm job hiệu tập hợp (disk \ log) ngay sau expiry, và alert trên
**bytes thu hồi được**, không phải số snapshot.
