# REFLECTION — Lakehouse Anti-Patterns

**Anti-pattern đội tôi dễ vướng nhất: tin rằng "chạy `vacuum` / `expire_snapshots` là đã dọn xong và giảm được chi phí lưu trữ."**

NB6 cho tôi hai bằng chứng ngược trực giác, đo thật trên máy:

- `VACUUM` của delta-rs chỉ thu hồi file đã bị *tombstone* trong transaction log. Ba orphan do job crash để lại (chưa từng commit) **vô hình** với vacuum ở mọi retention — phải tự quét bằng phép hiệu tập hợp (file trên đĩa − file trong `_delta_log`) mới tìm và xoá được.
- `expire_snapshots` của Iceberg đưa 20→3 snapshot nhưng **0 file avro bị xoá**, metadata còn phình thêm. Job expiry và job orphan-sweep là một **cặp**; chạy expiry một mình chính là lý do "đã expire mà hoá đơn S3 không giảm".

Dữ liệu đội tôi gồm nhiều pipeline ingest chạy song song, hay retry/crash giữa chừng — đúng kiểu sinh ra orphan chưa commit. Nếu chỉ đặt lịch `vacuum`/`expire` mặc định, chi phí object storage sẽ âm thầm leo thang.

**Bài học:** maintenance phải gồm cả orphan reconciliation, và luôn *đo byte thật sự giảm* thay vì tin lời hứa của API.
