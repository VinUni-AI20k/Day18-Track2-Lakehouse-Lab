# REFLECTION

**Anti-pattern team tôi dễ vướng nhất: chạy snapshot expiry mà không chain orphan sweep.**

NB6 đo được trên máy tôi: `expire_snapshots` giảm 20 → 3 snapshot nhưng **0 file avro bị xoá**,
và metadata trên đĩa còn *tăng* từ 310.4 KB lên 316.8 KB. Chỉ khi chain sang orphan sweep, avro
mới xuống 40 → 23 và metadata về 280.6 KB. Job 3 và Job 4 là một **cặp**; chạy riêng Job 3 thì
hoá đơn storage không bao giờ giảm — triệu chứng "đã expire mà bill S3 không giảm".

Dữ liệu team tôi nguy hiểm đúng kiểu này vì ingestion theo micro-batch. NB6 tái hiện: 200 commit
→ 200 file, trung bình 50 KB/file, trong khi production nhắm 128–512 MB. Ở NB5, metadata chiếm
**267.7%** kích thước dữ liệu vì mỗi file chỉ 4.7 KB — file nhỏ phạt hai lần: nhiều data file
để đọc *và* nhiều metadata để plan.

Điều tôi sẽ làm khác: coi expiry và orphan sweep là **một** job, và alert theo *số file* chứ
không theo dung lượng. Đánh đổi đã chấp nhận: `retention_hours=0` trong lab phá time travel —
production phải ≥ 168h, và đó là quyết định được viết ra, không phải giá trị mặc định.
