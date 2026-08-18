# Reflection

Anti-pattern team dễ mắc nhất là coi maintenance là việc phụ, đặc biệt là
small files và orphan files.

NB2 cho thấy 200 file nhỏ được giảm còn 55 file sau OPTIMIZE + Z-ORDER,
speedup đạt 10.5× và files-pruned đạt 55×. NB6 cho thấy compaction giảm
200 file còn 11 file, nhưng VACUUM chỉ thu hồi file đã có trong transaction
log; ba orphan chưa từng commit phải được tìm bằng phép đối chiếu file trên
đĩa với file trong log. Tương tự, expire snapshot của Iceberg giảm 20 còn 3
snapshot nhưng không xóa file Avro cho đến khi chạy orphan sweep.

Rủi ro này dễ xảy ra trong production vì ingestion streaming tạo batch nhỏ
liên tục, còn maintenance thường bị trì hoãn. Hậu quả là query phải lập kế
hoạch qua nhiều metadata, chi phí request tăng và dữ liệu cũ vẫn chiếm
storage. Bài học chính là lakehouse cần lịch compaction, clustering, expiry
và orphan cleanup độc lập, có số đo trước/sau thay vì chỉ dựa vào trạng thái
“job đã chạy”.
