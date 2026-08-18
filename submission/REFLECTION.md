# Reflection

**Anti-pattern #4 — `VACUUM 0 HOURS` để "tiết kiệm storage"** là cái team em dễ vướng nhất — không phải vì định set retention về 0, mà vì NB6 cho thấy *ngay cả VACUUM đúng cách cũng không an toàn như tưởng*.

Sau `dry-run`, `deltalake` báo 211 file tombstoned có thể dọn — nhưng **5 file orphan** do job giả lập crash để lại (chưa commit vào log) vẫn nằm nguyên trên đĩa, vô hình với mọi retention. Team mới dễ tin "chạy VACUUM xong là sạch", nhưng thực tế phải tự trừ tập hợp (đĩa vs. log) mới thấy khoảng trống đó.

Iceberg còn rõ hơn: `expire_snapshots` đưa 20 snapshot xuống còn 3, nhưng **0 file avro bị xoá**, metadata còn phình thêm — phải chạy tiếp Job 4 (orphan sweep) mới thu hồi 36.8 KB từ 17 manifest mồ côi. Job 3 và Job 4 là một cặp; chạy riêng Job 3 là lý do kinh điển "đã expire mà hoá đơn S3 không giảm".

Rủi ro thật với team em: cắt retention để tiết kiệm chi phí mà không audit orphan riêng, vừa mất time travel, vừa không đạt mục tiêu tiết kiệm.
