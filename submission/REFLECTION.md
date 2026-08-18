# REFLECTION

**Anti-pattern dễ vướng nhất: chạy snapshot expiry rồi coi như đã dọn rác.**

NB6 bác bỏ bằng số: `expire_snapshots` đưa 20 snapshot xuống 3, nhưng **40 file avro
vẫn nguyên 40 — xoá đúng 0 file**. Vì expiry chỉ gỡ con trỏ metadata; xoá file là pass
riêng mà pyiceberg không chạy. Nối thêm orphan sweep mới thu được 17 manifest list mồ côi.

Phía Delta tệ hơn: `VACUUM` không thấy 3 orphan do job crash để lại — file chưa từng
vào log nên chưa từng bị tombstone. Chỉ phép hiệu giữa đĩa (15 file) và log (10 file)
mới lộ ra 5 file đang trả tiền mà không ai thấy.

Quy ra tiền: bảng 500 GB / 2 triệu file tốn $990/tháng, trong đó **$240 là phí
per-object — 24% hoá đơn**, đúng phần expiry không bao giờ chạm tới. Full-scan 200
file nhỏ tốn $4/ngày; sau compaction (200 → 11 file) còn $0.08.

Team tôi vướng vì ingest liên tục sinh file nhỏ, ETL hay chết giữa chừng, và dashboard
chỉ đọc metadata — nơi orphan vô hình.

Hành động: expiry **luôn** kèm orphan sweep; cảnh báo khi số file trên đĩa lệch log.
