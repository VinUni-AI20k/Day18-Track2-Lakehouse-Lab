# Reflection — Top 5 Lakehouse Anti-Patterns

**Anti-pattern em mắc phải: bỏ qua table maintenance, cụ thể là chạy
snapshot expiry mà không quét orphan.**

Pipeline của nhóm ingest telemetry theo micro-batch — đúng hình dạng NB6 dựng lại:
200 commit nhỏ, file trung bình 51.5 KB, trong khi production nhắm 128–512 MB.
Compaction thì đã có cron. Cái thiếu là vế còn lại.

NB6 đo ra điều tôi vẫn tin ngược: `expire_snapshots` đưa Iceberg từ 20 xuống 3
snapshot nhưng **xoá đúng 0 file avro**, metadata còn tăng 337.6 → 345.4 KB. Phải
chạy tiếp phép hiệu tập hợp mới thu hồi được 37.0 KB từ 17 manifest list mồ côi.
Bên Delta cũng vậy: `VACUUM` không thấy 3 file do writer crash để lại, vì chúng
chưa từng được commit nên chưa từng bị tombstone.

Nghĩa là dashboard "đã expire" của nhóm không chứng minh được gì về hoá đơn lưu trữ.

**Khắc phục:** ghép Job 3 và Job 4 thành một job duy nhất, không cho chạy rời;
phép quét orphan bắt buộc có age guard 24h để không xoá file của writer đang ghi dở;
và thêm một metric theo dõi *bytes trên đĩa*, không chỉ *số snapshot*.

*(197 từ)*
