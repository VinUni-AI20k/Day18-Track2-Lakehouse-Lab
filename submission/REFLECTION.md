# Suy ngẫm — Top 5 Lakehouse Anti-Patterns

**Rủi ro lớn nhất của team: small-file problem — cụ thể hơn, coi maintenance là
việc tuỳ chọn thay vì hạ tầng có lịch chạy.**

Pipeline ingest của team có dạng streaming: nhiều commit nhỏ và hoàn toàn đúng.
NB6 cho thấy sau một đêm nó tích tụ thành gì — 200 commit, file trung bình
51.5 KB, một full-scan tốn 10 triệu GET/ngày (~$4/ngày chỉ tiền request) mà
compaction kéo về còn 4 file. Không dòng code nào sai; cái sai là thiếu một
cron job.

Điều làm tôi đổi suy nghĩ là nửa sau NB6. Tôi từng nghĩ `VACUUM` và
`expire_snapshots` là dọn dẹp. Cả hai đều không: vacuum tìm được 0/3 file rác đã
tạo (chưa bị tombstone nên log không thấy); Iceberg expiry giảm 20 → 3 snapshot
mà xoá **0** file avro, metadata còn phình to. Expiry chỉ đánh dấu file hết được
tham chiếu; xoá là một job khác.

Vậy rủi ro không nằm ở chỗ không biết anti-pattern — mà ở giả định một lệnh là
đủ. Hướng xử lý: lên lịch cả bốn job cùng nhau, và cảnh báo theo *số file trên
mỗi GB*, không chỉ dung lượng bảng.
