# Reflection — Top 5 Lakehouse Anti-Patterns

**Học viên:** Nguyễn Quang Tường · **Lab:** Track 2, Ngày 18

Dữ liệu team em dễ vướng nhất **anti-pattern small files: ingest mà không có cron
maintenance.** Team stream log LLM bằng trigger ngắn — đúng hình dạng NB6 tái
hiện: 200 commit đều đúng, file trung bình 51.5 KB, production nhắm 128–512 MB.
Không ai viết sai code; chính sự *tích luỹ* là bug. Ở 50K full scan/ngày: 10M GET
= **$4.00/ngày chỉ tiền request**, so với $0.08 sau compaction. NB6 còn cho thấy
**24% hoá đơn managed compaction do số lượng file quyết định, không phải dung
lượng** — thuê ngoài dọn dẹp là trả tiền cho trigger interval của mình.

Cách team sai còn tinh vi hơn, và NB6 đo được: team đã lên lịch expiry, tưởng thế
là xong. `expire_snapshots` giảm 20 snapshot còn 3 nhưng xoá **0 file avro**;
metadata còn *phình* 336.1 → 343.8 KB. `VACUUM` cũng không thấy 3 orphan đã cắm,
vì file chưa từng commit thì chưa từng bị tombstone. Expiry chỉ làm file mất tham
chiếu; xoá là việc khác. Chạy Job 3 mà thiếu Job 4 chính là lý do "đã expire mà
hoá đơn S3 không giảm".

**Khắc phục:** compaction + sweep gộp một job, cảnh báo theo file/GB.
