# Reflection — Lakehouse Anti-Patterns

**Dễ vướng nhất: đưa bảng lên production mà không có job bảo trì — để
small-file tích tụ.**

NB6 tái hiện đúng điều đó: 200 micro-batch chứa 100.000 dòng trong 10,1 MB.
Full-scan tốn 10 triệu GET/ngày, **4,00 $/ngày chỉ riêng request**, so với
0,08 $ sau compaction. Compaction rút 200 → 11 file (18×); clustering bỏ qua
được **90%** số file khi truy vấn điểm.

Kết quả làm tôi đổi ý đi ngược lời khuyên thường gặp. Tôi tưởng `VACUUM` lo
việc dọn dẹp. Không phải — delta-rs chỉ thu hồi file đã tombstone trong log,
nên 5 file parquet vẫn trên đĩa (15 thật, 10 trong log): vẫn tính tiền, vô
hình ở mọi mức retention. Iceberg tệ hơn: `expire_snapshots` đưa 20 → 3
snapshot, xoá **0** file avro, metadata còn phình 328,4 → 335,7 KB. Chỉ khi
nối expiry sang orphan sweep mới thu hồi được gì (avro 40 → 23).

Job 3 và Job 4 là một cặp. Team tôi dựng pipeline streaming sẽ vướng đúng chỗ
này: có expiry mà thiếu sweep — đúng lý do người ta nói "đã expire mà hoá đơn
không giảm".
