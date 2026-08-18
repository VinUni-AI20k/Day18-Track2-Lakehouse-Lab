Team mình dễ vướng nhất anti-pattern **small files** (slide §6): streaming append từng batch nhỏ rồi quên OPTIMIZE / Z-ORDER.

Data gần với NB4 nhất — LLM observability ghi Bronze theo micro-batch. NB2 tái hiện đúng hình đó: 200 append tạo 200 file nhỏ; nếu không compact thì point query gần như đọc cả bảng, không đạt files-pruned ≥ 10×.

Đi kèm là giả định “đã VACUUM là sạch”. NB6 đo `VACUUM` của deltalake chỉ thu hồi file đã bị tombstone trong `_delta_log`. Job crash để orphan chưa commit — vô hình với vacuum, vẫn tốn disk. Iceberg `expire_snapshots` cũng chỉ đụng metadata (20 → 3 snapshot, 0 file avro bị xóa) nếu không chạy orphan sweep.

Pipeline team hay retry/crash và ít khi chạy compaction + expiry + orphan như một cụm, nên đây là anti-pattern dễ dính nhất trên data thật.
