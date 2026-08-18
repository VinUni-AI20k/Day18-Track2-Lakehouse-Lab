# REFLECTION — Day 18 Lakehouse Lab

**Nguyễn Tuấn Khanh · 2A202601139**

## Anti-pattern dễ mắc nhất: Small-Files Problem (bỏ qua OPTIMIZE)

Nó **không đến từ code sai**. NB6 tái hiện một job streaming ingestion trigger 5
giây chạy qua đêm: 200 commit đều hợp lệ, kết quả 200 file trung bình **51.5 KB**
— xa mức 128–512 MB production. Thiếu một cron job, không phải thiếu năng lực.

Chi phí mới đáng sợ: bảng 500 GB / 2 triệu file tốn **$240/tháng cho thành phần
per-object** — 24% hoá đơn do *số lượng file*, không phải dung lượng.

## Giải pháp khắc phục

1. **Sửa nguồn:** tăng trigger interval (5s → 5–10 phút), bật auto-compaction.
2. **Lịch 4 job:** compaction hằng ngày (NB6 đo 18× ít file hơn), Z-ORDER trên cột
   filter nóng (bỏ 90% file), expiry ≥168h, orphan sweep.
3. **Nối Job 3 với Job 4.** `expire_snapshots` hạ 20 → 3 snapshot nhưng **xoá 0 file
   avro**: expiry chỉ làm file mất tham chiếu, xoá là việc của orphan sweep. Chạy
   riêng lẻ là lý do "đã expire mà hoá đơn S3 không giảm".
4. **Cảnh báo** khi file trung bình < 32 MB.
