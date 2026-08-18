# Reflection: Top 5 Lakehouse Anti-Patterns

Trong số "Top 5 Lakehouse Anti-Patterns", anti-pattern mà team chúng tôi dễ vướng phải nhất là **"Bỏ bê việc bảo trì và dọn dẹp (Orphan files & Snapshot expiry)"**. 

**Lý do:**
1. **Thiếu hiển nhiên:** Không giống như lỗi query chậm (nhận ra ngay) hay dữ liệu sai (khách hàng báo), việc hệ thống lưu trữ ngày càng phình to do rác (orphan files) từ các job bị crash hoặc các uncommitted files là "lỗi im lặng". Hệ thống vẫn chạy bình thường nhưng chi phí lưu trữ (S3/MinIO) tăng dần.
2. **Hiểu lầm về công cụ:** Trong thực tế làm lab (NB6), chúng tôi nhận ra rằng lệnh `VACUUM` của Delta Lake không hề xóa các orphan files chưa từng được commit vào log, và `expire_snapshots` của Iceberg chỉ dọn dẹp metadata chứ không xóa file data thực tế. Việc ngộ nhận rằng gọi lệnh là hệ thống tự dọn dẹp 100% sẽ dẫn đến hóa đơn cloud tăng vọt mà không rõ nguyên nhân.

**Giải pháp:** Team cần phải đưa 4 job bảo trì (Compaction, Clustering, Expiry, và quét Orphan thực sự) thành các scheduled jobs bắt buộc ngay từ đầu, kết hợp monitor chi phí lưu trữ hàng tuần để phát hiện bất thường.
