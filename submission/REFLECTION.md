# Reflection

Trong "Top 5 Lakehouse Anti-Patterns", anti-pattern mà team dễ vướng phải nhất trong thực tế là **"expire_snapshots của Iceberg không thực sự giải phóng storage"** (cũng như việc lệnh `VACUUM` bỏ sót các uncommitted orphans).

**Tại sao?**
Khi vận hành hệ thống Lakehouse, Data Engineer thường có thói quen thiết lập lịch chạy các API bảo trì (như `expire_snapshots`) và lầm tưởng rằng hệ thống sẽ tự động dọn dẹp toàn bộ rác vật lý (data files) giống như cơ sở dữ liệu truyền thống. 

Tuy nhiên, như đã kiểm chứng qua notebook 06, lệnh expiry của Iceberg thực chất chỉ dọn dẹp *metadata*, trong khi **0 file data nào thực sự bị xoá đi**. Nếu chỉ chạy expiry mà không kết hợp quét các orphan files (stranded manifest lists), hoá đơn lưu trữ trên S3/GCS vẫn sẽ tiếp tục phình to mỗi ngày dù tiến trình bảo trì báo cáo "chạy thành công". 

Việc thiếu hiểu biết về cơ chế "rác vô hình" (invisible garbage) này sẽ dẫn đến những lỗ hổng chi phí (FinOps) rất khó phát hiện ở môi trường production. Do đó, bài học rút ra là luôn phải chạy các job dọn dẹp theo cặp để xử lý triệt để cả metadata lẫn data files.
