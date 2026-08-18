# Reflection — Lakehouse Anti-Pattern

Anti-pattern team tôi dễ gặp nhất là bỏ qua compaction sau streaming ingestion. Mỗi micro-batch đều hợp lệ, nhưng hàng nghìn file nhỏ tích lũy sẽ làm metadata phình to, tăng số GET trên object storage và khiến truy vấn chậm không tuyến tính. Nguy hiểm ở chỗ dashboard vẫn chạy đúng nên vấn đề thường chỉ lộ ra khi quy mô tăng mạnh.

Giải pháp là coi maintenance như một phần bắt buộc của pipeline: theo dõi số file, kích thước trung bình và tỷ lệ metadata/data; chạy compaction theo ngưỡng thay vì lịch cố định; clustering theo khóa truy vấn nóng; checkpoint transaction log; đồng thời ghép snapshot expiry với orphan sweeping. Cần giữ retention đủ dài để bảo vệ reader và khả năng rollback, không dùng `VACUUM RETAIN 0` trong production. Ngoài ra, writer phải được điều chỉnh trigger interval để giảm small files ngay từ nguồn, vì phòng ngừa luôn rẻ hơn trả chi phí compaction liên tục.
