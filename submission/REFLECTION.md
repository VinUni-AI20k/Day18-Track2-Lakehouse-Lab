# Reflection

Anti-pattern em dễ gặp nhất là **small-file explosion**. Dữ liệu quan sát LLM và agent đến liên tục từ nhiều model, phiên bản và tool nên thường được ghi theo micro-batch. Nếu mỗi batch tạo một file riêng, số file sẽ tăng nhanh hơn dung lượng dữ liệu, làm tăng chi phí liệt kê file, đọc metadata và lập kế hoạch truy vấn.

Kết quả lab cho thấy vấn đề này rõ ràng: NB2 tạo 200 file trước khi tối ưu; sau compaction và Z-Order còn 55 file, đạt speedup 10,5× và pruning 55×. Trong NB6, 200 commit tạo 200 file, tương đương khoảng 10 triệu GET mỗi ngày ở workload mô phỏng. Sau compaction, số file giảm còn 11; clustering giúp bỏ qua 90% file cho point query.

Em sẽ theo dõi số file và kích thước trung bình theo partition, đặt lịch compaction, chọn clustering key theo mẫu truy vấn, đồng thời tách snapshot expiry khỏi orphan cleanup để tránh hiểu nhầm rằng metadata đã dọn thì chi phí lưu trữ cũng đã giảm.
