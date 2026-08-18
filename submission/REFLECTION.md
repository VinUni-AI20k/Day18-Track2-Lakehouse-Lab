# Reflection — Small-Files Problem

Trong năm Lakehouse Anti-Patterns, team tôi dễ mắc **Small-Files Problem do bỏ qua OPTIMIZE** nhất. Pipeline thu thập log LLM liên tục theo micro-batch; nếu mỗi batch tạo một file Parquet và một commit, số file sẽ tăng nhanh dù dữ liệu chưa lớn. Engine phải mở nhiều file, xử lý nhiều metadata và chịu thêm chi phí request trên object storage. Min/max statistics cũng kém hữu ích, làm file skipping giảm hiệu quả.

NB2 cho thấy rõ vấn đề này: 200 lần append tạo 200 file nhỏ. Sau compaction và Z-order theo `user_id`, số file đang hoạt động giảm còn 55; truy vấn chỉ cần xét 1/55 file, đạt pruning ratio 55×.

Team sẽ tăng kích thước batch ghi, đặt target file size phù hợp và kích hoạt compaction khi số file hoặc kích thước trung bình vượt ngưỡng. Sau đó, dữ liệu được Z-order theo cột lọc phổ biến như `user_id` hoặc `model`. Dashboard sẽ cảnh báo khi file count tăng bất thường. VACUUM và orphan cleanup được chạy bằng job riêng với retention đã phê duyệt.
