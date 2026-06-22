# Reflection — Top 5 Lakehouse Anti-Patterns

**Lê Hoàng Nam — 2A202600965**

Anti-pattern nhóm tôi dễ gặp nhất là **small-file explosion**. Dữ liệu LLM observability thường được đẩy liên tục theo các micro-batch nhỏ; nếu mỗi batch tạo một file, số file tăng nhanh hơn nhiều so với dung lượng dữ liệu. Notebook 02 cho thấy hậu quả khá rõ: trước tối ưu có 200 file và truy vấn điểm mất 512,7 ms.

Rủi ro này dễ bị bỏ qua vì pipeline vẫn ghi dữ liệu thành công, nhưng metadata, listing và file-open overhead sẽ dần làm truy vấn chậm và tăng chi phí compute. Cách phòng tránh của tôi là theo dõi số file, kích thước trung vị và tỷ lệ file được đọc; đặt lịch compaction theo ngưỡng thay vì chạy tùy ý; đồng thời Z-order theo khóa truy vấn thực sự phổ biến. Trong bài lab, OPTIMIZE và Z-order giảm còn 55 file, đạt speedup 24,8× và files-pruned ratio 55×. Kết quả này nhắc tôi rằng tối ưu layout phải được xem là một phần vận hành của pipeline, không phải công việc dọn dẹp sau cùng.
