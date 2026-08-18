# Reflection: Lakehouse Anti-Patterns

Trong việc theo dõi và lưu log gọi LLM (LLM Observability) của nhóm mình, bẫy kiến trúc dễ dính nhất chính là **Anti-pattern 3: Bỏ qua OPTIMIZE dẫn đến bài toán Small-files**.

Vì muốn ghi lại từng phản hồi của model theo dạng streaming gần như ngay lập tức (trigger mỗi 5 giây), tụi mình cho ghi thẳng dữ liệu xuống đĩa mà không tính tới hậu quả. Chỉ sau một đêm test ứng dụng, hệ thống đã sinh ra hàng chục nghìn file Parquet tí hon chỉ tầm vài chục KB. Đến lúc cần mở dashboard tổng hợp chi phí và độ trễ, câu lệnh truy vấn chạy mất gần nửa phút do engine phải mở và đọc hàng nghìn file lẻ tẻ. Không chỉ vậy, chi phí gửi yêu cầu GET lên S3 cũng sẽ tăng phi mã nếu đưa lên cloud.