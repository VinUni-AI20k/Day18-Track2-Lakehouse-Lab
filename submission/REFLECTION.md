# Reflection: Lakehouse Anti-Patterns in Natural Language Robot Control

Trong dự án điều khiển robot bằng ngôn ngữ tự nhiên, hệ thống của chúng tôi có nguy cơ cao nhất gặp phải anti-pattern **Small-file problem** (Vấn đề tích tụ nhiều file nhỏ).

**Lý do:**
1. **Dữ liệu dạng streaming thời gian thực:** Các lệnh điều khiển bằng giọng nói/văn bản từ người dùng, dữ liệu phản hồi từ cảm biến robot, và nhật ký cuộc gọi API LLM (prompts, trajectories, latency) được ghi liên tục với tần suất rất cao.
2. **Kích thước payload nhỏ:** Mỗi tương tác hoặc phản hồi trạng thái của robot chỉ nặng từ vài KB đến vài chục KB. Việc append liên tục các file nhỏ này lên storage layer mà không có lịch trình chạy `OPTIMIZE/compaction` tự động sẽ làm bùng nổ số lượng file.

Hậu quả là hệ thống sẽ bị thắt nút cổ chai khi đọc metadata của transaction log, trực tiếp làm chậm các tác vụ phân tích hành vi và tối ưu hóa mô hình điều khiển robot.
