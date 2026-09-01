# Reflection: Lakehouse Anti-Patterns

Trong các anti-patterns của Lakehouse, đội ngũ của chúng tôi dễ mắc phải lỗi **"Ignoring File Management (Vấn đề nhiều file nhỏ - Small Files Problem)"** nhất.

**Lý do:**
Hệ thống giám sát LLM (LLM observability) nhận dữ liệu từ các API call liên tục theo thời gian thực hoặc micro-batching. Việc append liên tục các lô dữ liệu nhỏ vào Bronze layer sẽ nhanh chóng tạo ra hàng nghìn file Parquet kích thước cực nhỏ (vài KB đến vài MB). Nếu không cấu hình tự động bảo trì bảng bằng các lệnh `OPTIMIZE` (compaction) và `VACUUM` định kỳ, hiệu năng đọc tại tầng Silver/Gold sẽ suy giảm nghiêm trọng do công cụ truy vấn phải quét qua quá nhiều file nhỏ, đồng thời làm tăng chi phí lưu trữ đám mây do overhead của metadata. Việc tự động hóa compaction là bắt buộc để duy trì SLA truy vấn.
