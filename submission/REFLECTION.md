# Reflection — Lab 18: Kiến Trúc Data Lakehouse

## Anti-pattern dễ mắc nhất: Bỏ qua tầng Silver (nhảy thẳng Bronze → Gold)

Trong số các anti-pattern được đề cập ở Slide §5, anti-pattern mà nhóm chúng
tôi dễ mắc nhất là **bỏ qua tầng Silver** — tức là đọc dữ liệu thô từ Bronze
rồi tổng hợp thẳng lên Gold mà không qua bước làm sạch và loại trùng.

Trong pipeline LLM observability, cám dỗ này rất thực tế: dữ liệu Bronze đã có
cấu trúc JSON tương đối rõ ràng, các trường như `model`, `latency_ms`, `status`
đều có vẻ sạch sẽ và đầy đủ. Áp lực phải ra dashboard nhanh cho stakeholder
càng khiến shortcut này trở nên hấp dẫn, đặc biệt ở giai đoạn đầu dự án khi
dữ liệu còn ít và lỗi chưa tích lũy đủ để lộ ra.

Hậu quả chỉ lộ ra ở cuối pipeline. Như NB4 đã minh chứng, Bronze chứa tới
**9.948 bản ghi trùng `request_id`** (do client retry khi mạng không ổn định).
Nếu bỏ Silver và tổng hợp thẳng, các chỉ số p50/p95 latency và `cost_usd`
trong Gold sẽ bị **thổi phồng âm thầm khoảng 5%** — một sai lệch không gây
exception hay cảnh báo nào, nhưng làm lệch toàn bộ quyết định kinh doanh dựa
trên đó, từ báo cáo chi phí đến SLA cam kết với khách hàng.

Kiến trúc medallion giải quyết điều này bằng cách buộc phải định nghĩa một
**ranh giới chất lượng tường minh** tại tầng Silver. Ranh giới đó vừa là nơi
loại bỏ dữ liệu xấu, vừa là điểm kiểm toán giúp trả lời câu hỏi "dữ liệu này
đến từ đâu và đã được xử lý như thế nào?" Không có Silver, câu hỏi đó không có
lời đáp.
