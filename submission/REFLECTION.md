# Phản hồi về các Anti-pattern trong Lakehouse

Dựa trên quá trình thực hiện Lab 18, em nhận thấy dự án của chúng em dễ vướng phải anti-pattern **"Small File Problem"** (Vấn đề file nhỏ) nhất.

**Lý do:**
Trong bài toán quan sát LLM (NB4), dữ liệu được thu thập liên tục từ hàng ngàn cuộc gọi API theo thời gian thực. Nếu mỗi cuộc gọi được ghi trực tiếp vào Lakehouse ngay lập tức dưới dạng một file riêng biệt mà không có cơ chế gom nhóm (compaction), hệ thống sẽ nhanh chóng tích tụ hàng triệu file nhỏ. 

Như đã thấy ở NB2, việc có quá nhiều file nhỏ khiến metadata của Delta Lake trở nên cồng kềnh, làm giảm đáng kể hiệu năng truy vấn do chi phí I/O tăng cao. Mặc dù Delta Lake hỗ trợ lệnh `OPTIMIZE`, nhưng nếu team không có quy trình vận hành tự động (Auto-compaction), rủi ro hệ thống bị chậm dần theo thời gian là rất lớn. Việc nắm vững kiến trúc Medallion và các lệnh tối ưu hóa là chìa khóa để chúng em tránh được tình trạng "vũng lầy dữ liệu" này.
