# REFLECTION — Top 5 Lakehouse Anti-Patterns

**Rủi ro lớn nhất với team tôi: sync một chiều sang index dẫn xuất (lifecycle skew).**

Team tôi làm RAG — corpus ở lakehouse, embedding sync đêm sang vector index. NB7 tái hiện đúng lỗi này: `user_042` xin xoá 8 doc, lakehouse trả **0 hit**, index cũ trả **8 hit**. Vì sync là one-way upsert, *delete* không có đường lan truyền: nội dung đã xoá tiếp tục vào prompt RAG vô thời hạn.

Tôi từng định chọn small-file: NB6 đo 200 file = $4/ngày chỉ tiền GET, và slide gọi đây là failure mode phổ biến nhất. Tôi loại nó vì small-file **tự bộc lộ**: query chậm dần, hoá đơn tăng, một cron compaction là xử lý được. Lifecycle skew thì **im lặng** — dashboard vẫn xanh — và không phải bug hiệu năng mà là bug **tuân thủ** (PDPL 91/2025, GDPR Art. 17).

Hành động: (1) cho index đăng ký **CDF** thay vì đoán — NB7 cho thấy delete event mang đúng `doc_id` cần evict; (2) tốt hơn: giữ vector **trong hàng**, để chính bảng cưỡng chế lifecycle; (3) coi retention là quyết định được viết ra, vì time travel giữ lại đúng thứ ta vừa cam kết xoá.
