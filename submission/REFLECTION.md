# Thu hoạch: Phân tích Lakehouse Anti-Pattern trong thực tế

Trong kiến trúc dữ liệu của team, anti-pattern có rủi ro cao nhất là **Stale External Vector Index (Lệch vòng đời dữ liệu)**.

### Lý do:
Khi xây dựng hệ thống GenAI/RAG, team thường đồng bộ bảng Lakehouse sang một Vector DB bên ngoài (như Pinecone/Qdrant) qua cronjob hoặc CDC một chiều. Việc thêm và cập nhật dữ liệu diễn ra trơn tru, nhưng thao tác `DELETE` và yêu cầu quyền được lãng quên (Luật Dữ liệu cá nhân 91/2025 / GDPR) rất dễ bị bỏ sót. Như minh chứng trong NB7, khi Lakehouse xóa dữ liệu người dùng, Vector DB bên ngoài vẫn tiếp tục phục vụ các vector bị xóa cho các truy vấn LLM tiếp theo, dẫn đến vi phạm pháp lý nghiêm trọng.

### Giải pháp khắc phục:
1. Đối với tác vụ phân tích/đo lường offline: Lưu embedding trực tiếp trong bảng Lakehouse (sử dụng int8 quantization và DuckDB cosine similarity).
2. Đối với serving online: Bật Delta Change Data Feed (CDF) để lắng nghe trực tiếp sự kiện `DELETE` và tự động thu hồi vector tương ứng trên Vector DB.