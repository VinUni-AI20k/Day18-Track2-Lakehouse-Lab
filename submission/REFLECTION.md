# Phản tư: Nguy cơ Anti-Pattern trong Kiến trúc Data Lakehouse

Trong Top 5 Lakehouse Anti-Patterns, team chúng tôi có nguy cơ vướng phải **Anti-pattern #3: Dual-system drift & Vector DB Sync Skew (Lệch pha vòng đời giữa Lakehouse và Vector DB ngoại vi)** cao nhất.

### Lý do:
1. **Bất đồng bộ vòng đời (Lifecycle Skew):** Khi triển khai RAG/Multimodal AI, dữ liệu văn bản và embeddings thường index sang Vector DB ngoài (Pinecone/Milvus) để tìm kiếm nhanh. Khi Lakehouse xóa dữ liệu (tuân thủ GDPR/Nghị định 13), nếu thiếu Change Data Feed (CDF) đồng bộ tự động, Vector DB vẫn lưu và trả về dữ liệu đã xóa (ghost records), gây vi phạm bảo mật và sai lệch mô hình.
2. **Thiếu ranh giới ACID thống nhất:** Lakehouse có ACID log, nhưng Vector DB ngoài không chung atomic commit, dễ sinh race-condition khi cập nhật hàng loạt.

### Giải pháp khắc phục:
Nhúng vector trực tiếp vào bảng (In-table Vectors) hoặc dùng Delta CDF làm Single Source of Truth kích hoạt trigger xóa đồng bộ tức thời trên Vector DB.
