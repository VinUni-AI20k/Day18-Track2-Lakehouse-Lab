# Reflection: Top Lakehouse Anti-Pattern

Trong các dự án AI/Data Platform, team chúng tôi có nguy cơ mắc phải **Anti-Pattern #3: Decoupled Vector DB Sync (Lệch vòng đời Vector)** nhất.

### Nguyên nhân & Rủi ro:
Khi phát triển hệ thống RAG, team thường lưu dữ liệu chính trên Lakehouse và đồng bộ embeddings sang Vector DB ngoài (Pinecone/Milvus). Do thiếu cơ chế đồng bộ transaction hai chiều, khi người dùng yêu cầu xóa dữ liệu (tuân thủ **Nghị định 13/2023/NĐ-CP** và **GDPR**), lệnh `DELETE` trên Delta/Iceberg xóa thành công nhưng Vector DB bên ngoài bị bỏ quên. Hệ quả là chatbot RAG vẫn truy hồi và sinh câu trả lời từ dữ liệu đã xóa, gây rò rỉ thông tin nhạy cảm.

### Giải pháp từ Lab 18:
Team chuyển sang lưu vector trực tiếp dưới dạng cột embedding (`fixed_size_list` / int8) ngay trong bảng Lakehouse, truy vấn qua DuckDB/Spark SQL. Kiến trúc này đảm bảo *Single Source of Truth*, đồng nhất vòng đời ACID và triệt tiêu hoàn toàn rủi ro lệch đồng bộ.
