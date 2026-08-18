# Reflection — Lab 18 Lakehouse Architecture

**Nguyễn Hùng Mạnh — AICB-P2T2 · Day 18**

## Anti-Pattern Dễ Vướng Nhất: "Stale External Vector Index"

Trong "Top 5 Lakehouse Anti-Patterns", anti-pattern **stale external vector index** là mối nguy hiểm trực tiếp nhất với dự án AI đang triển khai.

Lab NB7 đã tái hiện vấn đề này: khi xóa một document khỏi Delta table (bằng `DELETE` hoặc `VACUUM`), bản ghi biến mất khỏi bảng chính — nhưng vector embedding tương ứng **vẫn còn nguyên trong FAISS/pgvector/Pinecone** bên ngoài. RAG pipeline tiếp tục trả về kết quả từ dữ liệu "đã xóa" này, dẫn đến:

- **Rò rỉ thông tin:** dữ liệu nhạy cảm đã được yêu cầu xóa vẫn xuất hiện trong câu trả lời của AI.
- **Vi phạm quy định:** với dữ liệu cá nhân (Nghị định 13/2023, GDPR), đây là nguy cơ pháp lý nghiêm trọng.
- **Sai lệch kết quả:** retrieval trả về context lỗi thời, model đưa ra câu trả lời sai dù database đã cập nhật.

**Lý do dễ vướng:** Khi team tập trung vào chất lượng embedding và recall@K, việc đồng bộ vòng đời dữ liệu giữa lakehouse và vector index thường bị bỏ qua cho đến khi có sự cố thực tế.

**Giải pháp:** Dùng Delta CDF (Change Data Feed) để streaming propagate deletes xuống index — giống kiến trúc NB8 dùng Delta version pinning cho training reproducibility.
