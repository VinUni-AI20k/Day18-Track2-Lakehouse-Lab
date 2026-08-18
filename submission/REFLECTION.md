# Reflection: Lakehouse Anti-Pattern Risk

Trong 5 anti-pattern của Lakehouse, rủi ro lớn nhất với team chúng tôi là **Dual Source of Truth & Vector Lifecycle Desynchronization** (Lệch pha vòng đời giữa Lakehouse và Vector DB ngoại vi).

### Nguyên nhân và Rủi ro:
1. **Lệch pha xóa bỏ (Lifecycle Drift):** Khi thực thi GDPR/Nghị định 13 (Right-to-be-forgotten) trên Delta/Iceberg qua `DELETE`/`MERGE`, vector DB bên ngoài (Pinecone/Milvus) thường không nhận kịp CDC delete events hoặc gặp timeout, gây ra **Phantom Search Results** (truy vấn semantic vẫn trả về dữ liệu đã xóa trong Lakehouse).
2. **Mất tính tái lập (Non-reproducible Lineage):** Vector DB bên ngoài là mutable state liên tục, thiếu commit log snapshot. Khi audit mô hình hoặc debug RAG regression, team không thể time-travel về đúng vector index tại commit version $V_n$.

### Giải pháp:
Team chuyển sang **Lakehouse-native vector storage** (lưu embeddings `INT8`/`FLOAT` trực tiếp trong Parquet/Delta) và coi Vector DB bên ngoài chỉ là *ephemeral, rebuildable derived index* được đồng bộ tự động qua Delta Change Data Feed (CDF).
