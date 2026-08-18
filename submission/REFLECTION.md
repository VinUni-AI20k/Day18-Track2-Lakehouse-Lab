# Lakehouse Reflection

> **Requirement:** ≤ 200 words. Answer: *Which anti-pattern from the "Top 5 Lakehouse Anti-Patterns" slide is your team's data most at risk of, and why?*

## Selected Anti-Pattern
**Lệch nhịp lifecycle giữa Lakehouse bảng chính và External Vector Index (Stale Vector Index & Right-to-Erasure Violation).**

## Context & Risk Analysis
Trong kiến trúc GenAI/RAG của team, vector embeddings thường được sinh từ dữ liệu thô và đồng bộ sang một Vector Database độc lập (như Pinecone/Milvus) qua pipeline batch một chiều (upsert-only).

Rủi ro lớn nhất xảy ra khi người dùng yêu cầu xóa dữ liệu (Right to Erasure theo GDPR / Nghị định 13) hoặc khi tài liệu nguồn bị chỉnh sửa. Thao tác `DELETE` được thực thi hoàn tất trong Lakehouse (System-of-Record), nhưng external vector DB hoàn toàn không nhận được tín hiệu xóa. Hậu quả là vector index ngoài vẫn trả về các embedding "ma" đã bị xóa vào context của LLM prompt, gây vi phạm pháp lý và sai lệch thông tin (hallucination).

**Giải pháp áp dụng từ Day 18:**
1. Kích hoạt **Delta Change Data Feed (CDF)** để pipeline đồng bộ đăng ký (subscribe) các sự kiện `_change_type = 'delete'`, tự động evict vector khỏi index ngoài.
2. Với quy mô dữ liệu phù hợp, lưu trữ vector trực tiếp trong bảng Lakehouse và truy vấn bằng DuckDB để bảng tự bảo đảm tính toàn vẹn vòng đời (Lifecycle-enforced).
