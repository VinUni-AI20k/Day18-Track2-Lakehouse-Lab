# REFLECTION: Data Lakehouse Architecture — Day 18

**Họ và tên:** Đào Hồng Sơn
**MSSV:** 2A202600462

---

## Anti-pattern từ slide §5 mà team em có nguy cơ cao nhất

Trong 5 anti-patterns từ slide §5 (Top 5 Lakehouse Anti-Patterns), team em có nguy cơ cao nhất với **#1: "Đổ tất cả vào S3" (raw JSON, no schema)**.

### Tại sao nguy cơ cao?

1. **Context hiện tại:** Team đang xây LLM pipeline với prompt logs và inference responses — data dạng semi-structured JSON từ nhiều nguồn (OpenAI, Anthropic, local models).

2. **Áp lực "nhanh là trên hết":** Để demo sớm, team thường bypass schema enforcement, write trực tiếp raw JSON vào S3 với suy nghĩ "lát nữa mới clean".

3. **Scale effect:** Ở 10 GB → không vấn đề. Nhưng khi team mở rộng để handle batch inference 100K+ requests/ngày, data swamp xuất hiện: duplicate requests, missing fields, type inconsistency giữa các model versions.

4. **Hậu quả cụ thể:**
   - Bronze layer không có schema → Silver MERGE fails vì type mismatch
   - Gold aggregation sai vì một số rows có `latency_ms` là string, một số là float
   - Time travel restore về version cũ nhưng schema không tương thích

### Giải pháp đề xuất

- **Bắt buộc schema enforcement từ Bronze** (Delta Lake `enforceSchema` hoặc Iceberg `支柱` với required fields)
- **mergeSchema=true** cho backward compatibility khi thêm fields mới
- **Great Expectations validation** ở ingestion point để catch schema drift sớm

---

## Tổng kết

Anti-pattern #1 là "easy to slip into" nhất vì nó seductive: bypass schema = move fast ở prototype stage. Nhưng technical debt accumulate nhanh hơn team nhận ra. Prevention: enforce schema từ day 1, không matter bao nhiêu data.

---
