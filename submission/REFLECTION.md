# Reflection — Day 18 Lakehouse Lab

**Họ và tên:** Võ Thiên Phú  
**MSSV:** 2A202600336

---

## Anti-Pattern from Slide §5 My Team Would Be Most At Risk Of

**#3 — Bỏ qua OPTIMIZE → Small-File Problem**

## Why This Anti-Pattern

Trong LLM observability pipeline, mỗi API call được ghi như một batch nhỏ. Với hàng triệu LLM requests mỗi ngày, điều này tạo ra hàng ngàn small files trong vài ngày — đúng như NB2 đã mô phỏng.

**Hậu quả thực tế:**
- Query latency tăng dần (10× chậm khi có 10K files)
- File pruning không hiệu quả, đọc nhiều data không cần thiết
- Storage metadata overhead tăng

**Tại sao team dễ mắc phải:**
1. Không gây lỗi ngay — pipeline vẫn chạy được, chỉ chậm dần. Khó nhận ra cho đến khi dashboards trễ vàng.
2. Không có "quiet window" tự nhiên để chạy OPTIMIZE — LLM calls ghi liên tục 24/7.

**Giải pháp đã học:**
- `dt.optimize.compact()` + `dt.optimize.z_order(["user_id"])` — giảm 200 files → ~50 files
- Schedule: nightly cron hoặc sau mỗi N batches

Lab này cho thấy OPTIMIZE không phải optional — trong LLM production, nó là **required maintenance** để giữ economics của lakehouse.
