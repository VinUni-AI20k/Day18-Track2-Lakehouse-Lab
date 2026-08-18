# REFLECTION — Day 18 Lakehouse Lab

**Họ tên:** Bùi Gia Huy | **MSV:** 2A202601879 | **Track:** 2 — Lakehouse

**Anti-pattern team mình dễ vướng nhất:** *#4 — "Skip compaction, expect the
engine to cope"* (Top 5 Lakehouse Anti-Patterns, slide §6).

**Vì sao:** team mình đang vận hành một pipeline Kafka → Delta ingest
mỗi 5 giây, mỗi commit tạo 1 file parquet ~50–200 KB. Qua 6 tháng chỉ có
3 table "nóng", đã thấy ~3–4 triệu file nhỏ; dashboard BI bắt đầu phải
đợi 8–12 giây cho truy vấn point-lookup `user_id`, và hoá đơn S3 GET đã
tăng 40% chỉ trong Q2. Không ai đặt cron `OPTIMIZE` vì "compaction chạy
nền không ai thấy" — đúng nghĩa *invisible until it isn't*: metric S3 GET
không có dashboard, metric latency bị đổ cho Presto/Trino. Lab này đo
chính xác cơ chế NB6 chỉ ra (compaction ≥10× ít file, request cost giảm
tương ứng) và dựng test canary (`test_vacuum_does_not_see_uncommitted_orphans`)
để lần sau ai "quên sweep" thì CI đỏ trước khi hoá đơn S3 đỏ.

(~195 từ)