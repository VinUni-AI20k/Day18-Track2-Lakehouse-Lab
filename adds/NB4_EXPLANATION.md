# NB4 — Medallion Pipeline (Bronze → Silver → Gold): Giải thích chi tiết

## Notebook này làm về cái gì?

NB4 xây dựng **Medallion Architecture** — kiến trúc 3 tầng chuẩn của Data Lakehouse:

```
Bronze (raw) → Silver (cleaned) → Gold (aggregated)
```

Use-case thực tế: **LLM Observability** — theo dõi latency, cost, error rate
của các model AI (Claude Haiku/Sonnet/Opus) theo ngày. Đây là bài toán mà mọi
team vận hành LLM đều cần.

---

## Giải thích từng phần

### Cell 1 — Setup & Paths

```python
BRONZE = path("bronze", "llm_calls_raw")      # _lakehouse/bronze/llm_calls_raw/
SILVER = path("silver", "llm_calls")           # _lakehouse/silver/llm_calls/
GOLD   = path("gold",   "llm_daily_metrics")   # _lakehouse/gold/llm_daily_metrics/
```

Ba tầng Medallion, mỗi tầng là một Delta table riêng biệt.

---

### Cell 3 — Bronze: Verify raw data

```python
bronze_n = DeltaTable(BRONZE).to_pyarrow_table().num_rows
# → Bronze rows: 200,000
```

**Output:**
```
Bronze rows: 200,000
┌─────────────────┬─────────────────────────┬────────────────────┐
│ request_id      │ ts                      │ raw_json           │
│ str             │ datetime[μs, UTC]       │ str                │
│ a5ddd65c-...    │ 2026-04-01 00:00:00 UTC │ {"model":"claude.. │
└─────────────────┴─────────────────────────┴────────────────────┘
```

**Ý nghĩa:**
- Bronze = **raw data** từ `make data` (200K rows).
- Schema rất đơn giản: `request_id`, `ts` (timestamp), `raw_json` (JSON string chứa toàn bộ payload).
- **Không parse, không clean** — ghi nguyên trạng, đảm bảo không mất data.

> **Bronze trong thực tế:** Kafka/Kinesis stream → append trực tiếp vào Bronze.
> Dữ liệu thô, có thể có duplicate, schema lỏng. Mục tiêu: "land everything first".

---

### Cell 5 — Silver: Parse, Validate, Dedup

```sql
WITH parsed AS (
  SELECT
    request_id, ts,
    CAST(ts AS DATE) AS date,
    json_extract_string(raw_json, '$.model')     AS model,
    json_extract_string(raw_json, '$.user_id')   AS user_id,
    CAST(json_extract(raw_json, '$.usage.input')  AS INTEGER) AS prompt_tokens,
    CAST(json_extract(raw_json, '$.usage.output') AS INTEGER) AS completion_tokens,
    CAST(json_extract(raw_json, '$.latency_ms')   AS INTEGER) AS latency_ms,
    json_extract_string(raw_json, '$.status')     AS status,
    ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts) AS rn
  FROM delta_scan(BRONZE)
)
SELECT ... FROM parsed WHERE rn = 1 AND model IS NOT NULL
```

**Output:** `Silver rows: 190,052 (Bronze 200,000 → dedup dropped 9,948)`

**Ba bước chính:**

1. **Parse JSON** — extract các field từ `raw_json` thành cột typed:
   - `model` (string), `latency_ms` (int), `prompt_tokens` (int), `status` (string)...

2. **Dedup** — `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)`:
   - Giữ row **đầu tiên** cho mỗi `request_id` (loại bỏ retry/duplicate).
   - 200K → 190K = **9,948 duplicates bị loại**.

3. **Validate** — `WHERE model IS NOT NULL`:
   - Loại bỏ rows không parse được (malformed JSON).

**Partition by date:**
```python
write_deltalake(SILVER, silver_arrow, mode="overwrite", partition_by=["date"])
```
- Data được tổ chức theo ngày trên disk → query filter by date rất nhanh.

> **Silver trong thực tế:** Clean, typed, deduplicated. Đây là tầng mà
> analyst/ML engineer có thể query trực tiếp. Schema rõ ràng, data quality đảm bảo.

---

### Cell 7 — Gold: Aggregate metrics theo (date × model)

```sql
SELECT
  s.date, s.model,
  QUANTILE_CONT(s.latency_ms, 0.50) AS p50_latency_ms,   -- median latency
  QUANTILE_CONT(s.latency_ms, 0.95) AS p95_latency_ms,   -- 95th percentile
  SUM(s.prompt_tokens)              AS total_prompt_tokens,
  SUM(s.completion_tokens)          AS total_completion_tokens,
  AVG(CASE WHEN s.status <> 'ok' THEN 1.0 ELSE 0.0 END) AS error_rate,
  (SUM(prompt_tokens) * c_in / 1e6) +
  (SUM(completion_tokens) * c_out / 1e6) AS cost_usd
FROM delta_scan(SILVER) s
JOIN cost c USING (model)
GROUP BY s.date, s.model
```

**Cost model (minh hoạ):**
| Model | Input ($/1M tokens) | Output ($/1M tokens) |
|---|---|---|
| claude-haiku-4-5 | $0.80 | $4.00 |
| claude-sonnet-4-6 | $3.00 | $15.00 |
| claude-opus-4-7 | $15.00 | $75.00 |

**Các metric Gold tính:**
- **p50_latency_ms** — median latency (50% requests nhanh hơn giá trị này).
- **p95_latency_ms** — 95th percentile (chỉ 5% requests chậm hơn → SLA metric).
- **error_rate** — tỷ lệ request lỗi (status ≠ 'ok').
- **cost_usd** — chi phí USD dựa trên token usage × pricing.

**Sau khi ghi Gold:**
```python
DeltaTable(GOLD).optimize.z_order(["model"])
```
- Z-order theo `model` → dashboard filter by model sẽ nhanh.

> **Gold trong thực tế:** Bảng aggregate cho dashboard/BI. Cấp quản lý xem
> cost trend, SRE xem p95 latency, PM xem error rate — tất cả từ 1 Gold table.

---

### Cell 9 — Verify Gold

**Output:**
```
shape: (24, 8)  ← 8 dates × 3 models = 24 rows

──── Gold deliverable metrics ────
  Distinct dates:     8   (target ≥ 7)  ✅
  Distinct models:    3                  ✅
  Total Gold rows:   24   (= dates × models)
```

**Chi tiết data mẫu:**
| date | model | p50 (ms) | p95 (ms) | error_rate | cost_usd |
|---|---|---|---|---|---|
| 2026-04-01 | claude-haiku-4-5 | 567 | 1121 | 4.9% | $33.18 |
| 2026-04-01 | claude-opus-4-7 | 3045 | 6036 | 5.0% | $200.25 |
| 2026-04-01 | claude-sonnet-4-6 | 1381 | 2752 | 5.0% | $244.15 |

**Nhận xét từ data:**
- **Haiku** nhanh nhất (p50 ~560ms) và rẻ nhất (~$33/ngày).
- **Opus** chậm nhất (p50 ~3000ms) nhưng đắt nhất (~$200–290/ngày).
- **Sonnet** ở giữa, cost cao nhất tổng vì lượng request lớn.
- Error rate ~5% đều giữa 3 models.

---

## Kiến trúc Medallion tổng quan

```
┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐
│     BRONZE       │     │     SILVER       │     │      GOLD        │
│  200,000 rows    │────→│  190,052 rows    │────→│    24 rows       │
│                  │     │                  │     │                  │
│  raw_json (str)  │     │  model (str)     │     │  date × model    │
│  request_id      │     │  latency_ms (int)│     │  p50, p95        │
│  ts              │     │  prompt_tokens   │     │  cost_usd        │
│                  │     │  status          │     │  error_rate      │
│  No cleaning     │     │  Dedup + typed   │     │  Aggregated      │
└──────────────────┘     └──────────────────┘     └──────────────────┘
    "Land all"              "Clean & enrich"         "Business-ready"
```

**Tại sao 3 tầng?**
- **Bronze**: không mất data, có thể reprocess lại Silver bất cứ lúc nào.
- **Silver**: analyst query được, schema rõ ràng, data quality đảm bảo.
- **Gold**: dashboard/BI chỉ cần đọc 24 rows thay vì scan 200K → cực nhanh.

---

## Tổng kết — NB4 dạy gì?

| # | Khái niệm | Cell | Một câu tóm tắt |
|---|---|---|---|
| 1 | **Bronze (raw)** | 3 | Data thô 200K rows, chưa clean, chưa parse |
| 2 | **Silver (clean)** | 5 | Parse JSON + dedup + validate → 190K rows |
| 3 | **Gold (aggregate)** | 7 | Aggregate (date × model) → 24 rows với p50/p95/cost/error |
| 4 | **Partition by** | 5,7 | Tổ chức data theo date trên disk → query filter nhanh |
| 5 | **Z-order Gold** | 7 | Z-order theo model → dashboard filter nhanh |
| 6 | **Cost model** | 7 | Tính cost USD từ token usage × pricing table |

---

## Checklist NB4 — Đã pass ✅

- [x] Ba table tồn tại: `_lakehouse/{bronze,silver,gold}/` ✅
- [x] **Silver < Bronze**: 190,052 < 200,000 (dedup loại 9,948 rows) ✅
- [x] **Gold ≥ 7 dates × 3 models**: 8 dates × 3 models = **24 rows** ✅
- [x] `cost_usd` và `error_rate` đều populated và non-zero ✅
