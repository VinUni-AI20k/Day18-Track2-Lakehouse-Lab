# Bonus Challenge — LLM Observability Lakehouse tại 1B Requests/Ngày

**Topic A** · Architect on-call: Dương Quang Khải

---

## 1. Problem Statement

Một foundation-model API team cần log và phân tích toàn bộ traffic: **1 tỷ request/ngày, ~5 KB/request → 5 TB raw JSON/ngày**. Bốn constraint cứng:

1. **Dashboard** cost & latency theo tenant refresh mỗi **5 phút**
2. **Prompt/response đầy đủ** giữ **7 ngày** (incident review), sau đó chỉ giữ aggregates **1 năm**
3. **PII** (user_id, prompt content có tên người) phải được redact trước khi bất kỳ engineer nào đọc
4. **Tổng chi phí storage ≤ $5K/tháng**

Độ khó nằm ở chỗ ba mục tiêu mâu thuẫn nhau: (a) 5-phút freshness đòi streaming, (b) tiết kiệm $5K/tháng đòi batch-heavy tiering, (c) PII redact *trước khi lưu* đòi in-flight transformation — không phải post-hoc. Scale 1B req/ngày là 11.6K req/giây trung bình, peak 3× = 35K req/giây, không thể dùng single-node solution nào.

---

## 2. Architecture Diagram

```
╔══════════════════════════════════════════════════════════════════════════════╗
║  INGESTION LAYER                                                             ║
║                                                                              ║
║  API Pods (k8s) ──▶ Kafka (1-hour retention)  ──▶ Spark Structured          ║
║  35K req/s peak     topic: llm-calls-raw            Streaming (30s batch)   ║
║                     3 partitions × broker           PII tokenize in-flight  ║
╚══════════════════════════╦═══════════════════════════════════════════════════╝
                           ║ write ~1 TB/day compressed
           ┌───────────────▼─────────────────────────────────┐
           │  BRONZE  (Delta Lake · S3 Standard · 7-day TTL) │
           │  s3://bucket/bronze/llm_calls_raw/              │
           │  partition_by: [date, hour]                     │
           │  schema: request_id, ts, tenant_id,             │
           │          model, tokenized_user_id,              │
           │          raw_json (PII-free after tokenization) │
           │  Z-ORDER: tenant_id                             │
           │  ~1 TB/day · lifecycle → DELETE after 7 days   │
           └───────────────┬─────────────────────────────────┘
                           │ Spark batch every 30 min (dedup + parse)
           ┌───────────────▼─────────────────────────────────┐
           │  SILVER  (Delta Lake · S3 IA after 7d · 7-day) │
           │  s3://bucket/silver/llm_calls/                  │
           │  partition_by: [date, tenant_id]               │
           │  schema: request_id, ts, date, model,           │
           │          prompt_tokens, completion_tokens,      │
           │          latency_ms, status, cost_usd_est       │
           │  Z-ORDER: [model, status]                       │
           │  ~0.7 TB/day · lifecycle → DELETE after 7 days  │
           └───────────────┬─────────────────────────────────┘
                           │ Spark micro-agg every 5 min → MERGE into Gold
           ┌───────────────▼─────────────────────────────────┐
           │  GOLD  (Delta Lake · S3 Standard · 1 year)     │
           │  s3://bucket/gold/llm_daily_metrics/            │
           │  partition_by: [date]                           │
           │  schema: date, tenant_id, model,                │
           │          p50_ms, p95_ms, total_tokens,          │
           │          cost_usd, error_rate, request_count    │
           │  ~18 GB/year total                              │
           │  MERGE upsert mỗi 5 phút (partial-day rows)    │
           └───────────────┬─────────────────────────────────┘
                           │
           ┌───────────────▼─────────────────────────────────┐
           │  QUERY PATH                                      │
           │  DuckDB (dashboard) ──▶ Gold scan (< 2s p95)   │
           │  Trino on EKS (ad-hoc) ──▶ Silver scan          │
           │  AWS Glue Data Catalog (single schema source)   │
           └─────────────────────────────────────────────────┘
```

---

## 3. Quyết Định Chính Kèm Alternatives Đã Loại

### D1 — Table Format: Delta Lake

**Chọn Delta Lake (delta-rs + Spark).**

Loại **Apache Iceberg**: REST catalog overhead thêm một network hop cho mỗi commit; branching tốt hơn nhưng không cần thiết ở đây vì chúng ta không có multi-writer branching workflow. Delta UniForm có thể expose Iceberg metadata nếu cần interop sau này — đây là escape hatch, không phải blocker.

Loại **Apache Hudi**: Merge-on-Read (MOR) phù hợp cho CDC workload có nhiều update; workload này là append-heavy (log events không bao giờ update sau khi write). MOR thêm compaction overhead không cần thiết. COW mode của Hudi không có lợi thế so với Delta ở đây.

### D2 — Partitioning Strategy

**Chọn `[date, hour]` tại Bronze; `[date, tenant_id]` tại Silver.**

Loại `[date, tenant_id]` tại Bronze: Nếu có 100K tenants × 365 ngày = 36.5M partitions sau 1 năm. S3 listing performance sụp đổ, Glue catalog explode. Bronze chỉ cần partition cho lifecycle deletion (by date) và time-based pruning (by hour).

Loại `[date]` only tại Silver: không cho phép Trino prune theo tenant khi ad-hoc query "show me tenant X's data for last week." `tenant_id` ở Silver partition đủ nhỏ vì chỉ giữ 7 ngày và số tenants trong Silver partition là controllable (top-N active tenants, long tail bucket vào `__other__`).

### D3 — PII Tokenization: Tại Bronze Ingest

**Chọn tokenize (format-preserving encryption với AES-FF1) tại Spark Structured Streaming job, trước khi ghi Bronze.**

Loại "redact tại query time" (column masking): PII vẫn tồn tại trên disk ở dạng plaintext. Bất kỳ ai có S3 read access đều thấy dữ liệu thô. Không đáp ứng yêu cầu "PII redact trước khi bất kỳ ai đọc."

Loại "encrypt tại Silver": PII spend 30 phút ở Bronze dưới dạng plaintext giữa Kafka và Silver batch job. Nếu Bronze S3 bucket bị misconfigure (public read, hay IAM leak), PII lộ. Defense-in-depth đòi hỏi PII không bao giờ touch disk ở dạng raw.

Loại "delete/mask tại Bronze post-hoc": race condition — nếu có query vào Bronze trong 30 giây sau khi write nhưng trước khi mask job chạy, PII đã exposed.

### D4 — Streaming vs Micro-batch cho Bronze

**Chọn Spark Structured Streaming với 30-giây trigger interval.**

Loại "record-at-a-time streaming write vào Delta" (Flink per-event): 35K req/giây → 35K Delta commits/giây → _delta_log/ có 35K JSON files/giây. Transaction log explosion trong vài giờ. Delta khuyến cáo tối thiểu 1-phút batch để tránh small-file problem.

Loại "hourly Spark batch": Lag 60 phút trước khi data vào Bronze, sau đó thêm 30 phút Silver, tổng lag ~90 phút. Vi phạm SLA 5-phút dashboard freshness.

30-giây trigger: ~2.1M rows/batch → file size ổn (~50 MB/file), đủ nhỏ để OPTIMIZE weekly không tốn kém.

### D5 — Gold Refresh: Scheduled MERGE thay vì Streaming Aggregation

**Chọn Spark job chạy mỗi 5 phút, MERGE partial-day aggregates vào Gold.**

Loại "streaming aggregation Flink → Gold": UPDATE liên tục vào một Delta table theo cách streaming (DeltaTable.forPath.merge) tạo ra lock contention khi nhiều micro-batches cùng MERGE vào partition cùng ngày. Delta MERGE không phải streaming-native — mỗi MERGE là một full scan của target partition.

Loại "replace partition mỗi 5 phút" (INSERT OVERWRITE): Safe hơn MERGE nhưng quét lại toàn bộ Silver từ 00:00 đến giờ hiện tại mỗi 5 phút. Vào cuối ngày (23:55), job quét 24h × 0.7 TB/day = 0.7 TB mỗi 5 phút = 201 TB/ngày Silver scan. Compute cost bùng nổ.

Giải pháp chọn: chỉ aggregate 5-phút window mới nhất từ Silver, MERGE `(date, tenant_id, model)` key vào Gold. Mỗi MERGE chỉ động đến partition `date=today`.

### D6 — Storage Tiering cho Bronze

**Chọn S3 Standard 7 ngày → hard DELETE (không archive).**

Loại "S3 Glacier sau 7 ngày": Requirement nói giữ full data 7 ngày cho incident review, sau đó chỉ giữ aggregates. Archive sang Glacier ngụ ý data vẫn tồn tại sau 7 ngày — tạo ra ambiguity về retention policy và compliance risk. Nếu không cần, đừng giữ.

Loại "S3 Standard 30 ngày": Chi phí tăng 4× so với 7-ngày. Không có business requirement nào justify.

S3 Object Lock (WORM mode): apply cho Bronze partitions có incident tag để ngăn lifecycle deletes accidentally xóa evidence đang trong review.

---

## 4. Failure Modes

### FM1 — Schema Drift trong JSON Payload Lúc 3 Giờ Sáng

**Kịch bản:** Provider model mới deploy, đổi `usage.input` → `usage.input_tokens`. Silver parse job nhận NULL trên `prompt_tokens` column. Gold `cost_usd` compute thành 0. Dashboard hiển thị cost = $0 cho tất cả tenants — không ai alert vì "cost thấp" không trigger alarm thông thường.

**Detection:** Monitor Silver row count vs Bronze row count ratio theo 5-phút window. Nếu Silver/Bronze < 0.8 (tức hơn 20% rows bị drop vì parse fail) → PagerDuty. Monitor thứ hai: Gold `cost_usd` aggregate = 0 cho bất kỳ model nào trong bất kỳ 5-phút window nào → alert (cost không bao giờ = 0 khi có traffic).

**Rollback:** Delta time travel trên Silver: `DeltaTable(SILVER).restore(last_good_version)`. Bronze vẫn còn nguyên (append-only). Sau khi fix parse logic, replay Bronze → Silver từ `last_good_version_timestamp` dùng `DeltaTable(BRONZE, version=...)`. Tổng downtime dashboard: lag bằng đúng thời gian để replay, thường < 10 phút với Spark.

### FM2 — Kafka Consumer Lag Spike → Burst Write Vào Bronze

**Kịch bản:** Kafka consumer group (Spark Structured Streaming) bị lag đột ngột (GC pause, pod restart) 15 phút. Khi recover, Spark cố gắng xử lý 15 phút backlog trong batch tiếp theo → write 10× lượng bình thường vào Bronze trong 30 giây → Delta transaction log conflict nếu có concurrent compaction job đang chạy.

**Detection:** Spark UI lag metric + Delta concurrent write retry counter > 3 → alert. Consumer lag Kafka metric > 5 phút → warning (cảnh báo sớm trước khi burst xảy ra).

**Rollback:** Kafka consumer có `maxOffsetsPerTrigger` cap. Set cap = 5M rows/trigger → burst được rate-limited, spread ra nhiều batches. Data không mất vì Kafka vẫn giữ messages (1-hour retention). Delta write conflict: Spark retry với exponential backoff là đủ — Delta optimistic concurrency tự resolve.

**Tie với Day 18 concept:** Delta ACID transaction đảm bảo partial burst write không corrupt table — nếu batch fail giữa chừng, commit không xảy ra, data rolled back tự động.

### FM3 — PII Tokenization Key Bị Compromise

**Kịch bản:** HashiCorp Vault key (AES-FF1 master key) bị leaked qua misconfigured CI/CD secret. Attacker có thể decode tất cả `tokenized_user_id` trong Bronze và Silver để recover real user IDs.

**Detection:** CloudTrail + Vault audit log: bất kỳ Vault read từ IP không nằm trong allowlist (Spark job subnets) → immediate PagerDuty P0. Vault lease TTL ngắn (1 giờ) → key rotation dễ dàng hơn.

**Rollback:**
1. Vault: revoke key ngay lập tức. Spark Structured Streaming job không thể tokenize tiếp → dừng ingest (acceptable downtime < 1 giờ).
2. Rotate key trong Vault.
3. Dùng Delta time travel để đọc Bronze từ version 0 (hoặc từ last clean checkpoint), re-tokenize với new key, ghi lại vào Bronze mới.
4. `VACUUM` Bronze cũ với `retentionHours=0` ngay sau khi re-tokenize xong → xóa old versions có old tokenization.
5. Resume Spark Streaming với new key.

**Lesson:** Đây là lý do tại sao không tokenize tại query time (step D3) — rotate key dễ hơn nhiều khi token là deterministic FPE thay vì random encryption-at-rest.

### FM4 — S3 Lifecycle Deletes Bronze Partition Đang Trong Incident Review

**Kịch bản:** Security incident mở ngày 6, cần data từ ngày 1-7. S3 lifecycle rule tự động delete partition `date=day1` vào ngày 8, trước khi incident review team finish. Data gone permanently.

**Detection (proactive):** Khi incident ticket được tạo, automation tag Bronze S3 partitions liên quan với `x-amz-meta-incident-hold=true`. S3 lifecycle rule có condition `--filter "Tag={incident-hold,true}"` → bỏ qua objects có tag này.

**Rollback:** Không có rollback nếu đã delete. Đây là "no-fix" failure mode — mitigation duy nhất là prevention qua tagging SOP. Đây là lý do có monitoring: khi incident opened, Lambda function tự động tag partitions trong ±3 ngày range của incident timestamp.

---

## 5. Ước Lượng Chi Phí Back-of-Envelope

### Assumptions

| Parameter | Value |
|---|---|
| Raw throughput | 5 TB/day (1B req × 5 KB/req) |
| Parquet + Zstd compression ratio | 5:1 (JSON → columnar) |
| Dedup rate (Silver vs Bronze) | 70% (30% are retries) |
| S3 Standard price | $0.023/GB-month |
| S3 Infrequent Access price | $0.0125/GB-month |

### Storage

| Layer | Size | Retention | Tier | $/month |
|---|---|---|---|---|
| Bronze | 1 TB/day × 7 days = **7 TB** | 7 days | S3 Standard | 7,000 × $0.023 = **$161** |
| Silver | 0.7 TB/day × 7 days = **4.9 TB** | 7 days | S3 Standard → IA day 7 | ~**$113** |
| Gold | 24 rows/day × 365 × (100 B/row) ≈ **0.9 GB/year** | 1 year | S3 Standard | **$0.02** |
| Kafka (1-hour) | 5 TB/day × 1/24 = **~208 GB** | 1 hour buffer | MSK storage | ~**$5** |
| **Total Storage** | | | | **~$279/month** |

**$279/month, thoải mái trong budget $5K/month.** Headroom còn lại dùng cho:
- S3 request costs (PUT/GET/LIST): ~$200-400/month ở 1B req/ngày
- CloudWatch + PagerDuty alerting: ~$100/month
- **Tổng ước tính: ~$800-900/month storage + ops** — còn $4K+ headroom cho compute (Spark EMR, Trino EKS).

### Tại sao không đắt hơn?

Key insight: Bronze và Silver chỉ giữ 7 ngày. 5 TB/day × 7 = 35 TB raw, nhưng sau compression 5:1 → 7 TB. Đây là 86% savings so với naive "giữ raw JSON 7 ngày" = $322/month chưa compress vs $161 sau compress. Gold là "nearly free" vì aggregates nhỏ đến mức không đáng kể.

---

## 6. MVP Một Tuần — Slice Nhỏ Nhất Chứng Minh Architecture Work

**Mục tiêu:** Chứng minh end-to-end pipeline từ Kafka → Bronze → Silver → Gold hoạt động với đúng latency SLA, không cần scale full 1B req/ngày.

**Scope MVP (5 ngày):**

| Ngày | Deliverable |
|---|---|
| 1 | Kafka topic + Spark Structured Streaming job (30s trigger) ghi Bronze Delta table trên local MinIO. Schema fixed. PII tokenization với AES-FF1 mock key (no Vault). |
| 2 | Bronze → Silver batch job (dedup by request_id, parse JSON, validate). Assert Silver < Bronze. Verify Delta time travel: `DeltaTable(BRONZE, version=0)` vs `version=1`. |
| 3 | Silver → Gold MERGE job (5-phút schedule). Verify p50/p95/cost_usd/error_rate columns populated. Verify MERGE idempotent (chạy 2 lần → Gold unchanged). |
| 4 | Inject "bad schema" event vào Kafka (schema drift simulation). Verify Silver row drop alert fires. Verify restore-from-time-travel recovers Silver. |
| 5 | S3 lifecycle policy simulation: `aws s3api put-bucket-lifecycle-configuration` với 7-day rule. Verify Gold persists sau Bronze deleted. End-to-end latency measurement: event → Gold visible ≤ 5 phút. |

**Non-scope MVP:** real PII tokenization (Vault), full 35K req/s scale, multi-tenant partitioning, Trino cluster, CloudWatch alerting. Những thứ này validated by architecture, không cần demo ở MVP.

**Acceptance criteria:** chạy `docker compose up && python scripts/e2e_smoke.py` → in ra "Bronze wrote N rows; Silver deduped M rows; Gold has ≥7 dates × 3 models; end-to-end lag = X seconds (target ≤300s)."

---

## Self-Checklist

- [x] ≥5 quyết định chính với ≥2 alternatives bị loại (D1–D6: 6 quyết định)
- [x] Numbers xuyên suốt document (35K req/s, $279/month, 5:1 compression, etc.)
- [x] ≥4 Day 18 concepts áp dụng: medallion layout (Bronze/Silver/Gold), ACID + time travel (FM1 restore, FM2 concurrent write), Z-ORDER (Silver), deletion/lifecycle (FM4), PII tokenization (D3), MERGE upsert (Gold refresh)
- [x] ≥3 failure modes với detection + rollback cụ thể (FM1–FM4: 4 modes)
- [x] Cost math kiểm tra được: $0.023/GB × 7,000 GB = $161
- [x] MVP slice cụ thể, shippable trong 1 tuần

---

*Dương Quang Khải · AICB-P2T2 Day 18 Bonus Challenge*
