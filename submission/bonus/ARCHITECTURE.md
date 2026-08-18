# Bonus — Kiến trúc Lakehouse cho LLM Observability (1B requests/ngày)

> **Role:** Architect on-call. **Deliverable:** Quyết định kiến trúc team sẽ bảo vệ trong design review.
> Mọi con số là ước lượng dựa trên dữ liệu thật/đã kiểm chứng trong lab; math hiện ra để reviewer kiểm tra được.

---

## 1. Problem statement

Foundation-model API team log toàn bộ request/response để vận hành: **1B req/ngày, ~5 KB/req → 5 TB/ngày raw**. Bốn ràng buộc xung đột nhau:

1. **Dashboard cost & latency theo tenant**, refresh mỗi **5 phút**.
2. **Prompt/response đầy đủ giữ 7 ngày** cho incident review; sau 7 ngày chỉ giữ **aggregates 1 năm**.
3. **PII phải được redact trước khi bất kỳ ai đọc** — gồm cả dashboard và analyst ad-hoc.
4. **Tổng chi phí storage ≤ $5K/tháng.**

Vì sao khó: giữ đầy đủ payload (5 TB/ngày) trực tiếp đối nghịch với cap chi phí (150 TB/tháng nếu giữ không nén ≈ $3.45K chỉ riêng storage, chưa kể aggregates và compute). PII không thể "xử lý sau" — nếu chưa tokenize lúc ingest, mọi đường đọc sau đều phải trả chi phí redact và rò rỉ ở các tool ad-hoc. Dashboard 5 phút không cho phép quét 5 TB mỗi lần refresh. Mục tiêu là thiết kế khiến 4 ràng buộc này cùng thoả, thay vì chọn 3 bỏ 1.

---

## 2. Architecture diagram

```
                         ┌──────────────────────────────────────────┐
 Kafka (5 KB events,     │       Catalog = CONTROL PLANE (Polaris)  │
 1B req/ngày)            │  · per-tenant row filters (PII gating)   │
    │                    │  · schema registry (opt-in evolution)    │
    ▼                    │  · time-travel / retention policy        │
 Streaming ingest        └──────────────▲───────────────────────────┘
 (Spark 2-min microbatch)               │
    │  idempotent, keyed by request_id  │
    ▼                                   │
 ┌──────────────┐   dedup+MERGE   ┌──────────────┐
 │ Bronze raw   │───────────────► │ Silver valid │
 │ day(ts) part │  (NB4 pattern)  │ z-order(tenant)│
 │ tokenized@   │                 └──────┬───────┘
 │  ingest (PII)│                        │ nightly job
 └──────────────┘                        ▼
                                 ┌──────────────┐   Trino/DuckDB
                                 │ Gold metrics │──► Dashboard (5 min)
                                 │ tenant×minute│    per-tenant row filter
                                 └──────────────┘
 Lifecycle (nightly): OPTIMIZE cadence · VACUUM + orphan sweep (7d) · expire_snapshots pair
```

Một diagram duy nhất, dày đặc: ingestion path ở trên, query path ở dưới, lifecycle chạy ngang, catalog là vòng kiểm soát bao quanh.

---

## 3. Quyết định chính + alternatives đã loại

### Q1. Table format: **Delta Lake** (catalog-managed, Delta 4.1)
- **Chọn Delta vì:** Change Data Feed (CDF) là công dân hạng nhất — job aggregate đọc *incremental* từ CDF thay vì quét lại 5 TB/ngày; `MERGE WHEN MATCHED AND src.ts > tgt.ts` xử lý late-arrival đúng chỗ; stack đọc (Spark, Trino, DuckDB, delta-rs) đã chín.
- **Loại Iceberg vì:** hidden partitioning + partition evolution rất mạnh (lab NB5), nhưng OSS Python path chưa có CDF/incremental read ổn định — với 5 TB/ngày, aggregate phải quét lại sẽ phá cả cost lẫn SLA 5 phút.
- **Loại Hudi vì:** cơ chế indexing mặc định nặng với payload 5 KB/req, matrix tooling OSS hẹp hơn.
- *Concept Day 18 áp dụng:* ACID + CDF (NB7 dùng CDF để lan truyền delete), catalog-managed = catalog là control plane (NB5).

### Q2. Catalog: **REST catalog (Polaris)** — không dùng "thư mục làm catalog"
- **Chọn Polaris vì:** cấp row-filter theo tenant ngay tại catalog → *mọi* engine (Trino, DuckDB, ad-hoc analyst) đều bị chặn PII cùng một chỗ, không phụ thuộc ai nhớ `WHERE tenant_id`. Đồng thời cho time-travel + retention policy tập trung.
- **Loại "path-as-catalog" vì:** mỗi team tự quản đường dẫn → row filter không thể enforce ở tầng nền, PII gating là *lời hứa* chứ không phải ràng buộc.
- **Loại Unity Catalog vì:** vendor lock-in, đúng bài toán F của lab.
- *Concept Day 18:* catalog là security boundary (slide §12).

### Q3. Partitioning & clustering: `day(ts)` + **Z-order theo `tenant_id`**
- **Chọn:** partition theo `day(ts)` (retention 7 ngày cắt theo partition, không cần quét), Z-order theo `tenant_id` vì hot path là "filter by tenant trong khoảng thời gian". Đo được trong lab NB2: Z-order làm file-skipping hoạt động — từ 200 file còn ~1 file phủ tenant đích.
- **Loại Hive-style partition column (`dt=...`)** mà user phải tự nhớ filter: quên predicate = full scan — đúng bài toán NB5 (pruning ratio ≥ 5× mất hết khi quên).
- **Loại partition-only theo tenant vì:** skew — 1% tenant "hot" chiếm 60% traffic, partition lệch nặng.

### Q4. PII: **Tokenize một lần tại ingest (Bronze), không redact-on-read**
- **Chọn:** deterministic tokenization (HMAC-SHA256 + format-preserving, key theo tenant) ngay tại streaming job, trước khi ghi Bronze. Raw payload chỉ tồn tại trong bộ nhớ của worker.
- **Loại "redact khi query" vì:** mỗi dashboard refresh + mỗi analyst ad-hoc trả lại chi phí redact; cache không dùng được; tool nào quên filter là rò rỉ.
- **Loại "xoá cột PII hẳn" vì:** vi phạm yêu cầu incident review 7 ngày (cần payload gốc để điều tra).
- **Loại "blob pointer cho payload" vì:** đo trong lab NB7 — pointer không giảm tổng byte; column pruning của Parquet đã bảo vệ scan phân tích; pointer chỉ thêm một hệ quản lý lifecycle thứ hai.
- *Concept Day 18:* security + tokenization tại Bronze (slide §11/§12).

### Q5. Lifecycle & retention: **chính sách 2 tầng + cặp Job 3/Job 4**
- **Chọn:** hot 7 ngày giữ đầy đủ trên Standard; sau 7 ngày job nightly tính aggregates (tenant×minute) vào Gold, rồi VACUUM phần raw cũ. Retention đặt là **quyết định viết ra**, không phải default.
- **Loại "giữ mãi, khi cần mới xoá" vì:** storage tăng vô hạn 5 TB/ngày → vỡ cap $5K.
- **Loại "xoá raw ngay sau aggregate" vì:** vi phạm 7-ngày incident review.
- **Cặp bắt buộc:** chạy `VACUUM` **và** orphan sweep — lab NB6 đo: `VACUUM` chỉ dọn file tombstone trong log, file job crash để lại (chưa commit) **vô hình** với VACUUM; `expire_snapshots` (Iceberg) là metadata-only. Nếu chỉ chạy một nửa, "đã expire mà hoá đơn S3 không giảm".
- *Concept Day 18:* time travel + FinOps tiering + Job 3/Job 4 là một cặp (NB6).

### Q6. Streaming ingestion cadence: **micro-batch 2 phút + OPTIMIZE định kỳ**
- **Chọn:** Spark Structured Streaming trigger 2 phút, write idempotent keyed by `request_id`; chạy OPTIMIZE + Z-ORDER nightly theo partition. Thoả SLA dashboard 5 phút (có 2 batch buffer).
- **Loại per-request write sync vì:** ~1M writes/sec lên object storage là bất khả thi.
- **Loại trigger 5 giây vì:** đúng anti-pattern small-file của NB6 — hàng trăm commit tí hon mỗi đêm, mỗi file một `GET`, chi phí không tuyến tính. *"Sửa trigger interval rẻ hơn trả tiền cho người dọn hậu quả."*

### Q7. Serving: **Gold aggregate + engine đọc lakehouse, không nhân bản OLAP**
- **Chọn:** dashboard đọc Gold (tenant×minute, vài trăm GB) qua Trino/DuckDB; incident review 7 ngày đọc Silver với row filter. 
- **Loại "để dashboard quét Silver/raw trực tiếp" vì:** 5 TB/ngày × p95 không vào nổi 5 phút, cost tăng.
- **Loại "đồng bộ sang OLAP riêng (ClickHouse/StarRocks)" vì:** hai system-of-record → lifecycle skew đúng bài NB7 (external index còn trả data đã xoá); vector/aggregate phải sống cùng lifecycle dữ liệu gốc.

---

## 4. Failure modes (kịch bản 3 giờ sáng)

### FM1 — Rò rỉ PII vào Gold vì tokenizer không phủ field mới
Model mới thêm field `billing_address`; schema evolution opt-in không tự merge, nhưng engineer merge tay quên tokenize → PII lọt xuống aggregate.
- **Detection:** job scan mẫu 5% Gold mỗi ngày kiểm pattern PII (SĐT/email/CMND) → alert khi hit > 0; schema diff so với registry.
- **Rollback:** Gold là bảng aggregate — **time travel về version trước lỗi** và chạy lại job từ Silver (Silver vẫn tokenized). Không cần chạm raw.
- *Concept Day 18:* time travel + schema evolution opt-in (NB1/NB3).

### FM2 — Small-file storm sau burst + retry
Một tenant phát sóng sự kiện lớn; ingest retry nhiều lần → 100K file nhỏ, dashboard p95 tụt, bill `GET` tăng.
- **Detection:** metric `avg file size` + `file count per partition` (cảnh báo khi < 64 MB/file); alert trên DLQ depth.
- **Rollback:** chạy OPTIMIZE + Z-ORDER ngay trên partition đó; **orphan từ writer crash phải dùng phép hiệu tập hợp** (files on disk − files trong log) — VACUUM không thấy chúng (NB6).
- *Concept Day 18:* small-file anti-pattern + orphan sweep.

### FM3 — Một payload hỏng giết cả batch ingest
Một event thiếu field khiến job parse crash, cả 2-phút batch rớt, dashboard trễ SLA.
- **Detection:** dead-letter queue + alert DLQ depth; job metric "rows dropped".
- **Rollback:** event hỏng vào DLQ, batch đúng vẫn commit (per-batch try); repair bằng `MERGE` với guard `src.ts > tgt.ts` để event đúng luôn thắng event sai (late-arrival semantics).
- *Concept Day 18:* schema enforcement (NB1) — unknown field → quarantine, không auto-merge.

### FM4 — Retention chạy quá tay làm mất time travel
Sửa `retention_hours` nhầm về 0 → VACUUM xoá file đang được reader đọc giữa chừng, time travel vỡ.
- **Detection:** test expiry trên **shadow table** trước khi áp production policy; alert khi `history()` < ngưỡng.
- **Rollback:** phục hồi từ bản snapshot policy trước; giữ retention ≥ 7 ngày (bằng window incident review).
- *Concept Day 18:* VACUUM/expire + thời-gian-lùi là quyết định có chủ đích, không phải default (NB6/NB8).

---

## 5. Ước lượng chi phí back-of-envelope

Giả định: payload LLM là JSON, nén **ZSTD Parquet ~3.5×** → **~1.4 TB/ngày lưu**. 10K tenant.

| Hạng mục | Math | $/tháng |
|---|---|---|
| Hot 7 ngày (Standard $23/TB) | 7 × 1.4 TB = 10 TB | **$230** |
| Gold aggregates 1 năm (S3 IA $12.5/TB) | 10K × 1440 × 365 = 5.3B rows × ~150 B ≈ 0.8 TB | **$10** |
| Ingest pipeline (Spark, ~40 vCPU-hr/ngày @ $0.02) | 40 × 30 × 0.02 | **$24** |
| OPTIMIZE + aggregation nightly (write-amplification ~1.5×) | ~1.5 × 1.4 TB × 30 × $0.023/GB | **~$45** |
| **Tổng storage + compute** | | **≈ $310/tháng** |

**So sánh phương án loại:** giữ raw không nén 150 TB/tháng Standard = **$3,450**; cộng thêm bản aggregate nhân bản + redact-on-read thì vượt cap. Cap **$5K** chính là ràng buộc ép phải nén-tại-ingest + tiering + CDF (không quét lại). *"Nén 3.5× × 30 ngày × $23/TB"* kiểm tra được — không phải *"chắc rẻ thôi"*.

---

## 6. Build trước cái gì (slice MVP 1 tuần)

1. **Ngày 1–2:** ingest 1 tenant (data tổng hợp) → Bronze, tokenizer chạy inline (reuse hàm tokenize của NB4/NB7).
2. **Ngày 3:** Bronze → Silver dedup/validate + partition `day(ts)` + Z-order theo tenant (copy pattern NB4/NB2).
3. **Ngày 4:** Gold tenant×minute aggregate + một dashboard query đọc Gold.
4. **Ngày 5:** job lifecycle: expire + orphan sweep cặp Job 3/4 (skeleton, chạy trên shadow table).
5. **Ngày 6–7:** đo PII scan (0 hit ở Silver/Gold), đo refresh < 5 phút, in bảng cost model.

**Pass criteria:** dashboard < 5 phút · Silver/Gold không chứa PII pattern · cost model ≤ $5K · chạy được từ `make setup` clean.
