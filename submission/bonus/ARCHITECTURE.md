# Architecture: LLM Observability at 1 Billion Requests/Day

**Topic A** — Foundation-model API team, 1B req/day, ≤ $5K/month storage.  
**Author:** Đặng Thành Tùng — 2A202600023

---

## 1. Problem Statement

Một foundation-model API team log mọi request/response trên LLM serving
infrastructure. **1B requests/ngày × ~5 KB/request = 5 TB/ngày raw**.  
Four constraints collide simultaneously:

| Constraint | Detail |
|---|---|
| **Latency** | Dashboard cost & latency theo tenant, refresh mỗi **5 phút** |
| **Retention** | Prompt/response đầy đủ giữ **7 ngày** (incident review); sau đó chỉ giữ aggregates **1 năm** |
| **Privacy** | PII phải được redact **trước khi bất kỳ ai đọc** — kể cả direct S3 reads |
| **Budget** | Hard cap **≤ $5K/tháng** toàn bộ storage + compute |

**Tại sao khó:** Không có single storage tier giải quyết cả bốn. Hot storage đủ
nhanh cho 5-minute dashboard quá đắt nếu giữ 1 năm. Archive storage rẻ nhưng
không cho phép p95 < 2s. PII tokenization không thể là after-thought vì
cleartext đã tồn tại trên disk là vi phạm. Volume 5 TB/ngày có nghĩa là
compaction, Z-order, và lifecycle phải chính xác hoặc cost explode.

---

## 2. Architecture Diagram

```
 LLM API Servers (pods)
        │
        │  JSON events (request_id, tenant_id, ts,
        │  model, prompt_text*, response_text*, usage, latency_ms)
        │  * chứa PII (email, phone, tên người dùng)
        ▼
 ┌──────────────────────────────────────────────────────────────────┐
 │  KAFKA (topic: llm_raw_events, 64 partitions, 48h retention)    │
 │  Partitioned by tenant_id % 64 → ordering per tenant            │
 └────────────────────────┬─────────────────────────────────────────┘
                          │
          ┌───────────────▼───────────────┐
          │   FLINK INGESTION JOB         │
          │  1. Parse + validate schema   │
          │  2. PII Tokenization          │◄── PII Vault Salt Store
          │     (HMAC-SHA256 + per-tenant │    (AWS Secrets Manager)
          │      salt, deterministic)     │
          │  3. Dead-letter → DLT topic   │
          └───────────────┬───────────────┘
                          │
          ┌───────────────▼───────────────────────────────────────┐
          │  BRONZE  s3://lake/bronze/llm_calls_raw/              │
          │  Delta Lake, append-only, partition: date / hour      │
          │  Z-order: tenant_id                                   │
          │  Schema:                                              │
          │    request_id (STRING PK), tenant_id, ts,            │
          │    model, prompt_token_id*, response_token_id*,      │
          │    prompt_tokens (INT), completion_tokens, latency_ms │
          │    status, cost_usd                                   │
          │  * token references into PII Vault, NOT cleartext     │
          │                                                       │
          │  PII Vault: s3://secure/pii_vault/ (restricted IAM)  │
          │    token_id → cleartext, tenant_id, created_at       │
          │    Every read → audit log in CloudTrail               │
          │                                                       │
          │  Lifecycle: DELETE WHERE date < NOW()-7d + VACUUM     │
          │  → Files physically gone (PII compliance)             │
          └───────────────┬───────────────────────────────────────┘
                          │  Spark batch every 5 min
                          │  (reads Bronze Delta CDF)
          ┌───────────────▼───────────────────────────────────────┐
          │  SILVER  s3://lake/silver/llm_calls/                  │
          │  Delta Lake, partition: date / tenant_id              │
          │  Schema enriched: cost_category, latency_tier,        │
          │    is_error, normalized_model                         │
          │  MERGE ON request_id (dedup from retries)             │
          │  Lifecycle: 30d S3-Standard → S3-IA (no delete)       │
          └───────────────┬───────────────────────────────────────┘
                          │  MERGE every 5 min (Delta CDF)
          ┌───────────────▼───────────────────────────────────────┐
          │  GOLD  s3://lake/gold/llm_daily_metrics/              │
          │  Delta Lake, partition: date                          │
          │  Z-order: tenant_id                                   │
          │  Pre-aggregated: tenant_id × date × model            │
          │    → request_count, cost_usd_total,                  │
          │       p50_latency_ms, p95_latency_ms, p99_latency_ms  │
          │       error_rate, token_count_total                   │
          │  Lifecycle: 365d S3-Standard                          │
          └───────────────┬───────────────────────────────────────┘
                          │
          ┌───────────────▼──────────────────┐
          │  QUERY PATH                      │
          │  Dashboard (5-min SLA)           │
          │  └─► Trino/DuckDB → Gold         │
          │                                  │
          │  Incident Review (7-day window)  │
          │  └─► Restricted Spark → Silver   │
          │      + PII Vault lookup (audited)│
          │                                  │
          │  Compliance Audit                │
          │  └─► CloudTrail + Glue catalog   │
          └──────────────────────────────────┘

  CATALOG: AWS Glue Data Catalog + Lake Formation (column-level security)
  LINEAGE: OpenLineage emitted by Flink + Spark → Marquez
```

---

## 3. Quyết Định Chính, Kèm Alternatives Đã Loại

### Decision 1: Table Format — Delta Lake vs Apache Iceberg vs Apache Hudi

**Chọn: Delta Lake (delta-rs 0.21+, format version 3 với deletion vectors)**

- **Loại Apache Iceberg:** Iceberg có REST Catalog ecosystem tốt (Polaris,
  Nessie) và native branching/tagging cho time travel. Tuy nhiên, Delta có
  *deletion vectors* (format v3) — khi PII scrubbing cần xóa individual rows
  mà không rewrite toàn bộ Parquet file, deletion vectors hiệu quả hơn
  Iceberg's row-level deletes (vẫn rewrite). Ở 5 TB/day, rewrite cost là
  non-trivial. Ngoài ra, delta-rs cho phép Flink/Python ingest mà không cần
  JVM, quan trọng cho cost optimization trên ingestion cluster.

- **Loại Apache Hudi:** Hudi xuất sắc ở record-level upserts (MOR tables)
  và near-real-time CDC. Nhưng ecosystem analytics query của Hudi kém hơn
  (DuckDB không có native Hudi reader, Trino support còn lag). Với workload
  này, Bronze là append-only, Silver dùng MERGE không phải continuous upserts
  — overhead của Hudi's compaction và timeline management không mang lại lợi
  ích tương xứng.

**Tại sao Delta win:** Deletion vectors cho PII scrubbing hiệu quả + delta-rs
ecosystem trưởng thành + Z-order clustering native = ba features cùng lúc.

---

### Decision 2: PII Strategy — Tokenize-at-Bronze vs Encrypt-at-Rest vs Mask-at-Query

**Chọn: Deterministic tokenization tại điểm ingestion (Flink job), trước khi
write vào Bronze**

- **Loại Encrypt-at-rest (AES-256 trên S3):** Encryption chỉ bảo vệ khi data
  at-rest bị stolen. Khi Trino/Spark query decrypt để serve analyst, cleartext
  PII vẫn xuất hiện trong query results, query logs, và memory. Vi phạm
  constraint *"PII redact trước khi bất kỳ ai đọc."* Key rotation cũng phức
  tạp: nếu rotate key, toàn bộ Bronze cần re-encrypt.

- **Loại Column masking tại query time (Lake Formation / Trino view):** Mask
  hoạt động khi query đi qua catalog. Nhưng với Delta tables trên S3, direct
  Parquet reads (aws s3 cp + pandas) hoàn toàn bypass catalog. Với một team
  có quyền S3 read, cleartext PII accessible. Audit trail cũng không đầy đủ.

**Chọn Tokenize-at-Bronze:** Flink job thay thế PII fields (email, phone,
tên người dùng trong prompt/response text) bằng deterministic token
`HMAC-SHA256(value, tenant_specific_salt)`. Token-to-cleartext mapping lưu
trong *PII Vault* table (separate S3 prefix `s3://secure/pii_vault/`, IAM
policy deny tất cả trừ role `pii-incident-responder`, mọi read
audit-logged qua CloudTrail). **Bronze table không bao giờ chứa cleartext PII
— kể cả direct S3 read cũng an toàn.** Trade-off: prompt semantic search
không thể chạy trực tiếp trên Bronze (phải tokenize query string trước) —
chấp nhận được vì use case chính là cost/latency analytics, không phải
semantic search.

---

### Decision 3: Streaming Ingestion — Apache Flink vs Spark Structured Streaming vs Kinesis Firehose

**Chọn: Apache Flink 1.19 với Delta sink connector**

- **Loại Kinesis Firehose:** Fully managed, operational overhead thấp nhất.
  Nhưng Firehose minimum buffer = 60 giây → với 5-minute dashboard SLA,
  Bronze data sẽ luôn lag 1-2 minutes chưa kể Silver/Gold processing. Quan
  trọng hơn: Firehose không hỗ trợ stateful transformations — PII tokenization
  cần lookup tenant salt từ Secrets Manager, không thể làm trong Firehose
  Lambda transform (timeout 3 phút, không stateful).

- **Loại Spark Structured Streaming:** Viable alternative. Nhưng micro-batch
  latency của Spark (5-10s per checkpoint) cao hơn Flink's checkpoint interval
  (< 1s). Với 5 TB/day ≈ 58 MB/giây, Spark sẽ cần larger clusters để đạt
  throughput tương đương. Flink's backpressure mechanism cũng tốt hơn cho
  traffic spikes.

**Chọn Flink:** Sub-second checkpointing, exactly-once semantics với Delta
sink, stateful PII tokenization (in-memory tenant salt cache với 5-minute TTL
→ giảm Secrets Manager API calls), autoscale theo Kafka consumer lag.

---

### Decision 4: Partitioning Strategy — Date/Hour vs Date/Tenant vs Hash Bucket

**Chọn: `date` + `hour` tại Bronze; `date` + `tenant_id` tại Silver**

- **Loại Date/Tenant-only partition tại Bronze:** Với 10K active tenants × 365
  ngày = 3.65M partitions/năm. S3 LIST operations trên 3.65M prefixes rất
  chậm. Worse: long-tail tenants (90% tenants có < 1000 requests/day) tạo
  files < 1 MB → small file explosion → OPTIMIZE phải handle hàng triệu files.

- **Loại Hash bucketing only (no time partition):** Tốt cho JOIN lookups
  (request_id → tenant), nhưng lifecycle management không feasible. Để xóa
  7-day-old data cần scan toàn bộ table → O(N) cost thay vì O(1) partition
  drop. Z-order index phải được rebuild sau deletion.

**Chọn Date+Hour tại Bronze:** Lifecycle delete bằng partition drop
(O(1) metadata op). Z-order clustering trên `tenant_id` trong mỗi
date/hour partition → dashboard queries `WHERE tenant_id='X' AND date='Y'`
chỉ read 1-3 files thay vì full scan. Silver dùng Date/tenant_id (Silver
nhỏ hơn 30x, tenant-filtered query là hot path cho incident review).

---

### Decision 5: Retention Enforcement — Delta VACUUM vs S3 Lifecycle vs Application-level DELETE

**Chọn: Delta `DELETE` + `VACUUM` làm primary; S3 Object Expiration làm safety net**

- **Loại Application-level DELETE only (không VACUUM):** `DELETE FROM bronze
  WHERE date < x` chỉ tạo deletion markers trong Delta log — Parquet files
  vẫn tồn tại trên S3. PII vẫn accessible via direct S3 read. Không đáp ứng
  compliance requirement.

- **Loại S3 Lifecycle alone (no Delta DELETE):** S3 sẽ xóa Parquet files
  nhưng **không update Delta transaction log**. Kết quả: Delta log reference
  files không còn tồn tại → table corruption, `FileNotFoundException` khi
  query → production outage. Đây là failure mode nguy hiểm nhất và thường
  bị overlook.

**Chọn Delta DELETE + VACUUM:** Workflow: (1) `DELETE FROM bronze WHERE
date < NOW() - INTERVAL 7 DAYS` tạo deletion markers; (2) Confirm không có
active readers trên những versions đó; (3) `VACUUM bronze RETAIN 0 HOURS`
xóa Parquet files physically. S3 Lifecycle set expire = Day 10 làm safety net
(files không được VACUUM sau 10 ngày sẽ bị S3 expire). **Critical timing:**
VACUUM phải luôn chạy *trước* S3 Lifecycle expiry date để tránh table
corruption.

---

### Decision 6: Catalog — Unity Catalog vs Apache Polaris vs AWS Glue vs No Catalog

**Chọn: AWS Glue Data Catalog + Lake Formation**

- **Loại Databricks Unity Catalog:** Best-in-class governance, column-level
  lineage, fine-grained access. Nhưng tạo hard vendor lock-in: tất cả metadata
  (table definitions, permissions, lineage) sống trong Databricks control plane.
  Migration cost cao. Với $5K/month budget, Databricks workspace fee alone có
  thể exceed budget.

- **Loại Apache Polaris (Iceberg REST Catalog):** Open-source (Snowflake 2024),
  vendor-neutral, promising. Nhưng Delta Lake support qua UniForm còn
  experimental (không production-ready vào thời điểm design). Ecosystem tooling
  (DuckDB Delta reader, Trino Delta connector) không dùng Polaris catalog natively.

- **Loại No catalog (file-based only):** Không có lineage, không có
  column-level security (cần cho PII Vault access control), không audit trail
  cho compliance.

**Chọn Glue + Lake Formation:** Native AWS IAM integration, column-level
security cho PII fields (block `prompt_token_id` column trừ
`pii-incident-responder` role), Glue tables với Delta symlink manifest, Lake
Formation audit logs integrated với CloudTrail. Có thể migrate sang Polaris
sau khi ecosystem Delta support trưởng thành mà không thay đổi data files.

---

### Decision 7: Gold Refresh Mechanism — Materialized Views vs Streaming Aggregation vs Micro-batch MERGE

**Chọn: Micro-batch MERGE vào Gold mỗi 5 phút, driven by Delta CDF**

- **Loại Trino Materialized Views:** Open-source Trino không hỗ trợ
  incremental refresh — full refresh của materialized view trên 5 TB/day Silver
  sẽ mất 10-15 phút, không đáp ứng 5-minute SLA. Starburst Enterprise có
  incremental MV nhưng licensing cost không phù hợp budget.

- **Loại Pure streaming aggregation (Flink → Gold trực tiếp):** Yêu cầu
  dual-write (streaming write Gold + batch backfill khi late data arrive). Late
  requests (network issues ở tenants) arrive up to 10 phút sau event time.
  Streaming aggregation cần windowing với watermarks → nếu watermark thoải
  mái quá sẽ delay Gold; nếu tight quá sẽ miss late data. Reconciliation logic
  phức tạp hơn micro-batch.

**Chọn Micro-batch MERGE:** Spark job đọc Silver thay đổi qua **Delta Change
Data Feed (CDF)** (`readChangeFeed = true`), tính aggregates incremental, MERGE
vào Gold. Late data tự động được reconcile ở batch tiếp theo (5 phút sau).
Gold luôn consistent (ACID transaction). Spark job stateless → dễ restart,
dễ debug.

---

## 4. Failure Modes

### Failure 1: PII Vault Unavailable → Flink Job Dead-Letters All Events (3 AM)

**Scenario:** IAM role của Flink job expire (token rotation misconfiguration)
hoặc AWS Secrets Manager throttle (rate limit vượt do spike). Flink không đọc
được tenant salt → không thể tokenize → **fail-fast** (không bao giờ write
cleartext PII vào Bronze).

**Detection:**
- Flink job checkpoint lag > 30 giây → CloudWatch alarm → PagerDuty P1
- Kafka consumer group `flink-llm-bronze` lag tăng liên tục → dashboard alert
- Bronze write throughput drops to 0 (metric: `bronze_rows_written/min`)

**Rollback:**
1. Flink job configured với dead-letter topic `llm_raw_dlt` (Kafka, 7-day
   retention) — messages không bị mất.
2. Ops restore IAM role / Secrets Manager access.
3. Flink job restart từ last Kafka offset (Flink checkpoint stores offset) →
   exactly-once replay từ `llm_raw_dlt`.
4. Có thể có gap 5-30 phút trong Bronze → Silver/Gold sẽ có gap → dashboard
   sẽ show dip (acceptable — không phải data loss).

**Day 18 tie:** Flink exactly-once với Delta transactional writes đảm bảo
không có partial commits dù job crash mid-batch.

---

### Failure 2: VACUUM Runs After S3 Lifecycle Expires Files → Table Corruption (Day 18: Time Travel)

**Scenario:** S3 Lifecycle policy expire Bronze files ở day 8 (safety net).
VACUUM job scheduled daily có delay (job queue backup) và chạy ở day 9.
VACUUM thấy files đã bị S3 expire → Delta log reference files không tồn tại
→ queries tới `VERSION AS OF <day 7>` return `FileNotFoundException`. Worse:
VACUUM log cũng bị confused → subsequent VACUUMs có thể leave orphan files.

**Detection:**
- Hourly automated test: `SELECT COUNT(*) FROM bronze TIMESTAMP AS OF
  NOW() - INTERVAL 6 DAYS` → nếu fail → P1 alert
- VACUUM job status metric: `vacuum_files_deleted` vs
  `vacuum_files_expected` mismatch → alert

**Rollback:**
1. S3 Cross-Region Replication (us-east-1 → us-west-2) giữ một copy không
   bị S3 Lifecycle (lifecycle chỉ configured ở primary bucket).
2. Restore missing Parquet files từ replica bucket.
3. Re-run `VACUUM` với corrected retention.

**Lesson từ failure này:** S3 Lifecycle expiry phải set ≥
`VACUUM_retention_hours + 48h buffer`. Document trong runbook:
*"VACUUM phải chạy trước S3 Lifecycle expiry — đây là hard ordering constraint."*
Monitor: alert nếu VACUUM job không chạy trong 25 giờ.

---

### Failure 3: Uncontrolled Schema Evolution Breaks Silver MERGE (Day 18: Schema Evolution)

**Scenario:** LLM API team thêm field `reasoning_tokens` vào response payload
(chain-of-thought models). Flink job dùng schema inference → Bronze nhận thêm
column mới không có trong Silver schema. Silver MERGE job fail với
`AnalysisException: cannot resolve column 'reasoning_tokens'`.

**Detection:**
- Silver MERGE job failure → CloudWatch alarm → PagerDuty
- Gold MERGE job cũng fail (downstream) → `gold_last_refresh_ts` không update
  → dashboard banner "Data as of X minutes ago" (visible to all tenants)
- Metric: `silver_merge_lag_minutes > 10`

**Rollback:**
1. **Immediate (< 5 min):** Silver MERGE job rollback tới last known-good
   Docker image (GitOps, Argo CD). Bronze tiếp tục accumulate — không mất data.
2. **Gap analysis:** Xác định thời điểm schema changed dùng Bronze time travel:
   `DESCRIBE HISTORY bronze` → tìm version đầu tiên có schema mới.
3. **Schema migration (< 30 min):** `ALTER TABLE silver ADD COLUMN
   reasoning_tokens INT DEFAULT NULL`. Re-run Silver MERGE cho gap period
   (`readChangeFeed` từ version tìm được ở bước 2).
4. **Prevention:** Schema changes phải qua PR với automated compatibility
   check. Bronze writer dùng Delta's `mergeSchema=true` với
   **column allow-list** (không cho phép arbitrary new columns, chỉ columns
   trong approved schema registry).

---

### Failure 4: Late Data Flood Stales Gold Dashboard (Graceful Degradation)

**Scenario:** Top-5 tenant có network partition 90 phút. 900M events buffer
trong Kafka. Khi restored, flood vào Bronze đồng thời. Gold MERGE job nhận
~100M rows thay vì ~8M/5min → job OOM hoặc timeout (15 min) →
dashboard staleness > SLA.

**Detection:** Gold MERGE job duration > 8 phút → alert. Dashboard "last
refreshed" timestamp visible → users tự phát hiện.

**Mitigation (không phải rollback — graceful degradation):**
- Gold MERGE job có adaptive mode: nếu CDF volume > 20M rows → chuyển sang
  *hourly batch mode* với larger Spark cluster (auto-scaling).
- Dashboard shows degraded banner: "Data may be up to 15 minutes old" thay
  vì fail hoàn toàn.
- Sau khi flood absorbed, Gold MERGE tự động switch về 5-min mode.
- **Không cần manual intervention** — system tự heal.

---

## 5. Ước Lượng Chi Phí Back-of-Envelope

### Assumptions
- 1B req/day × 5 KB = **5 TB/day raw**
- Parquet + Zstd compression: **5:1** → 1 TB/day compressed
- ~10,000 active tenants
- AWS us-east-1

### Storage

| Layer | Raw size | Compressed | Duration | Tier | $/GB/month | Cost/month |
|---|---|---|---|---|---|---|
| Bronze (hot) | 5 TB/d | 1 TB/d | 7 days | S3 Standard | $0.023 | **$161** |
| Silver (enriched) | 1 TB/d×0.9 | — | 7d Standard + 23d IA | S3 Standard / IA | $0.023 / $0.0125 | **$270** |
| Gold (aggregates) | ~1 GB/d | — | 365 days | S3 Standard | $0.023 | **$9** |
| PII Vault (token maps) | ~500 MB/d | — | 7 days | S3 Standard | $0.023 | **$8** |
| CRR replica (DR) | 1 TB/d | — | 7 days | S3 Standard (us-west-2) | $0.026 | **$182** |
| **Total Storage** | | | | | | **~$630/month** |

*Silver math: 7 TB × $0.023 = $161 (first 7 days) + 23 TB × $0.0125 = $288 → ~$270 blended.*

### Compute

| Component | Spec | Cost/month |
|---|---|---|
| Flink ingestion cluster (always-on) | 8 × r5.2xlarge (32 vCPU, 256 GB RAM) | ~$800 |
| Spark Silver MERGE (micro-batch, 5min) | 2 × m5.4xlarge (spot) | **~$150** |
| Spark OPTIMIZE + VACUUM (daily batch) | 4 × m5.4xlarge × 2h/day | **~$100** |
| Kafka MSK (3 brokers + storage) | kafka.m5.large × 3, 2 TB EBS | **~$450** |
| Trino query cluster (Gold dashboard) | 4 × r5.2xlarge (spot, scale-to-zero off-peak) | **~$600** |
| AWS Secrets Manager API calls | ~50M calls/month (salt lookups) | **~$25** |
| **Total Compute** | | **~$2,125/month** |

### Data Transfer

| Flow | Volume | Rate | Cost/month |
|---|---|---|---|
| Kafka → Flink (same AZ) | 5 TB/day × 30 | free | $0 |
| S3 → Spark (same region) | ~20 TB/day reads | $0.01/GB | **~$600** |

### Total: **~$3,355/month** — dưới cap $5K còn **$1,645 buffer**

**Sensitivity analysis:**
- Nếu traffic tăng 1.5×: storage → $945, compute → $3,200, transfer → $900
  → **total ~$5,045** — vượt nhẹ. Mitigation: bật Kafka Snappy compression
  (giảm transfer 40%), scale Trino down ban đêm.
- Nếu traffic tăng 2×: cần re-architect Silver retention (giảm xuống 14 ngày
  thay vì 30 ngày) và aggressive spot instance usage.

---

## 6. MVP — Slice Một Tuần

**Goal:** Chứng minh end-to-end pipeline hoạt động với real data, PII không
bao giờ expose dù direct S3 read.

**Scope: Docker Compose trên laptop, không cần Kafka hay cloud.**

### Day 1–2: PII Tokenization + Bronze Write
- Python tokenization function: `tokenize_pii(value, tenant_id, salt_store)`
  dùng HMAC-SHA256
- Flink-simulated job (Python script): đọc JSON files → tokenize → write
  Delta Bronze + PII Vault
- Test: grep Bronze Parquet files cho emails/phones → expect zero hits
- PoC notebook: [poc/poc_pii_tokenization.ipynb](poc/poc_pii_tokenization.ipynb)

### Day 3: Bronze → Silver MERGE
- Spark (hoặc DuckDB) batch job: parse, dedup bằng `MERGE ON request_id`
- Schema evolution test: thêm column mới → Silver gracefully adopts
- Validate: Silver row count < Bronze row count (dedup worked)

### Day 4: Silver → Gold Micro-batch
- Script chạy mỗi 5 phút: đọc Silver CDF → tính aggregates → MERGE vào Gold
- DuckDB dashboard mock: query Gold, filter by tenant_id → validate latency
  < 200ms

### Day 5: Lifecycle Test
- Insert synthetic 8-day-old Bronze data
- Run `DELETE WHERE date < NOW()-7d` + `VACUUM RETAIN 0 HOURS`
- Assert: Parquet files gone (`aws s3 ls` or local filesystem check)
- Assert: `SELECT ... TIMESTAMP AS OF NOW()-5d` still works (time travel OK)
- Assert: `SELECT ... TIMESTAMP AS OF NOW()-8d` fails cleanly (expected)

**Success criterion:** End-to-end từ raw JSON → Gold dashboard với zero
cleartext PII trong Bronze/Silver/Gold, chạy trong Docker Compose, mọi tests
pass, lifecycle demo correct.

**Not in MVP:** Kafka, multi-region replication, Glue catalog, production Flink
cluster, Trino cluster, OpenLineage/Marquez integration.

---

## Appendix: Day 18 Concept Mapping

| Day 18 Concept | Áp dụng trong design |
|---|---|
| **Medallion architecture** | Bronze (raw tokenized) → Silver (enriched, deduped) → Gold (aggregates) với rõ ràng layer separation |
| **ACID transactions** | Flink → Bronze: exactly-once. Silver MERGE: atomic upsert. Gold MERGE: atomic partial update |
| **Time travel** | Incident review dùng `TIMESTAMP AS OF` trên Silver (7 ngày). Failure mode #2 explicitly tests time travel correctness |
| **Z-order / clustering** | Bronze và Gold đều Z-order trên `tenant_id` — hot path query filter |
| **Deletion vectors** | PII scrubbing ở Bronze dùng deletion vectors (row-level delete, không rewrite toàn file) |
| **Schema evolution** | Failure mode #3: controlled schema evolution với compatibility check và alter-table remediation |
| **Delta CDF** | Silver MERGE và Gold MERGE đều driven by CDF — incremental processing, không full scan |
| **Catalogs** | AWS Glue + Lake Formation: column-level security, audit logging, lineage foundation |
| **FinOps / lifecycle** | 5-tier analysis ($630 storage), S3 Standard → IA transition, aggressive VACUUM scheduling |
| **Lineage** | OpenLineage emitted by Flink + Spark → Marquez. PII Vault reads audit-logged in CloudTrail |
