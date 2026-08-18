# Technical Architecture Document: High-Scale LLM Observability Lakehouse (1B Requests/Day)

**Author:** Nguyễn Văn Hưng  
**Mã học viên (NXX):** 2A202601284  
**Target Topic:** Topic A — LLM Observability ở Quy Mô 1B Requests/Ngày  
**Date:** 2026-08-18  

---

## 1. Problem Statement

Our foundation-model API platform ingests **1,000,000,000 (1B) LLM inference requests/day**. At ~5 KB per raw JSON payload (prompts, completions, metadata, agent trace steps), the pipeline lands **5 TB of uncompressed raw data per day** (150 TB/month).

### Key Business & Technical Constraints:
1. **Real-time Tenant Analytics SLA:** Sub-5-minute dashboard refresh for cost, token usage, latency (p50/p95), and error rates aggregated per `tenant_id`. Point queries on `tenant_id` must return in < 2 seconds.
2. **Audit & Incident Review:** Full prompt/completion payloads must be retrievable for 7 days. After 7 days, raw payloads are purged, retaining only daily aggregates for 1 year.
3. **Data Governance & Privacy:** Mandatory PII tokenization/redaction **at the landing boundary** before data becomes queryable by analytics teams (Nghị định 13 & GDPR compliance).
4. **FinOps Cap:** Total infrastructure expenditure (Object Storage + Compute + Metadata operations) **must not exceed $5,000/month**.

---

## 2. Architecture Diagram (End-to-End Medallion Pipeline)

```
========================================================================================================
                                    HIGH-SCALE LLM OBSERVABILITY LAKEHOUSE
========================================================================================================

 [ API Gateway / LLM Proxy ] (1B req/day, 11,574 req/sec peak)
             │
             ▼
   [ Kafka / Redpanda Cluster ]  (Topic: llm.calls.raw — 5-sec buffer)
             │
             ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
 │  INGESTION & BRONZE LANDING LAYER                                                                  │
 │  - Inline PII Redactor & Salted HMAC Hash (Sha256) for user_id                                      │
 │  - Streaming Writer (Spark / Rust Delta-RS) with 10-sec micro-batches                              │
 │  - Partitioning: `date=YYYY-MM-DD/hour=HH`                                                         │
 └────────────────────────────────────────────────────────────────────────────────────────────────────┘
             │
             ▼
    ┌─────────────────────────┐
    │  BRONZE LAYER           │  Storage Format: Delta Lake (Unredacted Prompt/Response)
    │  `llm_calls_raw`        │  Retention: 7 Days (Strict Auto-Purge via Lifecycle)
    └─────────────────────────┘
             │
             ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
 │  SILVER CLEANING & DEDUPLICATION LAYER                                                             │
 │  - Streaming Deduplication via `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)`           │
 │  - Flattened JSON fields (prompt_tokens, completion_tokens, latency_ms, model, tenant_id)         │
 │  - Optimized Layout: `dt.optimize.z_order(["tenant_id", "model"])`                                 │
 └────────────────────────────────────────────────────────────────────────────────────────────────────┘
             │
             ▼
    ┌─────────────────────────┐
    │  SILVER LAYER           │  Storage Format: Delta Lake (Structured, PII-Free)
    │  `llm_calls_clean`      │  Partitioning: `date=YYYY-MM-DD` | Retention: 30 Days
    └─────────────────────────┘
             │
             ▼
 ┌────────────────────────────────────────────────────────────────────────────────────────────────────┐
 │  GOLD METRICS & ANALYTICS LAYER                                                                    │
 │  - Continuous Aggregation (every 5 mins): p50/p95 latency, cost_usd, error_rate per (tenant, model)│
 │  - DuckDB / Trino Serving Path (Zero-Copy Arrow scan)                                              │
 └────────────────────────────────────────────────────────────────────────────────────────────────────┘
             │
             ▼
    ┌─────────────────────────┐
    │  GOLD LAYER             │  Storage Format: Delta Lake / Iceberg Aggregates
    │  `tenant_daily_metrics` │  Partitioning: `year=YYYY` | Retention: 365 Days
    └─────────────────────────┘
             │
             ▼
 [ Grafana / Metabase Dashboards ]  (Query Latency p95 < 500ms)
========================================================================================================
```

---

## 3. Key Architectural Decisions & Rejected Alternatives

### Decision 1: Table Format — Delta Lake 4.1 (with Liquid Clustering)
* **Chosen:** **Delta Lake 4.1** using `deltalake` (delta-rs Rust binding) for ingestion micro-batches and DuckDB for gold aggregations.
* **Rejected Alternative A (Raw Parquet + Hive Metastore):** Rejected due to lack of ACID transactions. Under 11,500 req/sec streaming ingestion, concurrent compaction or read operations cause reader crashes (`FileNotFoundException`) and partial writes.
* **Rejected Alternative B (Apache Iceberg v2):** Iceberg is excellent for multi-catalog scenarios, but Delta-RS offers lower CPU overhead for ultra-fast 5-second micro-batch appends in pure Rust without JVM footprint.

### Decision 2: PII Redaction Boundary — Inline Ingestion Tokenization vs. Post-Hoc Batch Masking
* **Chosen:** **Inline Tokenization at Bronze Ingestion.** PII (emails, phone numbers, credit card numbers, API keys) is redacted via high-speed regex & regex-wasm filters before writing to disk. `user_id` is replaced with a salted HMAC-SHA256 hash.
* **Rejected Alternative (Post-Hoc Batch Redaction in Silver):** Rejected because storing raw unredacted PII even temporarily in Bronze violates Decree 13/2023/NĐ-CP and GDPR right-to-be-forgotten SLA. If unredacted data lands on disk, a subsequent deletion requires expensive Delta `MERGE` rewrites across terabytes of files.

### Decision 3: Storage Partitioning & Indexing — Hourly Partitioning + Z-ORDER (`tenant_id`)
* **Chosen:** Partition Bronze/Silver by `date=YYYY-MM-DD/hour=HH` combined with **Z-ORDER on `(tenant_id, model)`**.
* **Rejected Alternative (Partitioning by `tenant_id`):** Partitioning by `tenant_id` would generate over 10,000 tenant directories per day. For 1B requests, this creates a catastrophic **Small-File Problem** (over 240,000 micro-files/day), leading to S3 listing throttling and metadata explosion.
* **Tradeoff Reasoning:** Time-range partitioning (`date/hour`) keeps directory depth shallow, while Z-ORDER co-locates rows for the same `tenant_id` within Parquet row groups, achieving **>90% file pruning** on tenant queries.

### Decision 4: Storage Tiering & FinOps Strategy — S3 Lifecycle Auto-Tiering
* **Chosen:** 
  - **Days 0–7 (Hot):** S3 Standard for Bronze & Silver (`_lakehouse/bronze`, `_lakehouse/silver`).
  - **Days 8–30 (Warm):** S3 Standard-IA (Infrequent Access) for Silver.
  - **Days 31–365 (Cold):** S3 Glacier Instant Retrieval for Gold aggregated metrics.
  - **Day 8 Expiry:** Hard S3 Lifecycle rule to expire/delete raw Bronze payloads.
* **Rejected Alternative (Flat S3 Standard Retention):** Retaining 5 TB/day on S3 Standard for 1 year would cost **$3,450/month for storage alone**, leaving zero budget for compute or requests.

### Decision 5: Catalog & Control Plane — Local REST Catalog / Polaris with Managed Checkpoints
* **Chosen:** Catalog-managed control plane with explicit checkpoint frequency every 100 commits (`*.checkpoint.parquet`).
* **Rejected Alternative (File-system metadata listing):** Relying purely on file listing without checkpoints forces cold readers to replay 17,280 commit JSON files per day, causing query cold-starts to exceed 30 seconds.

---

## 4. Failure Modes & 3 AM Operational Playbooks

### Failure Mode 1: Small-File Explosion from Streaming Micro-Batches
* **Symptom:** S3 GET request costs spike, query latency degrades from 500ms to 25s, Grafana dashboards time out.
* **Root Cause:** Compaction cron job crashed, causing 10-second streaming appends to accumulate >20,000 tiny 200 KB Parquet files in `_lakehouse/silver`.
* **Detection:** Prometheus alert `lakehouse_file_count > 5000` or `avg_file_size_mb < 10`.
* **Playbook & Rollback:**
  1. Trigger immediate emergency compaction job:
     ```python
     dt = DeltaTable("_lakehouse/silver")
     dt.optimize.compact(target_size=256 * 1024 * 1024) # 256 MB target
     dt.optimize.z_order(["tenant_id", "model"])
     ```
  2. Run `dt.vacuum(retention_hours=168)` to clean tombstoned files once retention window passes.

### Failure Mode 2: Unredacted PII Leakage Landing in Bronze
* **Symptom:** Compliance audit detects raw credit card numbers or secret tokens in `_lakehouse/bronze`.
* **Detection:** Automated DLP scanner (e.g., Presidio / AWS Macie) raises PII alert on newly written Parquet files.
* **Playbook & Rollback:**
  1. Identify affected version range `[v_start, v_end]` via `dt.history()`.
  2. Execute atomic Delta `MERGE` or `DELETE` purge across affected partition:
     ```python
     dt.delete("raw_json LIKE '%credit_card%' OR raw_json LIKE '%api_key%'")
     ```
  3. Force snapshot expiry (`vacuum(retention_hours=0)`) and issue `remove_orphan_files` set difference sweep to purge physical Parquet files from disk within 30 minutes.

### Failure Mode 3: Metadata Replay Slowdown (Cold-Start Latency)
* **Symptom:** New DuckDB query engines take 15 seconds just to load the Delta table metadata.
* **Root Cause:** Missing `_last_checkpoint` file, forcing the engine to parse 50,000 JSON commit files.
* **Detection:** Engine initialization metric `delta_metadata_load_time_ms > 2000`.
* **Playbook & Rollback:**
  1. Force-generate a consolidated Parquet checkpoint:
     ```python
     dt.create_checkpoint()
     ```
  2. Verify `_delta_log/_last_checkpoint` exists and points to the latest checkpoint version.

---

## 5. Back-of-Envelope Cost Estimation (FinOps Math)

Target Cap: **≤ $5,000 / month**

### 1. Data Volume Math:
* **Raw Ingestion:** 1B requests/day × 5 KB = **5 TB / day raw**.
* **After Snappy/Zstd Compression (3.5× ratio):** 5 TB / 3.5 = **1.43 TB / day compressed**.

### 2. Storage Costs (AWS S3 US-East Rates):
* **Bronze Layer (7-day retention):**  
  `1.43 TB/day × 7 days = 10.01 TB` @ $0.023/GB-month = **$230.23 / month**
* **Silver Layer (30-day retention, 50% size after pruning raw prompt text):**  
  `0.71 TB/day × 30 days = 21.3 TB` @ $0.023/GB-month = **$489.90 / month**
* **Gold Aggregates Layer (365-day retention, highly aggregated):**  
  `5 GB/day × 365 days = 1.825 TB` @ $0.023/GB-month = **$41.98 / month**
* **Total Storage Cost:** **$762.11 / month**

### 3. S3 API Request Costs:
* **PUT/POST (Ingestion):** 11,574 req/sec grouped into 10-sec micro-batches = 8,640 PUTs/day = 259,200 PUTs/month @ $0.005 / 1,000 = **$1.30 / month**
* **GET Requests (With Compaction & Z-ORDER Pruning):** 50,000 dashboard queries/day × 4 files touched (due to Z-ORDER) = 200,000 GETs/day = 6,000,000 GETs/month @ $0.0004 / 1,000 = **$2.40 / month**

### 4. Compute Costs (Auto-Compaction & Analytics):
* **Compaction & ETL Workers:** 4 × `t4g.xlarge` spot instances (ARM-based Graviton3, 4 vCPU, 16 GB RAM) @ $0.0408/hr × 730 hrs = **$119.14 / month**
* **Query Engine (DuckDB / Trino Serverless):** **$1,500.00 / month**
* **Buffer & Contingency (20%):** **$476.99 / month**

### ── TOTAL MONTHLY ESTIMATE ──
$$762.11 \text{ (Storage)} + \$3.70 \text{ (API Requests)} + \$1,619.14 \text{ (Compute)} + \$476.99 \text{ (Buffer)} = \mathbf{\$2,861.94 / month}$$

✅ **Result:** **$2,861.94 / month ≤ $5,000 / month cap** (Leaves ~42% margin for growth).

---

## 6. One-Week MVP Rollout Plan

* **Day 1: Ingestion & PII Redactor Spike:** Implement inline PII redactor (regex + salted HMAC hash) and verify 10-second streaming appends to Bronze.
* **Day 2: Medallion Pipeline (Bronze → Silver → Gold):** Build DuckDB parsing SQL for Silver JSON extraction and Gold `(tenant_id, model)` metric rollups.
* **Day 3: Storage Optimization Setup:** Configure scheduled `dt.optimize.compact()` and `dt.optimize.z_order(["tenant_id"])` jobs; measure speedup and pruning ratio.
* **Day 4: FinOps & S3 Lifecycle Verification:** Set up S3 lifecycle expiration rules (7-day Bronze purge) and test `dt.vacuum()` byte reclamation.
* **Day 5: Failure Mode & Emergency Recovery Testing:** Simulate small-file explosion and unredacted PII leak; verify emergency compaction and Delta MERGE PII purge playbooks.
