# Architecture Design Brief: LLM Observability Lakehouse at 1B Requests/Day Scale

**System:** Enterprise Multi-Tenant LLM Observability & Auditing Platform  
**Target Scale:** 1 Billion Requests/Day (~5.0 TB raw/day)  
**Budget Cap:** Total Storage & Lifecycle Operations ≤ $5,000 / month  
**Compliance & SLA:** Decree 13 & GDPR PII Redaction at landing, 5-minute metric dashboard refresh, p95 ad-hoc tenant query < 1.5s, 7-day full payload retention with 1-year aggregate rollup.

---

## 1. Problem Statement

At 1,000,000,000 requests/day with an average payload of 5 KB per invocation (prompt, completion, tool calls, token usage, latency, tenant metadata), the system ingests **5.0 TB of uncompressed JSON raw traces daily (150 TB/month)**. 

The architecture must simultaneously satisfy four conflicting constraints:
1. **Ultra-Low FinOps Footprint:** Total storage expenditure must remain under **$5,000/month** despite accumulating ~1.8 PB gross yearly data.
2. **Strict Data Privacy:** End-to-end PII tokenization and masking must occur deterministically at the Bronze landing stage before analyst or debugger read access.
3. **High-Performance Multi-Tenant Analytics:** Real-time cost, token burn, and latency dashboards per tenant require 5-minute freshness with sub-second query latency.
4. **Reproducible Incident Auditing:** Full prompt/response debugging traces must be accessible for 7 days with point-in-time time travel, while rolling up into long-term Gold metric aggregates for 365 days.

---

## 2. End-to-End Architecture Diagram

```
                                  INGESTION & MEDALLION PIPELINE
                                  
  [1B API Req/Day] ──> [Kafka / Event Hubs] (Streaming Ingestion: 12,000 msg/sec avg, 35,000 peak)
                               │
                               ▼ (Micro-batches: 60s trigger)
                 ┌─────────────────────────────┐
                 │  Bronze Ingest Engine       │ ──> Deterministic HMAC-SHA256 Tokenizer (PII)
                 └─────────────┬───────────────┘
                               │ (Append-only snappy.parquet)
                               ▼
     ========================================================================
     BRONZE LAYER: `bronze.llm_raw_traces` (Raw + PII Sanitized)
     - Partition: `date=YYYY-MM-DD`
     - Storage Class: S3 Standard / GCS Standard
     - Lifecycle: Transition to S3 Glacier Instant Retrieval at Day 4 -> Purge at Day 8
     ========================================================================
                               │
                               ▼ (Continuous Spark / delta-rs Streaming Transform)
     ========================================================================
     SILVER LAYER: `silver.llm_invocations` (Cleaned, Typed, Enriched, Deduped)
     - Partition: `date=YYYY-MM-DD` | Clustering / Z-Order: `(tenant_id, model_name)`
     - Format: Delta Lake / Iceberg v2 with Deletion Vectors
     - Lifecycle: Retain 30 days -> Automated Vacuum & Snapshots Expiry
     ========================================================================
               │                                               │
               ▼ (5-min Micro-Batch Aggregations)             ▼ (Ad-hoc Debug / Time Travel)
     =========================================      ======================================
     GOLD LAYER: `gold.tenant_metrics_5min`         ANALYTICS & AD-HOC INCIDENT AUDIT
     - Dimensions: `(tenant_id, model, date)`       - Trino / DuckDB / StarRocks Engine
     - Metrics: p50/p95/p99 latency, token_cost,    - Zero-copy Arrow projection
       error_rate, token_in, token_out              - Stats-based file pruning (Z-Order)
     - Retention: 365 days (S3 Infrequent Access)   - Result: p95 latency < 1.2s
     =========================================      ======================================
```

---

## 3. Key Architectural Decisions & Discarded Alternatives

### Decision 1: Table Format — Delta Lake with Deletion Vectors & Z-Order Clustering
* **Chosen:** **Delta Lake 3.x** using Z-Order on `(tenant_id, model_name)` and deletion vectors enabled.
* **Alternative Discarded 1 — Apache Iceberg v2 with Merge-on-Read:** While Iceberg has superior catalog independence, Delta Lake's native Z-Order spatial clustering and rust-native streaming engine (`delta-rs`) allow lightweight, sub-second micro-compactors without spinning up heavy JVM clusters every 5 minutes.
* **Alternative Discarded 2 — Raw Parquet on Hive Metastore:** Rejected because lack of ACID transactions causes dirty reads during concurrent 5-minute dashboard queries and makes time-travel auditing impossible.

### Decision 2: Storage Tiering & Lifecycle Management
* **Chosen:** **3-Tier S3 Lifecycle with Automated Expiry Schedule**:
  - Days 1–3: S3 Standard ($0.023/GB) for hot streaming ingest, compaction, and active debugging.
  - Days 4–7: S3 Glacier Instant Retrieval ($0.004/GB) for full payload review with millisecond retrieval.
  - Day 8+: Hard delete Bronze/Silver payloads via Delta `VACUUM` + orphan sweep; retain Gold rollup tables in S3 Standard-IA ($0.0125/GB) for 365 days.
* **Alternative Discarded 1 — Keeping all data in S3 Standard for 30 days:** Would consume 150 TB × $0.023 = $3,450/month in raw storage alone, leaving zero budget headroom for compute and index maintenance.
* **Alternative Discarded 2 — Storing raw logs directly in OpenSearch / Elasticsearch:** Storing 5 TB/day in OpenSearch clusters requires hot SSDs and dedicated nodes costing >$18,000/month.

### Decision 3: Compaction & Clustering Strategy (Combating Small Files)
* **Chosen:** **Two-Stage Compaction Cadence**:
  1. *Micro-compaction (Every 10 min):* Merges 60-second raw append files into 32 MB intermediate batches.
  2. *Daily Bin-packing & Z-Order (Every 24h at 02:00 UTC):* Re-clusters the finalized date partition into 256 MB Parquet files ordered by `(tenant_id, timestamp)`.
* **Alternative Discarded 1 — Synchronous Large File Ingestion:** Buffering 256 MB in memory before committing increases end-to-end data latency to ~45 minutes, violating the 5-minute dashboard SLA.
* **Alternative Discarded 2 — Unmanaged Append without Compaction:** Ingesting 1,440 files/day per stream generates >200,000 small files monthly, deteriorating query planning and inflating S3 PUT/GET API costs.

### Decision 4: In-Stream PII Tokenization at Bronze Boundary
* **Chosen:** **HMAC-SHA256 Salted Surrogate Tokenization** embedded directly inside the streaming micro-batch reader.
* **Alternative Discarded 1 — Post-hoc PII Scrubbing in Silver Layer:** Writing raw PII to Bronze disk and attempting to clean it downstream leaves sensitive credentials in raw immutable Parquet files, violating GDPR Article 17 ("Right to Erasure") and Decree 13.
* **Alternative Discarded 2 — External KMS Call per Row:** Calling AWS KMS / Cloud KMS per API request creates 1B API calls/day ($3,000/day in KMS fees). Salted local key rotation in memory incurs $0 additional cost.

### Decision 5: Gold Aggregation Engine — Materialized Pre-aggregations
* **Chosen:** **Incremental State Streaming into Gold Delta Table** with 5-minute bucketing containing summary metrics (`count`, `sum_tokens_in`, `sum_tokens_out`, `t-digest sketch` for percentiles p50/p95/p99).
* **Alternative Discarded 1 — Ad-hoc Query Scanning on Silver:** Running analytical queries directly across 150 TB of Silver invocations to render executive dashboards would cost >$400/day in query scan fees.

---

## 4. Failure Modes & Resilience (The 3:00 AM Runbook)

### Failure Mode 1: Streaming Ingestion Worker Crash Leaves Uncommitted Orphan Parquet Files
* **Root Cause:** A batch worker dies midway through writing 200 MB of Parquet chunks to S3 before committing transaction log JSON `000000000000000X.json`.
* **Detection:** Daily FinOps audit alerts that total S3 bucket volume exceeds `DeltaTable.file_uris()` sum by > 2%.
* **Rollback & Fix:** Delta's native `VACUUM` ignores uncommitted files because they lack commit tombstones. We execute our custom **Job 4 Orphan Cleaner** (`differential set sweep: S3 ListObjectsV2 - TransactionLog.active_files - InFlight_Locks`) to safely purge uncommitted artifacts older than 6 hours.

### Failure Mode 2: Schema Drift (Upstream LLM Provider Adds Unannounced JSON Attributes)
* **Root Cause:** An upstream provider introduces a nested dictionary in `tool_calls.arguments` breaking the strict Arrow schema parser.
* **Detection:** Ingestion dead-letter queue (DLQ) triggers a PagerDuty alert when parse error rate exceeds 0.01%.
* **Rollback & Fix:** The ingestion engine leverages `schema_mode="merge"` with explicit JSON serialization for arbitrary nested metadata into a structured `extra_attributes: STRING` column, preserving field evolution without crashing the main pipeline.

### Failure Mode 3: Silent Metric Data Corruption via Late-Arriving Streaming Batches
* **Root Cause:** Offline edge gateway flushes 12-hour delayed telemetry, causing duplicate or out-of-order writes in current date partitions.
* **Detection:** Automated data quality assertion detects `count(distinct request_id) < total_rows` in Silver layer.
* **Rollback & Fix:** Execute idempotent `MERGE INTO silver.llm_invocations USING incoming_batch ON target.request_id = source.request_id WHEN MATCHED AND source.timestamp > target.timestamp THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *`. Delta Time-Travel allows instant fallback to version `N-1` via `RESTORE TABLE TO VERSION_AS_OF`.

---

## 5. Back-of-the-Envelope Cost Estimation (Show the Math)

### A. Storage Cost Calculation (Target: ≤ $5,000 / month)
1. **Raw Bronze Ingestion (7-day Rolling Retention with Snappy Compression):**
   - Daily volume after 65% Snappy compression: $5.0\text{ TB} \times 0.35 = 1.75\text{ TB/day}$.
   - 3 Days in S3 Standard: $1.75\text{ TB} \times 3 = 5.25\text{ TB} \times \$0.023/\text{GB} = \$120.75/\text{month}$.
   - 4 Days in S3 Glacier Instant Retrieval: $1.75\text{ TB} \times 4 = 7.00\text{ TB} \times \$0.004/\text{GB} = \$28.00/\text{month}$.
2. **Silver Curated Invocations (30-day Retention, Z-Ordered ZSTD Compression):**
   - Daily volume after projection & ZSTD: $1.2\text{ TB/day}$.
   - Total 30-day storage: $36\text{ TB} \times \$0.023/\text{GB} = \$828.00/\text{month}$.
3. **Gold Analytical Layer (365-day Rollup Retention, Aggregated):**
   - Gold daily footprint: $50\text{ MB/day} \times 365 = 18.25\text{ GB} \times \$0.0125/\text{GB} \approx \$0.23/\text{month}$.
4. **S3 API Operations (PUT / GET / Lifecycle Transitions):**
   - Compacted batch writes (every 10 min): $144\text{ writes/day} \times 30 = 4,320\text{ PUTs} \approx \$0.02$.
   - Ad-hoc queries + dashboard GET requests: ~10,000,000 GETs/month $\times \$0.0004/1000 = \$4.00$.

$$\mathbf{Total\ Storage\ Cost} = \$120.75 + \$28.00 + \$828.00 + \$0.23 + \$4.02 \approx \mathbf{\$981.00 / month}$$

### B. Compute Cost Calculation (Dataproc Serverless / Spot Compute)
- Streaming Micro-batching (8 vCPU / 32 GB RAM spot instance): $0.08/\text{hr} \times 730\text{ hrs} = \$58.40$.
- Daily 256 MB Bin-packing & Z-Order Maintenance (2 hrs daily on 4 nodes): $2\text{ hrs} \times 30 \times \$0.32/\text{hr} = \$19.20$.
- Trino / DuckDB Query Cluster for Dashboards: 2 Spot instances $\times \$0.15/\text{hr} \times 730 = \$219.00$.

$$\mathbf{Total\ Compute\ Cost} \approx \mathbf{\$296.60 / month}$$
$$\mathbf{Grand\ Total\ System\ TCO} = \$981.00 + \$296.60 = \mathbf{\$1,277.60 / month} \quad (\ll \mathbf{\$5,000\ Cap!})$$

---

## 6. One-Week MVP Implementation Slice

| Day | Target Deliverable & Validation Gate |
|---|---|
| **Day 1** | Ingestion pipeline with HMAC-SHA256 PII redactor streaming into `bronze.llm_raw_traces`. |
| **Day 2** | Silver medallion pipeline with schema validation and `schema_mode="merge"` support. |
| **Day 3** | Automated 10-minute micro-compaction and daily Z-Order on `(tenant_id, model_name)`. |
| **Day 4** | 5-minute Gold rollup table generator computing p50/p95 latency and token cost summaries. |
| **Day 5** | 4-Job Maintenance Suite integration: Snapshot Expiry, Orphan file cleaner, and Delta `VACUUM`. |
| **Day 6** | End-to-end benchmark proving p95 query latency < 1.5s on 1B simulated records and verifying storage TCO math. |
