# Lakehouse Architecture Specification: LLM Observability at 1B Requests/Day

**Author:** Pham Quoc Minh  
**Role:** Staff Data Platform Architect  
**Scope:** Foundation-Model LLM Observability & Analytics Infrastructure  
**Target SLA:** 5-min Dashboard Freshness, p95 Query Latency < 1.5s, Storage Budget $\le \$5,000/\text{mo}$

---

## 1. Problem Statement

A tier-1 foundation-model platform logs every inference API call across thousands of enterprise tenants. Operating at **1 Billion requests/day** with an average serialized payload of **5 KB** produces **5 TB/day of raw telemetry** (~150 TB/month uncompressed).

The data platform must satisfy four non-negotiable operational requirements:
1. **Freshness & Speed:** Per-tenant cost, token usage, latency (p50, p95, p99), and error-rate metrics refreshed every **5 minutes** with p95 dashboard query latency under **1.5 seconds**.
2. **Lifecycle Tiering:** Retain high-fidelity full prompt/response payloads for **7 days** (incident triage & security forensics), while retaining compact analytical rollups for **365 days** (audit, capacity planning, and trend forecasting).
3. **Strict PII & Compliance Isolation:** Prompts and completions contain PII (emails, phone numbers, secret keys) that must be deterministically redacted/tokenized before analyst or LLM evaluation access.
4. **Hard FinOps Constraint:** Total lakehouse storage expenditure must remain **under \$5,000 / month**.

The core technical challenge is preventing the Small-Files Problem during continuous high-throughput streaming (11,574 req/s peak) while maintaining cheap point-lookups and granular data skipping without running up excessive storage or catalog API costs.

---

## 2. End-to-End Lakehouse Architecture Diagram

```
+---------------------------------------------------------------------------------------------------------------+
|                                      INGESTION & MEDALLION STORAGE PIPELINE                                   |
+---------------------------------------------------------------------------------------------------------------+
  [API Gateways & LLM Proxy Nodes] (1B req/day ~ 11.6K req/s peak)
                 │
                 ▼ (Protobuf / Async Batch Ingest)
       [Apache Kafka / Redpanda] (Topic: `llm.telemetry.v1`, 64 Partitions)
                 │
  ┌──────────────┴─────────────────────────────────────────────────────────────────────────────┐
  │ Micro-Batch Stream Ingestion (delta-rs / Structured Streaming Engine, 60s trigger window)   │
  └──────────────┬─────────────────────────────────────────────────────────────────────────────┘
                 ▼
+───────────────────────────────────────────────────────────────────────────────────────────────+
| BRONZE LAYER: Raw Event Log (`bronze_llm_calls_raw`)                                          |
| • Storage: S3 Standard, Delta Lake format, ZSTD Level 3 compression                           |
| • Schema: `request_id`, `ts`, `tenant_id`, `model`, `raw_json` (payload + metadata + headers) |
| • Partitioning: `date(ts)`                                                                    |
| • Retention: Hard 7-day S3 Lifecycle Expiration + Daily VACUUM                                |
+───────────────────────────────────────────────────────────────────────────────────────────────+
                 │
                 ▼ (Continuous Medallion Sanitization + Structured Extraction)
+───────────────────────────────────────────────────────────────────────────────────────────────+
| SILVER LAYER: Cleansed, Structured & Tokenized (`silver_llm_events`)                          |
| • Transformations:                                                                            |
|     - JSON flattening & schema validation with Schema Enforcement                             |
|     - Exact deduplication via `ROW_NUMBER() OVER (PARTITION BY request_id ORDER BY ts)`       |
|     - Inline HMAC-SHA256 Tokenization / Vault Redaction for PII in prompts/responses          |
| • Storage: Delta Lake with Liquid Clustering / Z-ORDER on `[tenant_id, model, status]`        |
| • Partitioning: `date(ts)`                                                                    |
| • Retention: 7-day retention for raw text, 30 days for metadata fields                        |
+───────────────────────────────────────────────────────────────────────────────────────────────+
                 │
                 ▼ (5-Minute Micro-Batch Aggregation Job)
+───────────────────────────────────────────────────────────────────────────────────────────────+
| GOLD LAYER: Multi-Tenant Analytical Marts (`gold_tenant_metrics_5m`, `gold_model_daily`)      |
| • Rollups: `tenant_id`, `model`, `window_5m`, p50/p95/p99 latency, prompt/completion tokens,  |
|            error_count, total_cost_usd (joined against Token Price Dimension Table)           |
| • Storage: S3 Standard, Delta Lake with Z-ORDER on `[tenant_id, window_5m]`                   |
| • Retention: 365-day regulatory & reporting retention                                         |
+───────────────────────────────────────────────────────────────────────────────────────────────+
                 │
                 ├──────────────────────────────────────┬───────────────────────────────────────┐
                 ▼                                      ▼                                       ▼
    [DuckDB In-Memory / WASM]                  [Trino / Starburst]                     [Automated Maintenance]
  - Real-time tenant portal dashboards       - Ad-hoc incident forensic queries      - Hourly Auto-Compaction
  - Latency p95 < 200 ms via Arrow           - Cross-tenant security audits          - Daily `VACUUM` (7-day safety)
  - Zero network scan on cached metadata     - Point queries filtered by tenant      - Checkpoint Parquet creation
```

---

## 3. Key Architectural Decisions & Rejected Alternatives

### Decision 1: Storage Format — Delta Lake (delta-rs + Apache Spark)
* **Chosen:** **Delta Lake 3.x with delta-rs engine binding.** Delta Lake provides transactional ACID guarantees, native Change Data Feed (CDF), fast compaction (`dt.optimize.compact()`), multi-dimensional clustering (`z_order`), and lightweight Rust-based bindings for streaming workers without JVM overhead.
* **Rejected Alternative A (Raw Parquet on S3):** Lacks ACID transactions and atomicity. Concurrent writes during micro-batching produce dirty reads and broken state. Does not support metadata-level data skipping (`minValues`/`maxValues` stats) or schema evolution.
* **Rejected Alternative B (Apache Hudi):** Higher operational complexity (complex MOR/COW merge tuning, heavy timeline metadata overhead, and weaker zero-copy Arrow integrations for embedded analytics engines like DuckDB).

### Decision 2: Partitioning & Clustering Strategy — Hidden Date Partitioning + Multi-Column Z-Order
* **Chosen:** **Partition by `date(ts)` combined with Z-ORDER on `[tenant_id, model, status]`.**
* **Rejected Alternative A (Hierarchical Hive Partitioning `year/month/day/tenant_id`):** Multiplied partition directory explosion ($365 \times 5,000\text{ tenants} = 1.82\text{M partitions}$). This creates the classic Lakehouse Small-Files Anti-Pattern, saturates catalog metadata lookups, and degrades S3 GET performance.
* **Rejected Alternative B (Unclustered Flat Storage):** Forces full-table scans across 5 TB/day of data for tenant-specific lookups, causing query costs and dashboard latency to surge from 100ms to > 45 seconds.

### Decision 3: Compression & Page Encoding — Zstandard (ZSTD Level 3) + Dictionary Encoding
* **Chosen:** **Zstandard (ZSTD Level 3) with Parquet Dictionary & RLE Encoding.** Text-heavy JSON payloads and repetitive LLM metadata fields achieve a $3.5\times$ compression ratio ($5\text{ TB/day} \rightarrow 1.43\text{ TB/day}$) while maintaining high decompression throughput ($> 1.2\text{ GB/s}$ per core).
* **Rejected Alternative A (Snappy):** Only achieves $\sim 2.1\times$ compression on unstructured JSON logs, inflating monthly S3 storage by $40\%$ ($\sim \$600/\text{month}$ excess spend).
* **Rejected Alternative B (GZIP):** Yields slightly higher compression ($3.7\times$) but decompression is $4\times$ slower, failing the p95 $< 1.5$s query SLA.

### Decision 4: Lifecycle & FinOps Tiering — 7-Day Sliding Window with Auto-Compacted Gold Rollups
* **Chosen:** **Bronze & Silver 7-day hard retention with S3 Lifecycle rule `Expiration = 7 days` coupled with Delta `VACUUM`. Gold aggregated rollups retained for 365 days on S3 Standard.**
* **Rejected Alternative A (Store all Bronze logs for 365 days on S3 Standard):** $150\text{ TB/mo} \times 12\text{ mo} = 1.8\text{ PB}$ storage $\approx \$41,400/\text{month}$, exceeding the $\$5,000/\text{month}$ budget by $800\%$.
* **Rejected Alternative B (Glacier Deep Archive tiering of raw Delta directories):** Violates Lakehouse metadata consistency. When files in `_delta_log/` or underlying Parquet parts are asynchronously moved to offline archive tiers, query engines encounter `ObjectNotAvailable` errors upon transaction replay.

### Decision 5: Catalog & Governance Control Plane — Apache Polaris (REST Catalog) + Tag-Based PII Masking
* **Chosen:** **Apache Polaris (Open-Source Apache Iceberg / Delta REST Catalog) with unified RBAC.** Prompts and completions undergo tokenization during Silver ingestion (HMAC-SHA256 with key rotated in AWS KMS). Only cryptographically authorized auditing agents receive unmasked hashes.
* **Rejected Alternative A (AWS Glue / Hive Metastore):** Hive Metastore struggles under millions of partition/file metadata sync operations per day. Proprietary catalog lock-in restricts cross-engine execution between DuckDB, Trino, and Polars.
* **Rejected Alternative B (Plaintext Storage with Application-Level Authorization):** Vulnerable to data leakage. Any direct S3 reader or rogue lakehouse connection bypasses application checks and exposes customer PII.

---

## 4. Failure Modes, Incident Detection & 3-AM Playbooks

### Failure Mode 1: Upstream JSON Schema Drift & Malformed Payload Ingestion
* **Scenario:** Upstream LLM gateway deploys an update adding unexpected nested structures or changing `latency_ms` from `int` to `string`.
* **Detection:** Silver ingestion job crashes with Delta `SchemaEnforcementException`; Dead-Letter Queue (DLQ) alert triggers on PagerDuty when DLQ message rate $> 0.1\%$.
* **Mitigation & Rollback:** 
  1. Bronze ingestion continues uninterrupted because Bronze stores `raw_json: String` (Schema-on-Read preservation).
  2. For benign additive fields, trigger opt-in schema evolution via `schema_mode="merge"`.
  3. For corrupted data written to Silver, execute Time-Travel Rollback to the pre-incident commit version: `dt.restore(clean_version)`.
  4. Reprocess the affected Bronze interval with the updated parsing logic.

### Failure Mode 2: Small-Files Explosion during High-Frequency Ingestion
* **Scenario:** Sudden burst in traffic generates 50,000 10-KB Parquet files within 3 hours. Analytical queries slow down by $15\times$, and S3 list API latency spikes.
* **Detection:** Prometheus metrics track `num_files_per_partition > 200` or average file size $< 16\text{ MB}$.
* **Mitigation & Rollback:**
  1. Run automated Delta Compaction Job: `dt.optimize.compact(target_size=256*1024*1024)`.
  2. Re-cluster active partitions with `dt.optimize.z_order(["tenant_id", "model"])`.
  3. Immediately reclaim tombstoned Parquet files via `dt.vacuum(retention_hours=0)` to avoid paying duplicate storage charges.

### Failure Mode 3: Late-Arriving Events & Split-Brain Ingestion Duplication
* **Scenario:** Edge proxy disconnection causes 12 hours of cached LLM telemetry to arrive in a single massive burst, intermixed with current live traffic.
* **Detection:** Silver deduplication monitor detects duplicate `request_id` count $> 5,000/\text{min}$.
* **Mitigation & Rollback:**
  1. Execute Delta ACID `MERGE INTO` with idempotency predicate:
     ```sql
     MERGE INTO silver_llm_events AS target
     USING staging_deduped AS source
     ON target.request_id = source.request_id AND target.date = source.date
     WHEN MATCHED AND source.ts > target.ts THEN UPDATE SET *
     WHEN NOT MATCHED THEN INSERT *
     ```
  2. Re-run Gold 5-minute aggregation rollups for the affected historical time range using Delta Time-Travel audit trail.

---

## 5. Cost Back-of-Envelope Math (FinOps Validation)

### A. Raw Storage Calculations (per 30-Day Month)
* **Daily Ingestion:** $1\times 10^9 \text{ req} \times 5\text{ KB} = 5.0\text{ TB/day raw}$.
* **ZSTD Compression Ratio ($3.5\times$):** $5.0 / 3.5 \approx 1.43\text{ TB/day on disk}$.

| Layer | Retention Policy | Active Data Volume | S3 Unit Rate | Monthly Cost |
|---|---|---|---|---|
| **Bronze (Raw Delta)** | 7-Day Rolling Expiration | $7 \text{ days} \times 1.43\text{ TB} = 10.01\text{ TB}$ | \$0.023 / GB-mo | **\$230.23** |
| **Silver (Cleansed Delta)** | 7-Day Full + 30-Day Metadata | $7 \text{ days} \times 0.95\text{ TB} + 23 \text{ days} \times 0.15\text{ TB} = 10.10\text{ TB}$ | \$0.023 / GB-mo | **\$232.30** |
| **Gold (Aggregated 5m)** | 365-Day Retention | $365 \text{ days} \times 0.04\text{ TB/day} = 14.60\text{ TB}$ | \$0.023 / GB-mo | **\$335.80** |
| **Delta Log Metadata & Checkpoints** | Cleaned via VACUUM / Checkpoints | $0.25\text{ TB}$ total | \$0.023 / GB-mo | **\$5.75** |
| **Total Storage Spend** | | **$34.96\text{ TB}$ average active** | | **\$804.08 / mo** |

### B. Compute & Operational Costs
* **Streaming Ingestion Workers (delta-rs / Rust on Graviton c7g.xlarge):** 4 instances $\times \$0.145/\text{hr} \times 730\text{ hrs} \approx \$423.40/\text{mo}$.
* **Hourly Compaction & Z-Order Spark Jobs (Spot Instances):** \$280.00/mo.
* **Ad-hoc Query & Dashboard Engines (DuckDB + Trino Spot Fleet):** \$950.00/mo.
* **S3 API Requests (PUT/GET/LIST):** $1\text{B writes} / 10,000 \text{ batch size} = 100,000 \text{ PUTs/day} \times 30 = 3\text{M PUTs} \approx \$15.00/\text{mo}$.
* **Total System Monthly Spend:** **\$2,472.48 / month** (Safety Margin: **50.5% under the \$5,000 budget**).

---

## 6. One-Week MVP Slice (Implementation Roadmap)

The objective of the one-week MVP is not to deploy the entire production infrastructure, but to validate the two hardest architectural assumptions: (1) streaming throughput with zero-loss PII tokenization, and (2) sub-second multi-tenant querying via DuckDB on Delta Lake.

* **Day 1–2 (Streaming Landing & Schema Validation):**
  - Implement the `delta-rs` micro-batch writer in Python/Rust consuming mock JSON payloads.
  - Establish Bronze landing table with `date(ts)` partitioning and verify Schema Enforcement blocks corrupt payloads.
* **Day 3–4 (Silver Transformation, Deduplication & Z-ORDER):**
  - Build the Silver ETL with windowed deduplication and HMAC-SHA256 PII tokenization.
  - Implement auto-compaction and Z-ORDER on `[tenant_id, model]`.
  - Validate that file count drops $> 10\times$ and file skipping reduces query scan volume by $> 80\%$.
* **Day 5 (Gold Mart & Dashboard Interface):**
  - Implement Gold 5-minute rollup table computing p50/p95 latency, token counts, error rate, and dollar cost.
  - Register Gold Delta tables with DuckDB via zero-copy PyArrow and execute tenant dashboard queries.
* **Day 6–7 (Resilience, Time-Travel & FinOps Verification):**
  - Run failure injection test: schema mutation, bad data rollback via `dt.restore()`, and orphan cleanup verification via `VACUUM`.
  - Profile memory usage and confirm monthly extrapolated cost $< \$5,000$.
