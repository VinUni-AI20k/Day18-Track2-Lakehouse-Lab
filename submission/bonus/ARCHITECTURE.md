# Architecture Decision Record: High-Scale LLM Observability Lakehouse (1B req/day)

**Author:** Pham Quoc Thanh (2A202601407)  
**System Scope:** Foundation Model Telemetry & Observability Pipeline  
**Scale:** 1,000,000,000 requests/day (~5 KB/req raw → ~5 TB/day raw, ~150 TB/month)  
**Cost Ceiling:** ≤ $5,000 / month total storage budget  

---

## 1. Executive Summary & Core Constraints

| Metric / Requirement | Target SLA / Budget | Architectural Solution |
|---|---|---|
| **Ingestion Throughput** | 1B req/day (~11,570 req/s avg, 30K req/s peak) | Distributed Kafka / Kinesis buffer → Flink streaming micro-batch writer |
| **P50/P95 Latency & Cost Dashboard** | 5-minute refresh SLA by `tenant_id` | Streaming Silver→Gold aggregate pipeline; Gold partitioned by `date`, Z-ordered by `tenant_id` |
| **Full Payload Retention** | 7-day raw prompt/response retention | S3 Standard (7 days) → S3 Glacier Flexible (23 days) → Lifecycle Expiration (30 days) |
| **Long-Term Aggregates** | 1-year historical analytics retention | Gold summary tables stored in Apache Iceberg / Delta Lake with ZSTD compression |
| **Privacy & Security** | Zero unredacted PII exposed to analysts | In-stream PII Presidio tokenization & salt vault at Bronze landing boundary |
| **Storage Cost Target** | ≤ $5,000 / month | FinOps tiering + Parquet dictionary/ZSTD encoding achieves **~$3,180 / month** |

---

## 2. Medallion Storage Architecture & Lifecycle

```
[API Gateways / Proxies]
       │
       ▼ (Kafka / Redpanda: 100 partitions)
┌─────────────────────────────────────────────────────────────┐
│ Bronze Ingestion Layer (Raw Lakehouse)                      │
│ - In-stream deterministic PII Tokenization & Presidio Hash  │
│ - Schema: (request_id, ts, tenant_id, model, raw_payload)   │
│ - Storage: S3 Standard (ZSTD compressed Parquet)            │
│ - Retention: 7-day rolling window -> Lifecycle Expiry       │
└──────────────────────────────┬──────────────────────────────┘
                               │
                Structured Streaming / Flink
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Silver Layer (Cleaned & Enriched Telemetry)                 │
│ - Deduplication by request_id (watermarked 2-hour window)   │
│ - Parsed usage: prompt_tokens, completion_tokens, latency_ms│
│ - Storage: Partitioned by date, Z-ORDER by (tenant_id, ts)  │
│ - Compaction: Hourly bin-packing to 128MB target file size  │
│ - Retention: 30 days total (7 days Hot -> 23 days Warm)     │
└──────────────────────────────┬──────────────────────────────┘
                               │
                Micro-batch 5-minute Rollup
                               │
                               ▼
┌─────────────────────────────────────────────────────────────┐
│ Gold Layer (Aggregates & FinOps Reporting)                  │
│ - Rollups: 5-min & daily p50/p90/p99 latency, cost, errors │
│ - Dimensions: (tenant_id, model_name, status_code, date)    │
│ - Format: Apache Iceberg / Delta Lake with field-ID mapping │
│ - Query Engine: Trino / DuckDB / StarRocks (sub-second p95) │
│ - Retention: 365 days                                       │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Storage FinOps & Cost Modeling

### 3.1 Raw Ingestion & Compression Math
- **Raw Volume:** $10^9 \text{ req} \times 5 \text{ KB} = 5.0 \text{ TB/day}$.
- **Parquet + ZSTD Compression Ratio:** ~3.5× for structured JSON/tokens → **1.43 TB/day written**.
- **7-day Hot Bronze/Silver Working Set:** $7 \times 1.43 \text{ TB} = 10.01 \text{ TB}$.
- **23-day Warm Silver Set (Glacier Instant Retrieval):** $23 \times 1.43 \text{ TB} = 32.89 \text{ TB}$.
- **365-day Gold Aggregates:** ~15 MB/day rollup $\times 365 = 5.5 \text{ GB}$.

### 3.2 Monthly Cost Breakdown (AWS us-east-1 Pricing)

| Tier | Active Volume | Rate / GB / mo | Monthly Cost |
|---|---|---|---|
| **S3 Standard (Bronze + Hot Silver, 7 days)** | 10,010 GB | $0.023 | $230.23 |
| **S3 Glacier Instant Retrieval (Warm Silver, 23 days)** | 32,890 GB | $0.004 | $131.56 |
| **S3 Glacier Flexible Archive (Auditing Archive, 60 days)** | 85,800 GB | $0.0036 | $308.88 |
| **Gold Layer (1 Year Aggregates)** | 5.5 GB | $0.023 | $0.13 |
| **PUT / GET / Lifecycle Transition Requests** | ~1.5B API operations | Tiered batched | $2,240.00 |
| **Metadata Catalog / Glue & Maintenance** | Iceberg metadata & compaction | Flat rate | $270.00 |
| **Total Monthly Storage Cost** | — | — | **$3,180.80** |

> **Result:** Fits well within the **$5,000/month** budget with a 36% safety margin.

---

## 4. Technical Trade-Offs & Format Evaluation

### Why Iceberg over Delta for the Catalog Layer?
1. **Hidden Partitioning:** Filtering by `ts` automatically prunes `date(ts)` partitions without requiring users to supply redundant filter predicates (`date = '...' AND ts >= '...'`).
2. **Field-ID Stability:** Enables evolving schemas (e.g. adding new token pricing breakdowns or renaming models) without rewriting historical Parquet data.
3. **Pointers-Only Snapshot Isolation:** Multi-engine readers (Trino, DuckDB, PySpark) maintain consistent reads during high-frequency concurrent 5-minute Gold commits.

### Maintenance Schedule
- **Compaction Daemon:** Runs every 60 minutes, bin-packing micro-batch files (< 10MB) into optimal 128MB Parquet files.
- **Multidimensional Clustering:** Nightly Z-ORDER by `tenant_id` and `timestamp` ensures 90%+ file skipping on tenant-specific dashboard queries.
- **Orphan File Sweeper:** Weekly reconciliation comparing physical S3 object listings against active Iceberg manifest lists to prevent stranded multipart upload drift.
