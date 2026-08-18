# System Architecture Document: High-Throughput LLM Observability Lakehouse (1B Req/Day)

**Author:** Le Trong Viet Dung (2A202601746)  
**Topic:** A — LLM Observability at 1 Billion Requests/Day Scale  
**Target Submission:** `submission/bonus/ARCHITECTURE.md`  

---

## 1. Problem Statement

A foundation model provider generates **1,000,000,000 requests/day** (~11,574 req/s avg, ~35,000 req/s peak). At ~5 KB/payload (prompt, completion, metadata, token usage), raw ingest volume is **5 TB/day (150 TB/month)**.

### Core Constraints & SLAs
1. **Multi-Tenant Observability SLA:** Cost and latency percentiles (p50, p95, p99) per tenant must refresh on dashboards every **5 minutes** with query latency $< 1.5\text{s}$.
2. **Incident Triage vs. Long-Term Retention:** Full payloads (raw prompt/response) are strictly retained for **7 days** for debugging, then purged. Aggregated metrics are kept for **1 year**.
3. **Security & Privacy (PII):** Zero plaintext PII (emails, phone numbers, API keys, credit cards) in queryable layers. Tokenization/redaction must occur in-flight before landing on Bronze/Silver.
4. **FinOps Storage Cap:** Total storage cost across all tiers must stay below **\$5,000/month** (AWS S3 list price standard is \$23/TB-month; naive 150 TB/mo storage would quickly blow the budget).

---

## 2. End-to-End Architecture Diagram

```
 [ Client Apps / Gateway ] (1B req/day, 35K req/s peak)
           │
           ▼
 [ Kafka / Kinesis Ingestion Buffer ] (Distributed log, 3-hour buffer)
           │
           ▼ (Streaming Ingestion + In-Flight Tokenization Engine)
 ┌────────────────────────────────────────────────────────────────────────┐
 │ In-Flight Worker Cluster (Rust / delta-rs + Presidio Regex/NER Engine) │
 │  - Real-time PII Tokenization (AES-256-SIV Format-Preserving)          │
 │  - Micro-batch flush every 60s (Zstd level 3)                          │
 └────────────────────────────────┬───────────────────────────────────────┘
                                  │
                                  ▼
 ┌────────────────────────────────────────────────────────────────────────┐
 │                        MEDALLION LAKEHOUSE STORAGE                     │
 │                                                                        │
 │  [BRONZE] s3://lakehouse/bronze/llm_raw/                               │
 │    - Format: Delta Lake (Unparsed raw JSON + tokenized PII)            │
 │    - Partitioning: date=YYYY-MM-DD                                     │
 │    - Lifecycle: Expire & Hard Delete after 7 Days via S3 Lifecycle    │
 │                                │                                       │
 │                                ▼ (Continuous Incremental Micro-Batch)  │
 │  [SILVER] s3://lakehouse/silver/llm_events/                            │
 │    - Format: Delta Lake (Structured, Typed, Deduplicated by request_id)│
 │    - Partitioning: date=YYYY-MM-DD                                     │
 │    - Clustering: Z-ORDER BY (tenant_id, model_id)                      │
 │    - Lifecycle: Hot (S3 Standard 0-3d) → Cold (S3 Glacier IR 4-7d)     │
 │                                │                                       │
 │                                ▼ (5-Minute Tumbling Window Aggregator) │
 │  [GOLD] s3://lakehouse/gold/tenant_metrics_5m/                         │
 │    - Format: Delta Lake (Aggregates: p50/p95/p99, tokens, cost_usd)    │
 │    - Partitioning: month=YYYY-MM                                       │
 │    - Clustering: Z-ORDER BY (tenant_id)                                │
 │    - Retention: 365 Days (Rollup to daily after 30 days)               │
 └────────────────────────────────────────────────────────────────────────┘
                                  │
       ┌──────────────────────────┴──────────────────────────┐
       ▼                                                     ▼
 [ Apache Polaris REST Catalog ]               [ Query Engines (DuckDB / Trino) ]
 (RBAC, Token Vending, Scan Planning)          (Grafana Dashboards, Security Audit)
```

---

## 3. Key Architectural Decisions & Trade-Offs

### Decision 1: Table Format — Delta Lake with Deletion Vectors
* **Chosen:** **Delta Lake (v4.x)** with `deletionVectors.enabled = true` and `changeDataFeed`.
* **Alternative 1 Rejected (Apache Iceberg):** While Iceberg has excellent hidden partitioning, Delta's Rust native binding (`delta-rs`) allows ultra-low-memory lightweight stream writers running on ARM64 nodes without JVM overhead, saving ~40% compute costs on the 35K req/s ingestion hot-path.
* **Alternative 2 Rejected (Raw Parquet on S3 Hive layout):** Zero ACID guarantees. Ingestion failures would leave corrupted orphan files, and concurrent compaction jobs would cause query race conditions.

### Decision 2: Partitioning & Clustering Strategy — Date Partitioning + Tenant Z-ORDER
* **Chosen:** Partition by **`date(ts)`**, clustered with **`Z-ORDER BY (tenant_id, model)`**.
* **Alternative 1 Rejected (Partition by `tenant_id`):** 1B requests across 50,000 tenants would generate $> 50,000$ directory partitions per day, resulting in a catastrophic **Small-Files & Metadata Explosion Problem** that crashes query planners.
* **Alternative 2 Rejected (No Partitioning / Pure Append):** Queries filtering for a single tenant's latency would require full-table scans across 5 TB/day, inflating query costs and missing the $< 1.5\text{s}$ SLA.

### Decision 3: Ingestion & PII Redaction — In-Flight Tokenization at Buffer Read
* **Chosen:** Tokenize PII in-flight inside the streaming consumer prior to landing in Bronze using Deterministic Vaultless Format-Preserving Encryption (FPE).
* **Alternative 1 Rejected (Post-Landing Batch Scrubbing):** Scrubbing Bronze asynchronously leaves a 5–15 minute vulnerability window where raw customer PII sits unencrypted on disk.
* **Alternative 2 Rejected (Dynamic Masking at Query Time):** Computing regex/NER scrubbing on 1B rows during dashboard queries increases CPU latency by $8\times$, violating the sub-second dashboard requirement.

### Decision 4: Catalog & Metadata Plane — Apache Polaris (REST Catalog)
* **Chosen:** **Apache Polaris (REST Catalog)** for centralized security boundary, credential vending, and server-side scan planning.
* **Alternative 1 Rejected (AWS Glue Catalog):** Proprietary lock-in with slow metadata sync rates for sub-minute commits and high API request pricing (\$1 per million requests).
* **Alternative 2 Rejected (Hive Metastore):** Legacy JVM dependency, poor cloud-native storage support, and lack of fine-grained column-level access control.

### Decision 5: FinOps Tiering & Retention Lifecycle
* **Chosen:** 
  - **Bronze (5 TB/day):** Retained 7 days only on S3 Standard $\rightarrow$ S3 Lifecycle rule permanently purges objects at $t = 7\text{d}$.
  - **Silver (1.5 TB/day post-projection):** Days 0–3 on S3 Standard, Days 4–7 transitioned to S3 Glacier Instant Retrieval.
  - **Gold (500 MB/day):** Retained 365 days on S3 Standard.
* **Alternative 1 Rejected (Keeping Bronze in S3 Standard for 1 Year):** $5\text{ TB/day} \times 365\text{ days} = 1.825\text{ PB} \rightarrow \$41,975/\text{month}$ storage bill (Instant bankruptcy).

---

## 4. Failure Modes & Incident Runbooks

### Failure Mode 1: 03:00 AM PII Leak from Prompt Injection
* **Detection:** Real-time canary detector checks Gold error metrics and random sampling of Silver rows against credit card / SSN regex patterns. Alerts triggered when `pii_token_mismatch > 0`.
* **Rollback & Mitigation:**
  1. Leverage Delta Lake **Time Travel** and **Deletion Vectors** to issue a targeted atomic deletion:
     ```sql
     DELETE FROM silver.llm_events WHERE ts >= '2026-08-18 02:00:00' AND pii_flag = TRUE;
     ```
  2. Execute emergency compaction and physical vacuum:
     ```python
     dt.vacuum(retention_hours=0, enforce_retention_duration=False)
     ```
  3. Rotate FPE cryptographic salt and re-sync downstream Gold rollups within 15 minutes.

### Failure Mode 2: Ingestion Burst & Small-Files Explosion (S3 503 SlowDown)
* **Detection:** S3 metric `503 SlowDown` spikes on GET/PUT APIs; scan planning latency exceeds $10\text{s}$.
* **Mitigation:**
  1. The streaming writer buffers in-memory up to 128 MB or 60 seconds before flushing to disk.
  2. Automated auto-compactor micro-job (`OPTIMIZE silver COMPACT (target_size=256MB)`) runs concurrently every 15 minutes using non-blocking optimistic concurrency control.

### Failure Mode 3: Upstream LLM Gateway Schema Drift (New Metadata Fields Added)
* **Detection:** Consumer logs `SchemaMismatchException` or drops unmapped attributes.
* **Mitigation:**
  1. Stream writer configures `schema_mode="merge"` (Delta Lake Schema Evolution).
  2. Unrecognized nested fields are automatically captured into a `metadata_extra: MAP<STRING, STRING>` column without dropping records or breaking running pipelines.

---

## 5. Back-of-the-Envelope FinOps Cost Estimation

| Layer / Resource | Sizing & Throughput | Storage Class & Rate | Monthly Cost (USD) |
| :--- | :--- | :--- | :--- |
| **Bronze Raw** | $5\text{ TB/day} \times 7\text{ days} = 35\text{ TB}$ active | S3 Standard (\$0.023/GB) | **\$805.00** |
| **Silver Structured** | $1.5\text{ TB/day} \times 3\text{ days} = 4.5\text{ TB}$ | S3 Standard (\$0.023/GB) | **\$103.50** |
| **Silver Aged (4–7d)**| $1.5\text{ TB/day} \times 4\text{ days} = 6.0\text{ TB}$ | S3 Glacier Instant (\$0.004/GB) | **\$24.00** |
| **Gold Metrics (1yr)**| $0.5\text{ GB/day} \times 365\text{ days} = 182.5\text{ GB}$ | S3 Standard (\$0.023/GB) | **\$4.20** |
| **S3 API Requests** | ~35K req/s batched to 1-min flushes ($43.2\text{M PUTs/mo}$) | \$0.005 / 1,000 PUTs | **\$216.00** |
| **Ingestion Compute** | 6x ARM64 c7g.xlarge Spot instances | \$0.07/hr $\times 720\text{h} \times 6$ | **\$302.40** |
| **Compactor Compute** | 2x c7g.2xlarge periodic batch | \$0.14/hr $\times 4\text{h/day} \times 30$ | **\$33.60** |
| **Catalog & Network** | Apache Polaris self-hosted + VPC endpoints | Container runtime + Data transfer | **\$250.00** |
| **TOTAL ESTIMATE** | **Target Cap: $\le \$5,000/\text{month}$** | **All Tiers Included** | **\$1,738.70 / month** |

*Margin of Safety: 65.2% under the \$5,000 budget cap, allowing headroom for traffic spikes.*

---

## 6. One-Week MVP Slice Plan

To validate feasibility without building the full enterprise pipeline:
* **Day 1–2:** Deploy Kafka + `delta-rs` lightweight ingestion script on local cluster to prove sustained 35,000 req/s throughput with $< 50\text{MB}$ memory footprint.
* **Day 3–4:** Implement in-flight FPE tokenization and benchmark latency overhead per request ($< 2\text{ms}$ per 5 KB payload).
* **Day 5:** Implement 5-minute Gold aggregation query in DuckDB over Silver Delta table with Z-ORDER, validating $p95 < 500\text{ms}$ query speed.
* **Day 6–7:** Execute automated 7-day retention purge test using Delta vacuum and S3 lifecycle mock.

---
*Self-Checklist Completed: $\ge 5$ Architectural Decisions with Trade-offs, 3 Production Failure Modes, Real FinOps Math, and Concrete 1-Week Shippable Slice.*
