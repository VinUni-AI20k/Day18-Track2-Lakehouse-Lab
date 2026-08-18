# Storage Layout & Schema Design Contracts
**Version 1.0.0** · Design Specification · HADS Compliant

---

## AI READING INSTRUCTION

Read `[SPEC]` blocks for exact table schemas, data types, and partition keys.
Read `[NOTE]` blocks for storage sizing estimations and design rationale.

---

## 1. Directory Structure

**[SPEC]**
```
_lakehouse/
├── bronze/
│   ├── llm_calls_raw/             # Delta: Raw API requests (unredacted/unparsed JSON)
│   ├── agent_traces/              # Delta: Multi-agent execution step telemetry
│   └── docs_multimodal/           # Delta: Multimodal metadata + vector embeddings
├── silver/
│   ├── llm_calls/                 # Delta: Deduplicated, typed, partitioned by date
│   ├── training_corpus_governed/  # Delta: EU AI Act Art. 10 partitioned training sets
│   └── agent_trajectories/        # Delta: Policy-versioned agent execution chains
├── gold/
│   ├── llm_daily_metrics/         # Delta: Daily & 5-min p50/p95/cost aggregates
│   └── agent_performance/         # Delta: Tool accuracy, latency, and failure rates
├── iceberg/
│   ├── nb5/catalog.db             # SQLite REST-compatible Iceberg catalog
│   ├── nb6/catalog.db             # Maintenance verification catalog
│   └── nb8/catalog.db             # Model provenance & governance catalog
└── blobs/                         # External uncompressed multi-modal raw assets
```

---

## 2. Table Schema Contracts

### 2.1 Bronze Layer: `llm_calls_raw`
**[SPEC]**

| Column | Type | Nullable | Description |
|---|---|---|---|
| `request_id` | `VARCHAR` | No | UUIDv4 idempotency key |
| `ts` | `TIMESTAMP` | No | Request ingestion timestamp (UTC) |
| `raw_json` | `VARCHAR` | Yes | Raw stringified API request/response JSON |

---

### 2.2 Silver Layer: `llm_calls`
**[SPEC]**

| Column | Type | Partition Key | Description |
|---|---|---|---|
| `request_id` | `VARCHAR` | No | Unique deduplicated request ID |
| `ts` | `TIMESTAMP` | No | Ingestion timestamp |
| `model` | `VARCHAR` | No | Model name (`claude-sonnet-4-6`, `gpt-4o`, etc.) |
| `prompt_tokens` | `INT32` | No | Total input tokens |
| `completion_tokens` | `INT32`| No | Total output tokens |
| `latency_ms` | `FLOAT64`| No | End-to-end execution latency in ms |
| `cost_usd` | `FLOAT64`| No | Estimated monetary cost |
| `status_code` | `INT32` | No | HTTP status code (200, 500, etc.) |
| `date` | `VARCHAR` | **Yes** | Partition date in `YYYY-MM-DD` format |

---

### 2.3 Gold Layer: `llm_daily_metrics`
**[SPEC]**

| Column | Type | Partition Key | Description |
|---|---|---|---|
| `date` | `VARCHAR` | **Yes** | Aggregation date (`YYYY-MM-DD`) |
| `model` | `VARCHAR` | No | Model identifier |
| `total_calls` | `INT64` | No | Total executed calls |
| `p50_latency_ms`| `FLOAT64`| No | 50th percentile latency |
| `p95_latency_ms`| `FLOAT64`| No | 95th percentile latency |
| `total_cost_usd`| `FLOAT64`| No | Daily cumulative cost |
| `error_rate` | `FLOAT64`| No | Proportion of failed requests |

---

### 2.4 Multimodal Vectors: `docs_multimodal`
**[SPEC]**

| Column | Type | Storage Mode | Description |
|---|---|---|---|
| `doc_id` | `VARCHAR` | Columnar Parquet | Unique document identifier |
| `title` | `VARCHAR` | Columnar Parquet | Document title |
| `embedding_f32` | `FixedSizeList[Float32, 256]` | In-table Parquet | Raw floating-point embeddings |
| `embedding_int8`| `FixedSizeList[Int8, 256]` | In-table Parquet | Quantized 4× compressed vector |
| `blob_path` | `VARCHAR` | Pointer | Filepath to raw binary asset in `_lakehouse/blobs/` |

**[NOTE]**
Inlining multi-megabyte raw image/PDF blobs directly inside Parquet causes severe row-group I/O amplification during random access queries. We decouple binary assets into `_lakehouse/blobs/` and keep only fixed-size embeddings and pointers inside the table.

---

### 2.5 AI Governance & Provenance: `training_corpus_governed`
**[SPEC]**

Partitioned by EU AI Act Article 10 classification:
- `provenance_bucket=synthetic`
- `provenance_bucket=public_domain`
- `provenance_bucket=licensed`
- `provenance_bucket=scraped_optout_checked`
- `provenance_bucket=UNCLASSIFIED` *(Guaranteed excluded from training sets)*
