# Bonus: LLM Observability at 1B Requests/Day

**Mã SV:** 2A202600665

---

## 1. Problem Statement

Một foundation-model API team log mọi request/response: **1B req/ngày, ~5 KB/req → 5 TB/ngày raw JSON**. Yêu cầu: (1) dashboard cost & latency theo tenant, refresh mỗi 5 phút; (2) prompt/response đầy đủ giữ 7 ngày cho incident review, sau đó chỉ giữ aggregates 1 năm; (3) PII redact trước khi bất kỳ ai đọc; (4) tổng chi phí storage ≤ **$5 K/tháng**. Thách thức: throughput 1B req/ngày (~11.6 K req/s peak), retention policy phức tạp, budget storage cứng — không thể dump hết vào S3 Standard rồi tính sau.

---

## 2. Architecture Diagram

```
                         ┌──────────────────┐
                         │  API Gateway logs │
                         │  1B req/ngày     │
                         └────────┬─────────┘
                                  │ Kafka (200 partitions, retention 24h)
                                  ▼
    ┌─────────────────────────────────────────────────────┐
    │  BRONZE (_lakehouse/bronze/llm_calls_raw/)          │
    │  • Delta Lake, gzip, not partitioned                │
    │  • RAW JSON + ingest_ts                             │
    │  • Retention: 7 ngày (VACUUM job daily)             │
    │  • Cost: S3 Standard → 7-day window                 │
    └─────────────────────┬───────────────────────────────┘
                          │ Spark Structured Streaming (micro-batch 30s)
                          ▼
    ┌─────────────────────────────────────────────────────┐
    │  SILVER (_lakehouse/silver/llm_calls/)              │
    │  • Delta Lake, zstd, partitioned BY (date, tenant)  │
    │  • Parsed JSON → typed columns                      │
    │  • PII redacted (tokenization tại write)            │
    │  • Dedup BY request_id                              │
    │  • Z-order BY tenant_id                             │
    │  • Cost: S3 Standard → 90 ngày, S3 IA → 365 ngày   │
    └─────────────────────┬───────────────────────────────┘
                          │ Daily batch (post-incident window)
                          ▼
    ┌─────────────────────────────────────────────────────┐
    │  GOLD (_lakehouse/gold/llm_daily_metrics/)          │
    │  • Delta Lake, zstd, partitioned BY (date)          │
    │  • p50/p95/p99 latency, cost_usd, error_rate        │
    │  • Aggregated BY (date, tenant, model)              │
    │  • Z-order BY tenant_id                             │
    │  • Cost: S3 Standard → 365 ngày (~200 MB/tháng)    │
    └─────────────────────┬───────────────────────────────┘
                          │ Trino / DuckDB query engine
                          ▼
              ┌────────────────────────┐
              │ Grafana dashboards     │
              │ refresh 5 phút         │
              │ p95 < 2s query latency │
              └────────────────────────┘
```

---

## 3. Key Decisions & Rejected Alternatives

### 3.1 Table Format: Delta Lake
- **Chọn Delta Lake.** Rejected **Apache Iceberg** vì: team đã có Spark/Databricks stack, Delta có MERGE + CDF built-in phù hợp với CDC và incremental processing. Iceberg có catalog flexibility tốt hơn nhưng không đáng đổi stack cho use case này.
- Rejected **Parquet thuần** vì: mất ACID, mất time travel, mất file-skipping stats — không thể query nhanh trên 5 TB/ngày.

### 3.2 Compression: zstd (Silver/Gold), gzip (Bronze)
- **Chọn zstd cho Silver+Gold** (compression ratio ~10–15×, decompress ~500 MB/s). Rejected snappy (nhanh hơn nhưng ratio ~4× — tốn gấp 3× storage cost). Bronze dùng **gzip** vì write-once read-rare, ratio ~15×.
- Math: 5 TB/ngày raw → ~330 GB/ngày với zstd. 7 ngày Bronze = ~2.3 TB S3 Standard ≈ $80/tháng.

### 3.3 Partitioning: (date, tenant)
- **Chọn (date, tenant) cho Silver.** Rejected chỉ partition by date vì: query chủ yếu filter theo tenant (dashboard tenant-specific) — partition pruning giảm scan xuống 1/N partitions. Rejected bucket by tenant_id vì: không linh hoạt với tenant mới và gây small-file problem.
- Z-order BY tenant_id bổ sung để file-skipping ngay cả intra-partition.

### 3.4 Retention Strategy: Bronze VACUUM 7 ngày, S3 lifecycle IA/Galcier
- **Bronze:** VACUUM daily, giữ 7 ngày → S3 Standard. Qua 7 ngày → xóa (không cần IA vì không ai query Bronze sau 7 ngày).
- **Silver:** S3 Standard 90 ngày → S3 IA 275 ngày → Glacier trước khi xóa. **S3 lifecycle rule tự động chuyển object.**
- **Gold:** S3 Standard 365 ngày, dung lượng nhỏ (~5 GB/năm) → chi phí không đáng kể.
- Rejected "giữ tất cả trên Standard" — $10 K/tháng vs $4 K/tháng với tiering.

### 3.5 PII Redaction: Tokenization tại Silver write
- **Chọn tokenization tại Silver layer**: mapping table riêng (PII_hash → original) trong Delta table encrypted. Rejected redact tại Bronze vì: incident review cần raw prompt/response đầy đủ trong 7 ngày. Rejected encryption-at-rest toàn bộ vì: query không thể pushdown, mất file-skipping stats.
- Audit log mỗi lần đọc PII mapping table → OpenLineage event.

### 3.6 Ingestion Path: Kafka → Spark Structured Streaming
- **Chọn Kafka + Spark micro-batch 30s.** Rejected Kafka → Flink (thêm infra complexity, team không có Flink exp). Rejected direct write từ API Gateway → Delta (mất exactly-once semantics khi crash).
- Micro-batch cho phép dedup trong window, kiểm soát backpressure, và retry tự động.

### 3.7 Cost Model & Pricing

| Layer | Storage | Size | $/tháng | Calculation |
|---|---|---|---|---|
| Bronze (7d) | S3 Standard | 2.3 TB | ~$80 | $0.023/GB × 2,300 GB × 7/30 |
| Silver (90d) | S3 Standard | 29.7 TB | ~$683 | 330 GB/ngày × 90 ngày × $0.023 |
| Silver (275d) | S3 IA | 82.5 TB | ~$990 | $0.0125/GB × 82,500 GB |
| Gold (365d) | S3 Standard | ~5 GB | ~$0.12 | Negligible |
| **Subtotal** | | | **~$1,753** | |
| Compaction/OPTIMIZE | EC2 (r6g.2xlarge) | ~200 giờ/tháng | ~$500 | |
| Spark streaming | EMR (4 x r6g.xlarge) | 24/7 | ~$2,400 | |
| **Total** | | | **~$4,653** | ✅ Dưới $5 K |

---

## 4. Failure Modes

### 4.1 Kafka cluster down (3 AM)
- **Detection:** Bronze write latency > 60s, Kafka consumer lag alert.
- **Rollback:** Spark streaming tự động pause + retry với exponential backoff. Kafka retention 24h → buffer đủ cho 6h downtime. Nếu > 6h: backfill từ API Gateway logs (S3 fallback) bằng batch job.

### 4.2 Schema evolution — unexpected field in LLM response (new model version)
- **Detection:** Silver write fails với schema mismatch. Alert trong 5 phút.
- **Rollback:** schema_mode="merge" tự động thêm column với null default. Không block pipeline. Incident review: decide có nên backfill không.

### 4.3 PII leak — tokenization mapping table corrupted
- **Detection:** audit log show PII mapping read không có corresponding incident ticket.
- **Rollback:** RESTORE mapping table từ Delta time travel (v12 → v11). Verify bằng checksum so sánh với daily snapshot. Gọi security incident process.

### 4.4 Cost overrun — unexpected traffic spike (3× normal)
- **Detection:** Daily cost report > $200/ngày (threshold = 1.5× baseline).
- **Rollback:** Tự động chuyển Bronze compression từ gzip → zstd (giảm 30% size) + giảm partition count. Nếu vẫn over: pause non-critical tenants, notify engineering.

---

## 5. MVP Slice (1 tuần)

Tuần đầu, build path *một tenant, một model*:

1. Kafka topic → Spark streaming → Bronze Delta (1 partition)
2. Bronze → Silver: parse JSON + dedup + PII stub (mock tokenization)
3. Silver → Gold: daily aggregation p50/p95 latency, cost_usd
4. Grafana dashboard query từ Gold: refresh 5 phút
5. Verify: 10M req/day mock → validate latency SLA p95 < 2s

PoC optional tại `submission/bonus/poc/` show PII tokenization function + MERGE dedup.
