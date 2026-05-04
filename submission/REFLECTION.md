# REFLECTION: Data Lakehouse Architecture — Day 18

**Họ và tên:** Đào Hồng Sơn
**MSSV:** 2A202600462

---

## Tổng quan

Buổi học hôm nay đã cung cấp một bức tranh toàn diện về Data Lakehouse Architecture — từ lịch sử tiến hoá của các hệ thống lưu trữ dữ liệu, qua các open table formats (Delta Lake, Apache Iceberg), cho đến cách thiết kế medallion architecture cho AI/ML workloads. Dưới đây là những suy nghĩ và insights cá nhân.

---

## 1. Evolution & 3 Eras — Bài học về "Tại sao cần Lakehouse?"

### Câu chuyện tiến hoá

Điều đáng chú ý nhất là cách mỗi era lại sinh ra một class data mới mà era trước không xử lý được:

| Era | Data mới | Thách thức |
|-----|----------|------------|
| Traditional | Semi-structured (JSON) | Schema drift |
| ML | Multimodal + embeddings | Training data provenance |
| LLM | 10^12+ tokens, vectors | Dedup leak, license violation |

**Insight:** Stack lưu trữ mở rộng, không thay thế. Đây không phải cuộc đua "ai thắng" mà là cách tích hợp các công nghệ phù hợp cho từng use case.

### Câu nói đinh

> *"Đổ tất cả vào S3" — works ở 10 GB, ác mộng ở 10 TB, production outage ở 10 PB.*

Đây là một trong những anti-pattern phổ biến nhất. Bài học: **Metadata layer biến object storage thành transactional store.** Không có Delta/Iceberg, S3 chỉ là nơi lưu trữ chứ không phải hệ thống dữ liệu.

---

## 2. Delta Lake & ACID Transactions — Nền tảng quan trọng nhất

### Transaction Log

Delta Lake sử dụng `_delta_log/` với các file JSON để track mọi thay đổi:
- `add` — thêm file mới
- `remove` — đánh dấu file bị xoá
- Mỗi commit là atomic, có timestamp và version

Điều này giải quyết bài toán: **concurrency control trên shared object storage như S3** — thứ mà bản thân S3 không hỗ trợ.

### Deletion Vectors — Game changer cho GDPR

Vấn đề cũ: DELETE 1 row → rewrite cả file → write amplification 1000×.

Giải pháp: Lưu bitmap đánh dấu rows bị xoá trong sidecar file. Reader skip rows theo bitmap. Kết quả:
- DELETE/UPDATE/MERGE nhanh hơn 10–100×
- GDPR compliance từ giờ → phút thay vì ngày

**Ý nghĩa thực tiễn:** Đây là tính năng bắt buộc cho bất kỳ hệ thống nào xử lý user data có PII.

### Change Data Feed (CDF)

CDF cho phép downstream consumers biết **cái gì thay đổi** thay vì đọc full snapshot. Pattern chuẩn:
```
Bronze CDF → Silver MERGE → Gold incremental refresh = streaming-like batch
```

Đây là canonical CDC sink pattern — không cần custom apply logic.

---

## 3. Time Travel — "Git cho Data"

### Ba cách query history

| Cách | Use case |
|------|----------|
| `versionAsOf` | Reproducible training, A/B test datasets |
| `timestampAsOf` | Point-in-time audit |
| `restoreToVersion()` | Rollback bad ingestion (30s vs 2h manual) |

### MLflow Integration

Slide minh hoạ rất rõ: mỗi MLflow run ghi lại `data_version`. Khi model có vấn đề, ta có thể trace ngược về đúng dataset đã dùng.

**Compliance angle:** `DESCRIBE HISTORY` = compliance-grade audit log. Built-in!

---

## 4. Apache Iceberg vs Delta Lake vs Hudi

### So sánh nhanh

| Feature | Delta | Iceberg | Hudi |
|---------|-------|---------|------|
| ACID | ✓ | ✓ | ✓ |
| Time Travel | ✓ | ✓ | ✓ |
| Hidden Partitioning | ✗ | ✓ | ✗ |
| Multi-engine (native) | UniForm | ✓ | ✓ |
| Ecosystem | Databricks | Netflix, Apple, LinkedIn | Uber |

### Iceberg Hidden Partitioning — Tại sao quan trọng

Slide lấy ví dụ cụ thể:

```sql
-- Cách cũ (Hive)
CREATE TABLE events (
  ts TIMESTAMP,
  ts_day DATE,  -- duplicate!
  user_id BIGINT
) PARTITIONED BY (ts_day);

-- User QUÊN filter ts_day = full scan!
SELECT * FROM events WHERE ts > '2026-04-01'; -- BAD
```

```sql
-- Iceberg (hidden partitioning)
CREATE TABLE events (
  ts TIMESTAMP,
  user_id BIGINT
) PARTITIONED BY (days(ts));

-- User filter natural column, Iceberg tự prune
SELECT * FROM events WHERE ts > '2026-04-01'; -- AUTO-prunes!
```

**LinkedIn cite đây là lý do chính migrate Hive → Iceberg.** Đa số performance regression trong production lakehouse là vì user quên partition column — Iceberg loại bỏ class bugs này.

### Format War đã kết thúc (2026)

Key events:
- Databricks acquires Tabular $1B+ (2024)
- Snowflake → Iceberg native + Polaris catalog
- Iceberg v3 GA on Databricks (Apr 2026): deletion vectors + row lineage + VARIANT

**Kết luận:** 30% giảm DE workload khi dùng Iceberg v3. Chọn theo tooling fit, không cần "chọn bên".

---

## 5. Query Engines — Đừng dùng búa để giết ruồi

| Engine | Sweet Spot | Scale | Format |
|--------|------------|-------|--------|
| Spark SQL | ETL, batch ML | TB–PB | Delta, Iceberg, Hudi (native) |
| Trino | Federated BI, ad-hoc | GB–PB | Iceberg (native), Delta (connector) |
| DuckDB | Single-node analytics | MB–100GB | Parquet/Delta/Iceberg via extensions |

**Quy tắc quan trọng:**

> *Đừng chạy Spark cluster cho 5 GB query — DuckDB nhanh hơn, rẻ gần như 0.*

|< 100 GB, 1 dev|ETL Spark-native|BI multi-source|AWS serverless|
|---|---|---|---|
|DuckDB|Spark SQL|Trino|Athena|

---

## 6. Medallion Architecture — Thiết kế cho AI/ML

### Ba layers và contracts

```
Bronze (Raw)          → Silver (Cleaned)       → Gold (Analytics)
─────────────────────────────────────────────────────────────────
request_id, raw_json  → request_id, model,      → date, model,
                       prompt_tokens,           → p50/p95_latency,
                       completion_tokens,       → cost_usd, error_rate
                       latency_ms, user_id
─────────────────────────────────────────────────────────────────
30 ngày retention     1 năm retention          5 năm retention
Immutable (append)    MERGE upsert             Rebuild from Silver
```

### Contracts quan trọng

- **Bronze:** append-only (immutable audit)
- **Silver:** upsert (MERGE)
- **Gold:** rebuild-from-Silver (idempotent)

Schema rõ ràng mỗi layer = **data contract giữa teams.**

### LLM Observability

Pattern cho LLM monitoring:
```
Inference req/resp → Bronze (raw JSON, 30d)
                   → Dedup + parse tokens/latency → Silver
                   → Aggregate (date, model) metrics → Gold
```

---

## 7. Production Patterns & Anti-Patterns

### Top 5 Anti-Patterns (80% production pain)

| # | Anti-pattern | Hậu quả | Fix |
|---|--------------|---------|-----|
| 1 | "Đổ tất cả vào S3" (raw JSON, no schema) | Data swamp | Enforce schema từ Bronze |
| 2 | Partition theo user_id | Triệu partitions nhỏ | Partition by date, Z-ORDER user_id |
| 3 | Bỏ qua OPTIMIZE | 10× small-file problem | Daily OPTIMIZE cron |
| 4 | VACUUM 0 HOURS | Mất time travel | Giữ tối thiểu 168h |
| 5 | Spark cluster cho 5 GB | Lãng phí 10× | DuckDB / Athena |

### CDC Pattern cho Fintech Vietnam

```
Postgres (OLTP)
    → Debezium (WAL reader)
    → Kafka + Schema Registry
    → Hudi/Delta Streamer
    → Bronze table (MERGE upsert)
```

**Vietnam context:** Pattern chuẩn cho MoMo, VNPay, Cake — Decree 13 impact: sensitive data → on-prem MinIO + Iceberg.

---

## 8. Production Ops — Trifecta bắt buộc

### 1. Catalog (REST Standard 2026)

| Catalog | Origin | Killer feature |
|---------|--------|----------------|
| AWS Glue | AWS | Default AWS, 39% share |
| Unity Catalog | Databricks | Fine-grained governance |
| Apache Polaris | Snowflake | Vendor-neutral REST |
| Project Nessie | Dremio | Git-like branching/tagging |

**REST Catalog spec = lingua franca 2026.** Nessie đặc biệt hữu ích cho ML versioning:

```bash
nessie tag create v1-prod
nessie branch create exp-2026
# ...train + evaluate on branch...
nessie merge → main
```

"Model X dùng data nào?" → `nessie tag list` — trả lời 1 command.

### 2. Data Contracts (Schema + Constraints + SLA)

```
Great Expectations → Bronze (validate raw)
dbt schema.yml     → Silver/Gold (structural correctness)
SodaCL             → Prod monitoring (anomaly detection)
```

**Pattern:** Run trong CI (pre-merge) và runtime (per-batch). Bể contract → block pipeline.

### 3. Data Lineage (OpenLineage + Marquez)

```
Bronze.events → Spark job → Silver.events → dbt model → Gold.metrics → Dashboard
```

OpenLineage: Spark/Airflow/dbt/Flink emit lineage events tự động. Marquez: reference server.

### 4. Security & Governance

**Decree 13/2023/NĐ-CP impact:**
- Personal data: basic vs sensitive
- Data residency: sensitive ở VN
- Right-to-forget: 72h SLA
- Cross-border: consent + DPI

**Implication:** Sensitive data → on-prem MinIO + Iceberg.

### 5. FinOps — TCO Comparison

| Component (100 TB/tháng) | Snowflake | Iceberg + Trino/S3 | Databricks |
|--------------------------|-----------|---------------------|------------|
| Total | $24,000 | $12,800 (–47%) | $17,300 (–28%) |

**Quy tắc:** Savings chỉ materialize nếu actively optimize. Forget OPTIMIZE → small-file tax giết economics.

---

## 9. Case Studies — Scale Production

| Company | Format | Scale | Key Achievement |
|---------|--------|-------|-----------------|
| Uber | Hudi | 350 PB | 6T rows/day, freshness 24h → 1h |
| Netflix | Iceberg + Lance | PB | Query planning 9.6min → 42s |
| LinkedIn | Iceberg | PB | Hidden partitioning = main reason migrate from Hive |
| Shopify | Iceberg + Trino | PB | Multi-engine BI + ML |

**Pattern chọn format:**
- Append-mostly (logs, events) → Delta/Iceberg
- Mutation-heavy (orders, sessions) → Hudi
- Multimodal (video, embeddings) → Lance + Iceberg

---

## 10. Lab Insights — Những gì cần thực hành

### NB01 — Delta Lake basics
- Schema enforcement + transaction log

### NB02 — OPTIMIZE + Z-ORDER benchmark
- Small-file problem: 10,000 × 1 MB → 10 × 1 GB
- Benchmark: query time trước/sau (target ≥ 3×)

### NB03 — Time travel + MERGE
- `restoreToVersion()` rollback 30s vs 2h manual
- MERGE upsert 10–50× faster at production scale

### NB04 — Medallion pipeline
- Bronze → Silver → Gold cho LLM observability hoặc RAG corpus

---

## Key Takeaways

1. **Lakehouse = ACID + object storage + open formats.** Foundation chung cho 3 era.

2. **Format war kết thúc.** Iceberg + Delta UniForm = de facto standard. On-disk Parquet identical, chọn theo tooling fit.

3. **Time travel + branching (Nessie) = "git checkout" cho dataset.** OPTIMIZE + Z-ORDER + Deletion Vectors bắt buộc cho production.

4. **LLM era cần thêm tầng:** Vector DB (RAG), Lance (multimodal), embedding versioning, training data provenance.

5. **Production ops trifecta:** Catalog + Data Contracts + Lineage. Bật từ ngày 1.

---

## Questions & Open Thoughts

1. **Iceberg v3 trên Databricks (Apr 2026):** Với VARIANT type và row lineage, liệu đây có phải là bước tiến để thay thế hoàn toàn traditional DW?

2. **Vietnam-specific:** Decree 13 yêu cầu data residency cho sensitive data. On-prem MinIO + Iceberg là giải pháp khả thi, nhưng operational overhead như thế nào so với managed services?

3. **DuckDB vs traditional databases:** Với < 100 GB, DuckDB gần như "free" về infra. Khi nào thì thực sự cần scale lên Spark/Trino?

4. **Embedding versioning:** Pin `doc_version × model_version` vào Delta/Iceberg version + MLflow run_id. Cách implement nào hiệu quả nhất?

---

## Next Steps

- Hoàn thành Lab 18 (4 notebooks)
- Đọc case studies: Netflix, Uber, Apple Iceberg + Lance multimodal docs
- Cài Docker + Qdrant image cho Day 19 (Vector Store + ANN)