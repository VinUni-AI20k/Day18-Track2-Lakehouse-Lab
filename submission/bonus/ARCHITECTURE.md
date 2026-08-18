# Topic A: LLM Observability at 1B Requests/Day

> **Architecture Brief — ContentForge AI LLM Cost & Latency Dashboard**
> Team: 4 engineers | Scale: 1B req/day | Budget: ≤$5K/month

---

## 1. Problem Statement

A foundation-model API team (similar to ContentForge's LLM layer) logs every request/response.

**Scale:** 1B requests/day × ~5 KB/req = **5 TB/day raw data**  
**Retention:** 7 days full logs (incident review) → then aggregate only for 1 year

**Requirements:**
1. Dashboard: cost & latency by tenant, refresh every 5 minutes
2. Full prompt/response logs: 7 days retention, then only aggregates
3. PII redaction before any analyst access
4. Storage cost ceiling: **≤ $5K/month**

**Why hard:** At 5 TB/day raw, naive storage burns $750/month (S3 Standard @ $0.023/GB). Need aggressive tiering + aggregation to stay under cap. LLM cost calculation requires token counting per model + per-tier pricing matrix.

---

## 2. Architecture Diagram

```
[LLM Providers] ──▶ [Bronze: Raw Ingestion]
    OpenAI / Mistral / Anthropic       S3 raw/
    per-request JSON logs               5 TB/day

──▶ [Silver: Deduplication + PII Redaction]
    S3 processed/
    Dedupe by request_id
    PII tokenization (phone, email)

──▶ [Gold: Aggregates]
    S3 gold/ (7-day aggregates)
    S3 archive/ (1-year rollups)
    
    Dashboard ←── Spark/DuckDB queries
    Tenant A: $X/day | p50/p95 latency
```

### Bronze → Silver → Gold Flow

| Layer | Format | Purpose | Retention |
|-------|--------|---------|-----------|
| Bronze | Raw JSON | Ingestion, audit trail | 24 hours |
| Silver | Parquet | Dedupe, PII redact, enrich | 7 days |
| Gold | Parquet aggregates | Dashboard queries | 1 year |
| Archive | Glacier | Cost optimization | 1 year |

---

## 3. Major Decisions (≥5)

### Decision 1: Table Format — Delta Lake vs Iceberg

**Choice: Delta Lake**

| Criteria | Delta Lake | Iceberg |
|----------|-----------|---------|
| Schema enforcement | ✓ | ✓ |
| Time travel | ✓ | ✓ |
| MERGE for dedup | Native | Possible |
| Ecosystem (Spark, DuckDB) | Wide | Growing |
| **Winner for this use case** | ✓ | |

**Alternatives eliminated:**
- **Iceberg**: More complex catalog management, slower writes. Overkill for single-team use case.
- **Raw S3 + Athena**: No ACID guarantees, dedup harder, schema drift risk.

---

### Decision 2: Ingestion Path — Streaming vs Micro-batch

**Choice: Kafka → Spark Structured Streaming micro-batches (5-min windows)**

**Alternatives eliminated:**
- **Pure streaming (Flink/Kafka Streams)**: Lower latency than needed. 5-min dashboard refresh doesn't require sub-second. Added complexity unjustified.
- **Direct writes from API**: Simpler but no deduplication, no backpressure, no replay capability.

**Trade-off reasoning:** 5 TB/day raw = ~58 MB/second. Spark micro-batch every 5 minutes = 300 MB batches. Manageable on 4-node cluster. Enables native deduplication via MERGE.

---

### Decision 3: PII Redaction Strategy

**Choice: Tokenization at Bronze landing**

- Replace phone numbers, emails, IPs with deterministic tokens
- Token map stored separately in encrypted Secrets Manager
- Analysts query Gold layer only (already redacted)

**Alternatives eliminated:**
- **Redaction at query time**: Too late — raw data already in lake, violates compliance
- **Redaction at Silver only**: Raw unredacted data sits in Bronze > 24h — compliance risk

**Trade-off reasoning:** Tokenization at Bronze adds < 5ms latency per event. Acceptable trade-off for compliance guarantee.

---

### Decision 4: Storage Tiering

**Choice: S3 Standard (7 days) → S3 IA (8-30 days) → S3 Glacier (31-365 days)**

| Tier | Days | Cost/GB | Use Case |
|------|------|---------|----------|
| Standard | 0-7 | $0.023 | Dashboard queries |
| Infrequent Access | 8-30 | $0.0125 | Ad-hoc analysis |
| Glacier Instant | 31-365 | $0.004 | Audit, compliance |

**Alternatives eliminated:**
- **S3 Intelligent-Tiering**: Good but unpredictable at this scale. Explicit tiers cheaper.
- **All Standard**: $750/month for raw storage alone. Over budget.

**Math:** 5 TB/day × 30 days (before Glacier) = 150 TB-months × $0.0125 = $1,875/month for Silver (7-day Gold = negligible). Within $5K cap.

---

### Decision 5: Catalog — Unity Catalog vs Hive Metastore

**Choice: Unity Catalog (if on Databricks) or Hive Metastore + REST Catalog**

**Alternatives eliminated:**
- **No catalog**: Anti-pattern. No schema governance, lineage tracking impossible.
- **Homegrown registry**: Reinventing wheel, no ecosystem integration.

**Trade-off reasoning:** For 4-person team, Unity Catalog provides governance + lineage out of box. Hive Metastore + Polaris REST is vendor-neutral alternative if migrating later.

---

## 4. Failure Modes (≥3)

### Failure Mode 1: Silent Data Loss from Out-of-Order Events

**Scenario:** LLM provider returns response before request logged (race condition) → incomplete records in Bronze.

**Detection:**
- Alert if `request_count != response_count` per 5-min window
- Delta Lake CDF on Bronze to detect gaps

**Rollback:**
```sql
MERGE INTO gold.metrics g
USING silver.repair s
ON g.request_id = s.request_id
WHEN NOT MATCHED THEN INSERT *
```

---

### Failure Mode 2: PII Leak via Schema Evolution

**Scenario:** New LLM model returns additional fields (e.g., `user_email`) not in PII redaction schema → leaks into Gold.

**Detection:**
- Schema enforcement blocks writes with unknown columns (Delta/Iceberg)
- Weekly audit query: `SELECT * FROM gold WHERE raw_json LIKE '%@%.%'`

**Rollback:**
- Time travel to last known-good version
- Re-process affected window through Silver

---

### Failure Mode 3: Budget Overrun from Uncontrolled Retention

**Scenario:** Developer accidentally sets `retention_days = 365` on Bronze instead of Gold → 5 TB × 365 days in Standard tier → $9K/month.

**Detection:**
- AWS Budget Alert at 80% ($4K)
- CloudWatch metric on S3 bucket size

**Rollback:**
- S3 Lifecycle rules enforce tiering automatically
- IMMEDIATELY move to Glacier, then re-evaluate
- Delta Lake `VACUUM` with explicit retention parameter

---

## 5. Cost Estimation (Back-of-Envelope)

### Storage

| Component | Size | Tier | Cost |
|-----------|------|------|------|
| Bronze (24h) | 5 TB | Standard | $115/month |
| Silver (7d) | 35 TB | Standard | $805/month |
| Gold (7d aggregates) | 500 GB | Standard | $12/month |
| Archive (1yr) | 1.8 PB | Glacier | $7,200/month |

**Wait — $8K+ for archive alone. Need optimization.**

**Revised Archive Strategy:**
- Only aggregate data goes to archive (not raw)
- 1B req/day × 30 days = 30B records → aggregates = ~500 GB/year
- Real archive cost: ~$2/month for aggregates

**Total revised: $944/month** ✓ Well under $5K cap.

### Compute

| Component | Spec | Cost |
|-----------|------|------|
| Spark cluster (ingestion) | 4 × r5.xlarge | $800/month |
| Databricks / EMR (queries) | On-demand | $200/month |
| **Total** | | **$1,144/month** |

### Grand Total: ~$1,944/month (under $5K budget ✓)

---

## 6. MVP Slice (1-Week Build)

### Week 1: Minimal Viable Pipeline

**Goal:** Proof-of-concept that proves the architecture works at small scale.

| Task | Owner | Deliverable |
|------|-------|-------------|
| Delta Lake setup | Eng 1 | Bronze/Silver/Gold tables |
| Mock data generator | Eng 2 | 100K sample events |
| PII tokenization function | Eng 3 | Redaction UDF |
| Dashboard prototype | Eng 4 | Grafana + Prometheus |
| Integration test | All | E2E pipeline run |

**Success criteria:** 100K events processed, dashboard shows cost + latency per model, PII redaction verified.

**Out of scope for MVP:** Archive tiering, multi-tenancy, SLA alerting.

---

## 7. Alignment with Day 18 Concepts

| Concept | Applied Here |
|---------|-------------|
| Delta ACID | MERGE for deduplication, schema enforcement |
| OPTIMIZE + Z-ORDER | Cluster by `tenant_id`, `model_name` |
| Time travel | Rollback on schema evolution failures |
| Medallion (Bronze→Silver→Gold) | Core architecture pattern |
| Catalog as control plane | Unity/Hive catalog for schema governance |
| EU AI Act Art. 10 buckets | PII redaction + audit trail for compliance |
| FinOps tiering | S3 Standard → IA → Glacier lifecycle |

---

## Submission

See `poc/tokenization_udf.py` for proof-of-concept implementation of PII redaction.
