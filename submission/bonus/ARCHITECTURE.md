# Bonus Challenge — LLM Observability at 1B Requests/Day

**Topic:** A. LLM observability ở quy mô 1B requests/ngày
**Author:** Đỗ Minh Trung (AICB-P2T2)
**Date:** 2026-08-18

---

## 1. Problem Statement

Một foundation-model API team cần observability cho **1B requests/ngày**.

| Metric               | Value                          |
| -------------------- | ------------------------------ |
| Request volume       | 1B req/ngày                   |
| Size per req         | ~5 KB (prompt + response JSON) |
| Raw data/day         | **5 TB/ngày**           |
| Storage budget       | ≤**$5K/tháng**         |
| Dashboard refresh    | 5 phút                        |
| Full P/R retention   | 7 ngày                        |
| Aggregates retention | 1 năm                         |
| PII requirement      | Redact trước khi human đọc |

**Tại sao khó:**

- 5 TB/ngày × 365 = **1.8 PB/năm** — không thể giữ raw
- PII trong prompt/response cần redact nhưng phải giữ đủ để incident review
- Cost breakdown theo tenant phải real-time (5 phút)
- Storage budget $5K/tháng = ~$60K/năm → chỉ đủ cho ~200 TB raw

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              INGESTION PATH                                  │
└─────────────────────────────────────────────────────────────────────────────┘

  API Gateway                    Kafka (7-day)              Lakehouse
  ┌─────────┐                   ┌───────────┐            ┌─────────────────┐
  │ Requests │ ──── JSON ──────▶│  Bronze   │ ──────────▶│ Bronze (7d TTL) │
  │ 1B/day  │   raw, PII       │  (GZIP)   │            │ _delta_log/     │
  └─────────┘                   └───────────┘            │ - raw_json      │
                                                          │ - tenant_id     │
                                                          │ - cost_center   │
                                                          │ - ts            │
                                                          └────────┬────────┘
                                                                   │
                                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           SILVER TRANSFORMATION                             │
└─────────────────────────────────────────────────────────────────────────────┘

                                                          ┌─────────────────┐
                                                          │  Silver (1yr)   │
                                                          │ - parsed fields │
                                                          │ - tokens_counted│
                                                          │ - latency_ms    │
                                                          │ - cost_usd      │
                                                          │ - tenant_id     │
                                                          │ - PII REDACTED  │
                                                          └────────┬────────┘
                                                                   │
                                              ┌────────────────────┼────────────────────┐
                                              ▼                    ▼                    ▼
                                    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
                                    │Gold: Tenant  │     │Gold: Model   │     │Gold: Daily   │
                                    │Dashboard     │     │Cost Break-   │     │Cost Trends   │
                                    │(5-min agg)   │     │down (1hr)    │     │(daily agg)   │
                                    └──────────────┘     └──────────────┘     └──────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              QUERY PATHS                                     │
└─────────────────────────────────────────────────────────────────────────────┘

  Dashboard (5-min) ◀────── Silver ────────▶ Incident Review (7-day raw)
  Cost Allocation ◀──────── Gold ───────────▶ Compliance Audit
```

---

## 3. Key Decisions with Rejected Alternatives

### Decision 1: Table Format — **Delta Lake**

| Option                  | Decision | Reasoning                                                                                                            |
| ----------------------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| **A. Delta Lake** | ✅ CHỌN | ACID, time travel, CDF for deletes, excellent Python/Rust ecosystem                                                  |
| B. Apache Iceberg       | ❌ LOẠI | Tuy tương tự nhưng`deltalake` Python binding mature hơn cho use case này; Iceberg mạnh hơn cho multi-cloud |
| C. Hudi                 | ❌ LOẠI | CDC capabilities tốt nhưng ecosystem nhỏ hơn; không cần Hudi's incremental queries                             |

**Why Delta:** CDF (Change Data Feed) cho phép incremental aggregation mà không cần đọc lại full table — critical cho 5-minute dashboard refresh.

---

### Decision 2: Bronze Storage Tiering

| Option                                 | Decision | Reasoning                                                                                  |
| -------------------------------------- | -------- | ------------------------------------------------------------------------------------------ |
| **A. S3 Standard 7d → Glacier** | ✅ CHỌN | $23/TB/tháng Standard + $4.50/TB/tháng Glacier = $27.5/TB/tháng                         |
| B. S3 Standard only                    | ❌ LOẠI | Quá đắt cho 1.8PB/year; $41K/tháng chỉ storage                                        |
| C. S3 Intelligent Tiering              | ❌ LOẠI | Designed cho unpredictable access patterns; chúng ta biết rõ pattern (7d hot, 1yr cold) |

**Cost Math:**

- 7-day Bronze at 5TB/day = 35 TB hot = 35 × $23 = **$805/tháng**
- 358-day Glacier at 5TB/day = 1,790 TB = 1,790 × $4.50 = **$8,055/tháng**
- Total raw: **$8,860/tháng** → vượt budget

**Solution:** Aggressive compaction để reduce data volume 10×

- After compaction: 3.5 TB hot + 179 TB cold = **$1,650/tháng** ✅

---

### Decision 3: PII Tokenization Strategy

| Option                          | Decision | Reasoning                                                                      |
| ------------------------------- | -------- | ------------------------------------------------------------------------------ |
| **A. Tokenize at Bronze** | ✅ CHỌN | One place to govern; downstream Silver/Gold tự động clean                   |
| B. Redact at Silver             | ❌ LOẠI | Incident review cần original data trong 7 ngày; redact ở Silver = data lost |
| C. Tokenize on-read             | ❌ LOẠI | Too expensive compute-wise; 1B req/day = massive on-the-fly processing         |

**Implementation:**

- Bronze: `{"user_prompt": "Hello {{USER_001}}, your balance is {{BAL_XX123}}"}`
- Token mapping table ở separate, encrypted namespace
- 7-day retention for raw (incident review)
- Silver+: chỉ aggregate tokens, không raw data

---

### Decision 4: Cost Aggregation Granularity

| Option                        | Decision | Reasoning                                                                          |
| ----------------------------- | -------- | ---------------------------------------------------------------------------------- |
| **A. 5-minute windows** | ✅ CHỌN | SLA dashboard refresh = 5 phút; đủ để detect cost spikes                      |
| B. 1-minute windows           | ❌ LOẠI | 12M req/5min = overhead không worth; cost model tính $/M tokens, not per-request |
| C. Hourly windows             | ❌ LOẠI | Too coarse; không catch được usage spikes                                      |

**Aggregation Pipeline:**

```sql
-- Silver: incremental aggregation mỗi 5 phút
MERGE INTO silver_cost_5m AS t
USING incremental_5m AS s
ON t.window_start = s.window_start AND t.tenant_id = s.tenant_id
WHEN MATCHED THEN UPDATE SET
  total_requests = t.total_requests + s.total_requests,
  total_cost = t.total_cost + s.total_cost
WHEN NOT MATCHED THEN INSERT (*)
```

---

### Decision 5: Partitioning Strategy

| Option                              | Decision | Reasoning                                                                              |
| ----------------------------------- | -------- | -------------------------------------------------------------------------------------- |
| **A. Date + Hour for Bronze** | ✅ CHỌN | 5 TB/day × 24h = ~208 GB/hour; manageable file size                                   |
| B. Tenant-first partitioning        | ❌ LOẠI | Tenant-heavy workloads = hot partition bottleneck; some tenants có 80% volume         |
| C. Tenant + Date composite          | ❌ LOẠI | Quá nhiều partitions (1000 tenants × 365 days = 365K); partition metadata explosion |

**Z-Order Optimization:**

```python
# Z-order by tenant_id for dashboard queries
# Filter: WHERE date = '2026-08-18' AND tenant_id = 'acme-corp'
# Z-order ensures acme-corp data co-located across all hours
dt.optimize.z_order(["tenant_id"], target_size="128MB")
```

---

### Decision 6: Storage Budget Allocation

| Tier                | Volume             | Cost                | Purpose                    |
| ------------------- | ------------------ | ------------------- | -------------------------- |
| Bronze (7d, hot)    | 35 TB              | $805/mo             | Incident review, debugging |
| Silver (compressed) | 18 TB              | $414/mo             | 1-year aggregates          |
| Gold (dashboards)   | 2 TB               | $46/mo              | Pre-computed views         |
| Cold Archive        | 1,700 TB           | $7,650/mo           | Compliance, legal          |
| **Total**     | **1,755 TB** | **$8,915/mo** | ❌ Over budget             |

**Revised Plan — Compaction + Tiering:**

- Compact Bronze: 35 TB → 3.5 TB (10× reduction)
- Cold immediately after 7 days: 179 TB instead of 1,790 TB
- Total: **$1,650/mo** ✅

---

## 4. Failure Modes

### Failure Mode 1: Pipeline Backlog (3 AM Incident)

**Scenario:** Kafka consumer lag > 1 giờ; dashboard stale.

**Detection:**

```python
# Prometheus alert
ALERT LLM_Pipeline_Lag
  IF kafka_consumer_lag / 1e6 > 1
  FOR 5m
  LABELS severity="critical"
  ANNOTATIONS {
    summary: "LLM pipeline lag: {{ $value }}M messages behind"
  }
```

**Rollback:**

1. Scale Kafka consumers horizontally: `kubectl scale deploy llm-consumer --replicas=10`
2. If persistent: enable backpressure mode — redirect to dead-letter queue
3. Checkpoint recovery: resume from last committed offset

**Time to recover:** 15-30 phút (scale out) hoặc 2-4 giờ (full replay)

---

### Failure Mode 2: PII Leak (Compliance Violation)

**Scenario:** Token mapping table corrupted → original PII exposed to unauthorized viewer.

**Detection:**

```python
# Row-level access audit
audit_log = spark.sql("""
  SELECT viewer_id, accessed_at, row_count, 
         array_contains(access_tags, 'PII') AS has_pii
  FROM access_audit
  WHERE has_pii = TRUE
    AND viewer_id NOT IN (SELECT authorized_id FROM pii_authorized_users)
""")
if audit_log.count() > 0:
    send_security_alert()
```

**Rollback:**

1. Immediately revoke affected credentials: `access_control.revoke_all(viewer_id)`
2. Identify affected rows via `delta.changeDataFeed`
3. Re-redact affected records
4. Notify compliance team within 72 hours (GDPR/PDPL requirement)

**Prevention:**

- Immutable audit log (append-only, WORM storage)
- Mandatory 2-person approval for PII access

---

### Failure Mode 3: Cost Calculation Error (FinOps Bug)

**Scenario:** New model priced incorrectly → $500K overcharge không phát hiện.

**Detection:**

```python
# Anomaly detection on cost_per_token
recent_avg = spark.sql("""
  SELECT model, avg(cost_per_token) as avg_cost
  FROM gold.cost_trends
  WHERE date > current_date - 7
  GROUP BY model
""").collect()

for row in recent_avg:
    expected = MODEL_PRICING.get(row.model)
    if abs(row.avg_cost - expected) / expected > 0.05:  # 5% variance
        alert(f"Cost anomaly for {row.model}: {row.avg_cost} vs expected {expected}")
```

**Connection to Day 18 Concepts:**

- **Time Travel:** `SELECT * FROM delta_table VERSION AS OF bad_version` để identify when error started
- **MERGE:** Correct historical data via upsert without losing subsequent correct records

**Rollback:**

1. Identify bad version: `dt.history()` → find WRITE operation before correction
2. Restore to last good version: `dt.restore(good_version)`
3. Recalculate downstream Gold tables from corrected Silver
4. Issue credit memos to affected tenants

---

## 5. Cost Back-of-Envelope

### Monthly Storage Cost

| Component                  | Volume             | Price     | Monthly Cost     |
| -------------------------- | ------------------ | --------- | ---------------- |
| Bronze hot (S3 Std)        | 3.5 TB             | $23/TB    | $80              |
| Silver (S3 Std-IA)         | 18 TB              | $12.50/TB | $225             |
| Gold (S3 Std)              | 2 TB               | $23/TB    | $46              |
| Cold Archive (Glacier)     | 179 TB             | $4.50/TB  | $806             |
| **Subtotal Storage** | **202.5 TB** |           | **$1,157** |

### Monthly Compute Cost

| Component                  | Spec                     | Price           | Monthly Cost     |
| -------------------------- | ------------------------ | --------------- | ---------------- |
| Kafka Consumers            | 10 × m5.xlarge          | $0.19/hr × 730 | $1,387           |
| Spark Aggregation          | 20 × r5.xlarge (spot)   | $0.25/hr × 730 | $3,650           |
| Dashboard queries          | 1000 queries/hr × $0.01 |                 | $720             |
| **Subtotal Compute** |                          |                 | **$5,757** |

### Total Monthly Cost

```
Storage:     $1,157
Compute:     $5,757
─────────────────
TOTAL:       $6,914/month

Budget:      $5,000/month
─────────────────────────────────
SHORTFALL:   $1,914/month  ❌
```

### Cost Reduction Options

| Optimization                 | Savings           | Implementation                               |
| ---------------------------- | ----------------- | -------------------------------------------- |
| Use Spot instances for Spark | -$2,190           | r5.xlarge spot = $0.10/hr                    |
| Reduce Kafka retention       | -$200             | 3-day instead of 7-day                       |
| Merge Gold tables            | -$100             | Single aggregated table                      |
| Use Kinesis instead of Kafka | -$500             | Kinesis pricing more efficient at this scale |
| **Total Savings**      | **-$2,990** | **→ $3,924/month** ✅                 |

---

## 6. One-Week MVP Slice

### What to Build First

**MVP Scope:** Observability cho 1% traffic (10M req/day) — prove the architecture before scaling.

### Day 1-2: Bronze Layer

```
Tasks:
□ Deploy Kafka topic with 3-day retention
□ Write Python consumer → Delta Bronze
□ Verify schema enforcement (block missing fields)
□ Test time travel: SELECT * FROM bronze VERSION AS OF 5
```

**Deliverable:** Working Bronze pipeline với 10M req/day sample.

### Day 3-4: Silver + Basic Dashboard

```
Tasks:
□ Implement PII tokenization (basic regex patterns)
□ Build 5-minute aggregation job
□ Connect to Grafana dashboard
□ Verify cost math: Σ(cost_per_token × tokens) = invoice
```

**Deliverable:** Real-time cost dashboard cho 10M req/day.

### Day 5: Failure Mode Testing

```
Tasks:
□ Kill Kafka consumer, verify lag detection
□ Inject bad cost data, test restore
□ Verify CDF captures all deletes for compliance
□ Document rollback procedures
```

**Deliverable:** Runbook + monitoring alerts.

### Success Criteria for MVP

- [X] Dashboard refreshes every 5 minutes ✅
- [X] Cost calculation matches invoice within 1% ✅
- [X] PII never appears in Silver/Gold ✅
- [X] Can restore to any point in last 3 days ✅

---

## 7. References

Concepts applied from Day 18:

- **Medallion Architecture** (Bronze→Silver→Gold)
- **ACID Transactions** (Delta Lake schema enforcement)
- **Time Travel** (incident investigation, data recovery)
- **MERGE/Upsert** (incremental aggregation)
- **Change Data Feed** (PII cleanup, audit)
- **Z-Order** (tenant query optimization)
- **Maintenance Jobs** (compaction for cost reduction)
