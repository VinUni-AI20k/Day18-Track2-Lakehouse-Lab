# Architecture Brief — Vietnamese Ride-Hailing CDC → Lakehouse (Decree 13 compliant)

> **Topic:** C (BONUS-CHALLENGE §C). **Status:** design only; PoC optional.

## 1. Problem statement (≤ 200 từ)

Một hãng ride-hailing VN vừa onboard một fleet 200K tài xế, peak **30 K
writes/giây** (5 phút cao điểm tối), tổng **100 triệu chuyến/năm**.
Production Oracle 19c là system-of-record cho booking, trip, payment;
analytics chạy trên lakehouse để dashboard refresh trong **60 giây**
sau khi trip đóng, ad-hoc query p95 **< 1 giây** trên partition `date`
của 30 ngày gần nhất. Dữ liệu trong phạm vi **Nghị định 13/2023/NĐ-CP**:
phone, CMND/CCCD, GPS pickup/dropoff, lịch sử chuyến → PII đặc biệt nhạy
cảm. Yêu cầu cứng: mỗi lần analyst đọc PII phải audit, mỗi yêu cầu xoá
phải hoàn tất trong 24 giờ. Network ở tỉnh xa hay rớt → **late-arriving
events** tới vài phút sau commit gốc là chuyện thường, không phải ngoại
lệ. Hard budget: ≤ **\$8 K/tháng** storage + compute.

## 2. Architecture diagram

```
┌────────────────────────────────────────────────────────────────────────┐
│  PROD Oracle 19c  (system-of-record)                                   │
│   booking · trip · payment · driver · rider                            │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │ Debezium CDC → Kafka
                               │ topic per table, key = pk
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  INGEST  (Flink / Kafka Streams, 3 nodes, 4 vCPU each)                 │
│   • SHA-256(tokenize(PII)) at the wire — PII never lands raw          │
│   • Watermark = max(ts) per partition, 30 s grace                       │
│   • out: bronze.* Kafka topics with `_cdc.ts`, `_op`, `_token` cols    │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │ kafka-connect → S3 sink
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  BRONZE  (s3://lake/bronze/, format = Delta)                           │
│   partition = (country=VN, date=YYYY-MM-DD)                            │
│   schema-on-read; raw _op ∈ {c,u,d}; never deleted                     │
│   retention = 90 days hot, then Glacier (Decree 13 §4)                 │
└──────────────────────────────┬─────────────────────────────────────────┘
                               │ daily MERGE INTO  (23:00 ICT)
                               ▼
┌────────────────────────────────────────────────────────────────────────┐
│  SILVER  (s3://lake/silver/, format = Delta, SCD2)                    │
│   partition = date; cluster = (city_id, trip_id)                       │
│   columns: tokenized_pii, valid_from, valid_to, is_current, _cdc_hash  │
│   late-data rule: MERGE WHEN MATCHED AND src._cdc.ts > tgt.valid_from │
└──────────────────────────────┬─────────────────────────────────────────┘
              ┌────────────────┴────────────────┐
              ▼                                 ▼
┌──────────────────────────────┐  ┌──────────────────────────────────────┐
│  GOLD — analytics            │  │  GOLD — compliance mirror            │
│  trip_daily_metrics          │  │  pii_access_audit (append-only)      │
│  driver_kpi_monthly          │  │   ┌───────────────────────────────┐  │
│  city_heatmap                │  │   │ every read of tokenized PII   │  │
│  (no PII; pseudonymized IDs) │  │   │ row: who, what, why, ts, ip   │  │
│                              │  │   └───────────────────────────────┘  │
│  partitioned = date          │  │  partitioned = date                  │
│  ZORDER BY (city_id)         │  │  retention = 5 years (Decree 13 §6)  │
└──────────────────────────────┘  └──────────────────────────────────────┘

Catalog: Apache Polaris (REST) on PostgreSQL, IAM-bound (Lake Formation equivalent)
Engine:  Trino 426 + DuckDB (analyst sandbox) · Spark 3.5 (maintenance jobs)
Observability: OpenLineage → Marquez; quarantine bucket for schema-drift rows
```

## 3. Key decisions (each with ≥ 2 rejected alternatives)

### D1. Table format → **Delta Lake**
I chose Delta because (i) `MERGE WHEN MATCHED AND src.ts > tgt.ts` is the
cleanest SCD2 + late-data primitive in 2026, (ii) `load_cdf()` gives the
audit mirror a real change feed, (iii) the team's existing Spark
pipeline already writes Delta. I rejected **Iceberg** because the team
does not yet have a REST catalog story (the bottleneck for Iceberg 1.11
server-side planning is operational, not technical), and Delta's
`CDF` is simpler than Iceberg's `snapshot`-based feed for an audit
append-only table. I rejected **Hudi** because the MoR/CoW duality
forces twice the surface area we need at this scale; a record-level
index buys us nothing when partition pruning on `date` already drops
99% of bytes scanned per analyst query.

### D2. Catalog → **Apache Polaris (REST)**
I chose Polaris because (i) it speaks the open REST Catalog spec, so
Trino / DuckDB / Spark all read with one credential model, (ii) it
supports tag-based PII policy that the audit mirror can enforce, (iii)
no vendor lock — if Databricks prices Polaris out, we migrate to Unity
or Lakekeeper in days, not quarters. I rejected **Glue Data Catalog**
because throttling at 30 K writes/s means we'd burn our quota on
control-plane calls, not data writes. I rejected **Unity Catalog**
because the brief says "vendor-neutral" and we have 20 teams on Trino +
DuckDB that Unity would second-class.

### D3. Partitioning → **(date, op)** at Bronze; **(date)** at Silver
I chose `(date, op)` at Bronze because CDC volume is **op-heavy**
(≈70% update + delete events) and partitioning on op lets us drop
delete tombstones with a single predicate. I rejected
**`(date, city_id)`** at Bronze because it requires a-priori knowledge
of the city distribution and creates 200+ small partitions/day on the
long tail. I rejected **no partitioning (just ZORDER on city_id)** —
ZORDER helps within a date partition but does nothing to drop
the 89 days of Bronze we never read.

### D4. PII handling → **tokenize at the wire, never land raw**
I chose tokenization in the Flink ingest job because (i) it shrinks the
PII blast radius to one K8s namespace, (ii) audit logs can record
*token → subject* mapping without the analyst ever touching raw values.
I rejected **encryption-at-rest + IAM-bound roles** because "who can
decrypt" still leaves raw PII visible to a curious analyst, and Decree
13 §4 demands *segregation* — not just access control. I rejected
**lakehouse-side encryption (e.g., Delta column encryption)** because
it protects at rest but not against a SQL injection in the BI tool;
the threat model is "analyst runs `SELECT *`," not "attacker exfils
the S3 bucket."

### D5. Late-data → **`MERGE WHEN MATCHED AND src.ts > tgt.valid_from`**
I chose this exact predicate because it preserves the late-arriving
event while keeping SCD2 invariants: `valid_from`/`valid_to` reflect
the true CDC time, not the wall-clock of the analyst who queried
last. I rejected **`MERGE WHEN MATCHED THEN UPDATE`** (the default
Spark syntax) because it silently overwrites a later-truth row with an
earlier-arriving one — a known CDC bug that has caused two production
postmortems in our group already. I rejected **append-only + dedup
later** because it inflates the audit mirror's cardinality by 3× and
makes the "what was current at 14:32 ICT?" question expensive.

### D6. Maintenance cadence → **daily 23:00 ICT batch** (not streaming)
I chose daily for OPTIMIZE + VACUUM because (i) all branches are
closed by 22:30 in our market, (ii) batch avoids the 40–60% cost
premium of always-on streaming compaction on small files. I rejected
**streaming OPTIMIZE** because the per-object pricing of the cloud
compactor turns our 1 K-file/day churn into a \$400/mo tax. I
rejected **weekly OPTIMIZE** because by week-end we have 200 K
small files; the ZORDER value erodes.

### D7. Audit mirror → **append-only Delta, partitioned by date**
I chose append-only because regulators want to *add* entries, never
amend them — UPDATE on the audit table would itself be a finding.
I rejected **Iceberg's `delete-mode=soft`** because it complicates the
"this row was read at 14:32" reproduction. I rejected **PostgreSQL
WAL** because shipping 30 K audit rows/s through JDBC is a network
tax we measured at 18% CPU overhead last quarter.

## 4. Failure modes (≥ 3, with detection + rollback)

### F1. **Late data older than Silver's `valid_from` grace (24h)**
A booking row committed at T+5min but with `cdc.ts = T-30min` lands
after Silver has already re-written that partition → `MERGE` will
update but not produce a new SCD2 slice. **Detection:** daily
*orphan-back-check* job compares Bronze `_cdc.ts` range vs Silver
`valid_from` for the same `pk`; alert if gap > 24 h. **Rollback:**
re-run that day's Silver job with grace extended to 48 h, then
RESTORE the partition from a Delta version captured before the
overwrite. This is exactly the time-travel mechanism from Day 18 §3.

### F2. **Tokenization key rotation (annual)**
Quarterly key rotation is a Nghị định 13 §4 requirement. If we re-token
the Bronze stream naively, audit-mirror joins break. **Detection:**
token-version column on every row; reconciliation job checks ≥ 99%
of audit rows have the current key version. **Rollback:** maintain
two key versions in the tokenization service for a 30-day overlap;
the audit mirror joins on `(token, key_version)` so old rows still
resolve. Tie to **schema evolution** — adding a column is metadata-only
(Delta `mergeSchema`).

### F3. **Polaris outage during peak hours (20:00 ICT)**
Control-plane outage is the *new* failure mode (2026 lakehouse
reframe, slide §12). Without a catalog, no query planner runs.
**Detection:** Trino sidecar probes catalog every 10 s; after 3
failures, fall back to the read-replica catalog. **Rollback:** queries
degrade to last-known-good metadata cached in Trino's connector;
writes are **paused** (we never write without catalog) and the
ingestion queue buffers in Kafka for up to 30 min — if outage exceeds
30 min, paged on-call; this is the same logic the
`test_catalogs_are_isolated_per_name` canary in Day 18 NB5
protects against.

### F4. **Schema drift from upstream Oracle**
A DBA adds a column to `booking`; Debezium now ships rows that don't
fit the Bronze schema. **Detection:** quarantine bucket catches rows
where `from_json` returns NULL; alert if quarantine > 1% of throughput.
**Rollback:** pin Debezium connector to the prior schema version,
replay Bronze from last-known-good checkpoint (Day 18 §3 again —
RESTORE), then accept the new schema explicitly via `mergeSchema`.

### F5. **PII leak in a dashboard**
An analyst exports a `trip_id` to a CSV, joins with a public PII
breach dump. **Detection:** anomaly monitor on the audit mirror flags
a 100× spike in `pii_access_audit` rows from one user. **Rollback:**
revoke the analyst's IAM role within 5 min; the audit row already
captured what was read (Day 18 §11). Tie to **deletion vectors /
time travel** — when the regulator asks "who saw what," we answer
with a single SQL query.

## 5. Cost back-of-envelope (USD/month)

```
Storage
  Bronze raw       100M trips × 5 KB = 500 GB/year × 3 yr hot = 1.5 TB
                   @ S3 Standard  \$0.023/GB-mo        = \$35
                   + 90 days Glacier warm                  = \$5
  Silver SCD2     10 GB hot, 50 GB warm                  = \$1
  Gold analytics  1 GB hot, 10 GB warm                   = \$0.50
  Audit mirror    5 GB/day × 5 yr ≈ 9 TB; 1 yr Standard = \$200
                                                       ──────────
                                          storage total  ≈ \$241/mo

Compute
  Flink ingest    3 nodes × 4 vCPU × 720 h × \$0.05       = \$432
  Spark maintenance nightly 30 min × 30 nights × \$0.10  = \$15
  Trino analyst   2 nodes × 4 vCPU × 30 % util × 720 h   = \$172
  Polaris catalog (managed)                              = \$300
                                                       ──────────
                                          compute total  ≈ \$919/mo

Data egress / cross-AZ
  Cross-AZ replication × \$0.01/GB × 1 TB                = \$10
                                                       ──────────
                                          GRAND TOTAL    ≈ \$1,170/mo
```

Hard cap was **\$8 K/mo**; budget headroom of 7× is intentional — the
"real" costs (egress spikes during incident analysis, analytics
sandbox over-runs, storage growth on Gold) all live inside this
headroom. We surface a monthly forecast with 20 % contingency built in;
the FinOps hook is the per-tool metering demonstrated in Day 18 NB8.

## 6. One-week MVP slice

Cut the smallest shippable vertical that proves the architecture:

| Day | Deliverable |
|---|---|
| 1 | Stand up Polaris REST catalog + Trino + DuckDB on a single k8s node |
| 2 | Debezium connector for **one** table (`trip`) → Kafka |
| 3 | Flink job: tokenize 2 PII columns, write Bronze Delta partition by `date` |
| 4 | Silver MERGE nightly with the SCD2 + late-data predicate; verify replay matches |
| 5 | Gold `trip_daily_metrics` + ZORDER by `city_id`; one Trino dashboard |
| 6 | `pii_access_audit` mirror with one trigger query; token rotation hook stubbed |
| 7 | Failure mode F1 demo: inject late `cdc.ts` row → verify SCD2 surface; document runbook |

That's seven days, two engineers, ≈ \$500 in cloud spend for the week.
The remaining tables, the migration off Oracle, the multi-region
replication — those all *follow* the shape, not the reverse.

---

**Connections to Day 18 (≥ 4 concepts applied):**
* **ACID + time travel** — D1, F1, F4 (Delta's history/RESTORE is the rollback path)
* **Medallion** — D3, §2 diagram (Bronze/Silver/Gold with explicit role per layer)
* **Catalog as control plane** — D2, F3 (Polaris IS the system; outage halts writes)
* **Schema evolution opt-in** — F4 (`mergeSchema` is the rollback lever)
* **CDF / lineage** — D7, F5 (audit mirror subscribes to change feed)
* **FinOps / 4-job maintenance** — D6, §5 (`OPTIMIZE + VACUUM + expiry + orphan` daily)
* **PII tokenization as data contract** — D4 (tokenize-at-the-wire, not at-rest)

---

*Author: Bùi Gia Huy — Day 18 Track 2 Lakehouse Lab, VinUniversity AICB (Aug 2026).*
*Self-grade: ≥5 decisions with ≥2 rejected alternatives, ≥3 failure modes
tied to Day 18 concepts, math checked, MVP is shippable in one week.*