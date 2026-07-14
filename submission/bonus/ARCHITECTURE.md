# Bonus — Lakehouse Architecture Brief

**Topic:** A — LLM API observability at **1B requests/day** (~**5 KB** average payload → **~5 TB/day** logical raw before compression; architected so **hot retention + aggregates** fit a **\$5 K/mo** storage budget and **5-minute** dashboard freshness).

**Author note:** Design review–style document. Numbers are order-of-magnitude defensible; tune with your cloud list prices before procurement.

---

## 1. Problem statement (≤ 200 words)

A multi-tenant foundation-model API must log every call for billing, reliability, and incident replay. Peak scale is **1B requests/day**, average **~5 KB** per record including headers, token usage, latency, status, and a truncated prompt fingerprint—roughly **5 TB/day** landing as newline-delimited or protobuf batches before compression. Product requires: (**1**) per-tenant **cost and latency** dashboards refreshed at most **5 minutes** behind real time; (**2**) **full prompt/response text** retained **7 days** for security investigations, then **only aggregates and sampled errors** for **365 days**; (**3**) **PII** (emails, phone numbers, government IDs occasionally pasted into prompts) must be **redacted or tokenized before any human or notebook** can read Bronze; (**4**) **total storage spend ≤ \$5 K/month**—a hard FinOps cap, not a guideline. The difficulty is not ingestion throughput alone; it is **retention tiering + query patterns + governance** without turning the lake into millions of tiny files or an unbounded full-text archive. A lakehouse with **ACID tables**, **time-bounded partitions**, **compaction**, and **catalog-enforced access** is the spine; **streaming + medallion** is the organizing idea.

---

## 2. Architecture diagram (single view)

```
                    ┌─────────────────────────────────────────────────────────────┐
                    │                    GOVERNANCE LAYER                          │
                    │  Unity Catalog / Polaris: ABAC by tenant; Bronze = LOCKED;  │
                    │  Silver readable by "analyst" role; Gold by "dashboard_bot" │
                    └─────────────────────────────────────────────────────────────┘
  [Tenants]──►API GW──►[Kafka/Pulsar]──►Flink/Databricks SS──┬──► Bronze Delta  (raw + hashes)
       ▲                         │                           │         │  partition: ingest_date,hour
       │                         │                           │         │  ZORDER: tenant_id, request_id
       │                         │                           └──► PII scrubber (sidecar)──► quarantine Delta (encrypted, break-glass only)
       │                         │
       │                         └──► Object lock WORM bucket (7d full prompt) ──lifecycle──► Glacier Deep Archive (compliance)
       │
       └─── BI tools / Grafana ◄── Trino/DuckDB ◄── Gold Delta ◄── dbt/Spark batch (5m)
                                              ▲                    │
                                              │                    └── Silver Delta  (typed, PII-tokenized)
                                              │                         MERGE upserts by request_id
                                              │                         partition: event_date
                                              └─── metadata: OpenLineage → Marquez
```

**Ingest path:** API → queue → stream processor writes **Bronze Delta** (append-only micro-batches with idempotent `request_id`), emits scrubbed copy to **Silver** via MERGE, and ships **aggregates** to **Gold**. Full prompt bodies optionally bypass the main table: **WORM object store** with **7-day expiry**, catalog holds pointer + hash only.

**Query path:** Dashboards and ad-hoc SQL hit **Gold** (wide, cheap) or **Silver** (narrow investigations) through Trino with **column masks**; Bronze is **deny-by-default** except break-glass roles.

---

## 3. Key decisions (with rejected alternatives)

**Decision 1 — Table format**

- **I chose managed Delta Lake on object storage** (S3/GCS) as the system of record for Bronze–Gold.
- **I rejected raw Parquet directories without a log** because we need **ACID concurrent writes**, **schema enforcement**, and **time travel** when a bad Silver MERGE ships—Day 18’s `RESTORE` / version pins are operational tools, not nice-to-haves.
- **I rejected Apache Iceberg as the only format** for v1 because our org already standardizes on **Delta + Unity Catalog** and dual-format ops (UniForm) adds migration tax before we have basic SLOs; Iceberg remains a **read bridge** later if Trino-heavy teams demand it.

**Decision 2 — Catalog**

- **I chose Unity Catalog (or Apache Polaris in a vendor-neutral future) with row/column filters and ABAC.**
- **I rejected “bucket IAM only”** because coarse IAM cannot express **“see latency but not prompt columns”** per role, and audit trails for PII reads become DIY.
- **I rejected Hive Metastore alone** because policy propagation and cross-engine consistency lag what we need for Decree-style audit stories in regulated extensions of this design.

**Decision 3 — Partitioning vs clustering**

- **I chose time partitions (`ingest_date`, `hour`) on Bronze + Z-order on `(tenant_id, request_id)` after compaction windows.**
- **I rejected `tenant_id` as the sole partition key** because **cardinality explosion** (millions of tenants) makes directory listing and metastore pressure worse than the small-file problem we are trying to solve.
- **I rejected unpartitioned Bronze** because time-pruned retention and lifecycle rules map cleanly to prefixes; legal holds can pin specific prefixes without scanning the whole bucket.

**Decision 4 — Full prompt retention**

- **I chose dual-path storage: Bronze keeps hashed fingerprint + length; full text lands in **WORM object** with 7-day TTL and KMS key per environment.**
- **I rejected keeping full prompts in Delta Bronze** for 7 days at this row width because **repeated compaction and clones** would blow the **\$5 K** cap even with ZSTD; object store lifecycle to IA/Glacier is cheaper per GB-month.
- **I rejected “no full prompt retention”** because incident response realistically needs replay; the compromise is **isolated, encrypted, short-lived** storage with strict break-glass logging.

**Decision 5 — Stream engine**

- **I chose Flink or Databricks Structured Streaming writing idempotent Delta sinks with `foreachBatch` + `mergeSchema` off where possible.**
- **I rejected only batch hourly loads** because the **5-minute** SLA for tenant dashboards implies **near-continuous** aggregate inputs; hourly batch misses the product contract without huge catch-up windows.
- **I rejected a second proprietary streaming DB** as primary store because we want **one physical copy** in the lake and **compute disaggregation**—Trino/DuckDB read the same Gold Delta.

**Decision 6 — Compression and file sizing**

- **I chose ZSTD on Parquet, target **~256 MB–1 GB** files post-compaction on hot paths, automated OPTIMIZE + Z-order nightly + adaptive micro-batch coalescing upstream.**
- **I rejected Snappy-only defaults** on cold paths where ZSTD’s CPU cost pays for fewer bytes under a hard monthly cap.
- **I rejected “optimize only when users complain”** because that is exactly the §5 **small-file / metadata inflation** failure mode NB2 measures.

---

## 4. Failure modes (≥ 3)

**F1 — Silver MERGE regression corrupts aggregates (wrong `when_matched` clause).**

- **3 AM symptom:** Gold error rate jumps; finance sees impossible token counts.
- **Detect:** dbt/Great Expectations tests on Gold row counts vs Silver checksums; anomaly alert on `ABS(cost_usd)` spikes.
- **Rollback:** **Delta time travel** to pin dashboards to `versionAsOf` / restore Silver to last known good version, then rebuild Gold partitions for affected hours (incremental backfill). *Tie to Day 18: time travel + MERGE.*

**F2 — Bronze schema drift from a new SDK field.**

- **3 AM symptom:** Stream starts routing rows to dead-letter; partial table writes fail schema checks.
- **Detect:** DLQ rate SLO; Flink checkpoint failures; `_delta_log` `AddColumn` events monitored.
- **Rollback:** Pause job; **opt-in schema evolution** with `mergeSchema` in a staging Bronze table; replay Kafka from committed offsets after validation—avoid silent `overwrite` of production Bronze.

**F3 — Small-file storm after traffic spike (new tenant onboarded with 10× burst).**

- **3 AM symptom:** p95 dashboard latency degrades; `numFiles` explodes; OPTIMIZE queue backs up.
- **Detect:** File-count metrics per table; Iceberg/Delta maintenance dashboards; cost anomaly on LIST requests.
- **Mitigation:** Emergency **higher target file size** compaction window, temporary **coalesce** in Flink, tenant-level **rate limits** upstream; not “add nodes forever.”

**F4 — PII leak via misconfigured Trino view.**

- **3 AM symptom:** Audit log shows `SELECT prompt_raw` by a role that should be masked.
- **Detect:** Catalog audit + query log classification.
- **Rollback:** Revoke view; rotate masking UDF; **restore** Silver snapshot if rows were materialized incorrectly (rare); notify legal.

---

## 5. Cost back-of-envelope (\$/month)

Assume **aggressive** compression and **tiering** so we are not storing 5 TB × 30 as hot.

| Layer | Assumption | Math (order-of-magnitude) |
|-------|--------------|-----------------------------|
| Hot Bronze (last 48h, ZSTD ~4×) | ~2.5 TB logical → ~0.6 TB on disk short window rolling | \$0.023/GB-mo S3 Standard × 600 GB ≈ **\$14** (rolling average low because short retention window) |
| Warm Silver (14d, ~3× compression) | Avg 1.5 TB physical | × \$0.023 ≈ **\$35** |
| Gold aggregates (narrow, 365d) | ~25 TB-month-equivalent stored compressed over year growth managed by rollups | × \$0.012 IA blended ≈ **\$300** |
| WORM prompts (7d at 5 TB/day peak but dedupe + text-only heavy compression ~10×) | ~0.5 TB average resident | Standard + lifecycle ≈ **\$12** |
| Glacier / Deep for legal year-2 | 50 TB archive | \$0.00099/GB × 50,000 GB ≈ **\$50** |
| **Compute** | Continuous Flink ~96 vCPU average + nightly OPTIMIZE | **\$3.5k–\$4.2k** (committed use / spot mix) |
| **Egress + LIST** | Cached; prefixes per day | **\$200** buffer |

**Rough total:** **~\$4.0k–\$4.8k/mo**—inside **\$5 K** if we enforce **prompt offload**, **Gold narrowing**, and **spot** for batch compaction. If compute creeps, first cut is **longer Gold refresh interval** for cold tenants, not storage panic.

*(Replace \$0.023 with your region’s S3 price; replace vCPU pricing with your contract.)*

---

## 6. One-week MVP slice

Ship **one production tenant** (or a shadow tenant) end-to-end: Kafka → **Bronze Delta** (partitioned by hour) with **schema enforcement**, **Silver MERGE** keyed on `request_id` with **tokenized PII UDF**, **Gold** table with **5-minute** rollups of `latency_ms`, `tokens_in/out`, `status`, **`cost_usd`** using a static price card. Wire **Grafana** to Gold via Trino. Add **three alerts**: DLQ rate, file count per table, Gold–Silver row checksum delta. Prove we can hit **5m** freshness and **\$5k** trajectory on measured GB-day—not slide fiction.

---

## PoC

See `submission/bonus/poc/bronze_pii_spike.ipynb`: deterministic tenant/user tokenization for raw JSON lines (mirrors Bronze landing hygiene before Silver).
