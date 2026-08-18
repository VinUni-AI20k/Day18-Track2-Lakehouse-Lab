# Architecture Brief — LLM Observability at 1B Requests/Day

Topic A. Author's note: this is a full draft, not a lightly-edited outline —
flag anything that doesn't survive scrutiny in review.

---

## 1. Problem Statement

A foundation-model API team logs every request/response: **1B req/day,
~5 KB/req ≈ 5 TB/day raw**. Requirements: (1) a cost & latency dashboard
per tenant, refreshed every 5 minutes; (2) full prompt/response retained
7 days for incident review, then only aggregates for 1 year; (3) PII
redacted before *anyone* reads the data, no exceptions; (4) total storage
spend ≤ $5K/month. The hard part isn't any one requirement — it's that they
fight each other: a 5-minute SLA rules out nightly batch, a 7-day full-detail
window at this volume is ~19 TB sitting hot at all times, and "redact before
anyone reads" means the redaction step is on the ingestion critical path,
not an afterthought layered on top later.

---

## 2. Architecture

```
                              INGESTION PATH
┌──────────┐   Kafka topic     ┌──────────────────────┐
│ API edge │ ──(~11.6K msg/s)─▶│ llm-requests-raw       │
│ (1B/day) │                   │ 5 KB/msg, 5 TB/day     │
└──────────┘                   └───────────┬────────────┘
                                            │ Structured Streaming, 1-min trigger
                                            ▼
                          ┌───────────────────────────────────┐
                          │ BRONZE  llm_calls_raw                │
                          │  • PII tokenized BEFORE commit       │  Delta
                          │  • partition = date                  │  cluster (Z-ORDER) = tenant_id
                          │  • 7-day hard partition-drop          │
                          └───────────────┬─────────────────────┘
                                            │ streaming dedup(request_id) + schema-enforce
                                            ▼
                          ┌───────────────────────────────────┐
                          │ SILVER  llm_calls                    │  Delta + Change Data Feed
                          │  • dedup on request_id                │  partition = date, cluster = tenant_id
                          │  • 7-day hard partition-drop          │  (gated — see Failure Mode 3)
                          └──────┬───────────────────┬───────────┘
                        CDF stream│                   │CDF stream
                                  ▼                   ▼
                   ┌────────────────────┐  ┌─────────────────────────┐
                   │ GOLD                 │  │ token → PII vault         │
                   │ tenant_5min_metrics  │  │ separate KV, KMS-encrypted,│
                   │ cost/p50/p95/errors  │  │ append-only, every read    │
                   │ 365-day retention     │  │ audited                    │
                   └──────────┬───────────┘  └─────────────────────────┘
                              │
                    QUERY PATH (dashboard, 5-min refresh)
                              ▼
                   ┌────────────────────┐
                   │ BI / dashboard        │  reads Gold ONLY —
                   │ (Superset/Looker)     │  never touches Bronze/Silver
                   └────────────────────┘

Incident review (≤7-day window): analyst queries Silver directly (still
tokenized) and requests vault de-tokenization through an audited service —
never ad-hoc SQL straight to the vault.
```

---

## 3. Key Decisions — What I Chose, What I Rejected, and Why

**1. Table format: Delta Lake.**
Rejected **Iceberg** — its win is multi-engine catalog interop (Spark, Trino,
Snowflake reading the same table via REST catalog), and this pipeline is
single-engine end to end (ingest and query both run on the same compute
platform), so I'd be paying Iceberg's catalog-governance overhead for a
benefit I don't use. Rejected **Hudi** — weaker Z-order-equivalent clustering
maturity for the tenant-filter hot path, and less team experience raises
operational risk at 1B req/day, where a maintenance-job bug is expensive to
debug live.

**2. Ingestion: Kafka → Structured Streaming micro-batch (~1 min trigger).**
Rejected **row-by-row writes per request**: this is the small-file problem
NB2 in this lab measured directly — 200K unbatched rows produced 200 files;
at 1B req/day that's millions of tiny files per day, and OPTIMIZE can't keep
up with that write rate. Rejected **batch-every-5-min via a scheduler
(Airflow-triggered Spark job)**: works, but couples freshness to job-startup
latency (JVM cold start ~1-2 min on triggered infra), leaving no buffer
inside a hard 5-minute SLA.

**3. PII: tokenize at Bronze, before commit — not masked at read time.**
Rejected **store raw, mask via a read-time view/ACL**: the raw PII is on
disk in Parquet; anyone with direct table access (DuckDB, Spark, a debugging
data engineer) bypasses the view and reads it raw. That directly violates
"redacted before anyone reads." Rejected **mask at Silver, keep raw
Bronze**: Bronze is exactly the layer engineers query most when debugging
ingestion — same leak, one hop later.

**4. Retention: hard 7-day partition-drop, not soft-delete + VACUUM.**
Rejected **soft-delete rows + rely on VACUUM to reclaim space**: NB6 in this
lab measured that `VACUUM` only diffs against the transaction log — 5 of 20
planted orphan files survived a vacuum run because they were never
committed. Relying on VACUUM as a compliance boundary ("guaranteed gone by
day 8") is provably unsafe with evidence from this lab's own output.
Partition-drop (delete the `date=X` directory, commit a metadata-only
remove) is deterministic. Rejected **S3 lifecycle rule deleting objects
outside Delta's control**: the transaction log wouldn't know the files are
gone — readers get 404s on files the log still lists as present, and time
travel silently breaks.

**5. Partitioning: `date` (daily), clustered by `tenant_id` within each day.**
Rejected **partition by `tenant_id/date`**: with thousands of tenants and
most sending well under 1 GB/day, this reproduces NB2's small-file problem
multiplied by tenant count. Rejected **no partitioning, single Z-order
across the whole table**: partition-by-date is what makes the 7-day-drop
retention job O(1) (drop a partition) instead of O(n) (rewrite/filter the
full table) — losing that turns the retention job itself into the
bottleneck.

**6. Gold aggregation: incremental streaming MERGE off Silver's CDF.**
Rejected **nightly full recompute**: incompatible with a 5-minute SLA, and
wastefully re-scans 5 TB/day of data that's already mostly unchanged.
Rejected **engine materialized views**: don't give the same
replay-from-a-specific-version guarantee a CDF-driven MERGE gives — which
matters directly for Failure Mode 1 below.

---

## 4. Failure Modes

**1. Streaming job falls behind Kafka; ingestion lag exceeds the 5-min SLA.**
*Detection:* alert on Kafka consumer-group lag (message count) and on
`now() − max(Silver.event_time)` exceeding 5 minutes.
*Rollback:* no data loss — Kafka retains ≥7 days, so scale out streaming
executors and let it catch up. If the incremental Gold aggregation
double-counted during the lag spike, this is exactly NB3's mechanic:
`RESTORE`/`versionAsOf` Gold to the version before the bad merge, then
replay the CDF stream from that Silver version forward.

**2. A deploy changes the Bronze schema without `schema_mode="merge"`.**
Ties directly to NB1. *Detection* is the failure mode itself being loud:
schema enforcement rejects the mismatched writes immediately (the same
mechanism that blocks NB1's `age=str` write) — a bad failure mode here would
be *silent* null-filling, which enforcement prevents by construction.
*Rollback:* `RESTORE` Bronze to the pre-deploy version, replay Kafka offsets
from that timestamp once the schema fix ships.

**3. Retention job drops a `date=X` Silver partition before Gold's CDF
consumer finished aggregating it — Gold under-counts a tenant's cost, and
nobody notices until the tenant disputes an invoice weeks later.**
*Detection:* reconciliation job comparing `sum(Gold.cost)` per date against
an independent counter (Kafka message count × avg cost, stamped into Bronze
commit metadata at ingest time); alert on drift > 0.5%.
*Rollback:* this one has none, by design of the 7-day TTL — once Silver's
partition is physically gone, Gold cannot be recomputed from source. The
control is prevention: the retention job carries a hard dependency gate —
never drop a partition until Gold's CDF watermark has passed it, plus a
24-hour safety buffer.

---

## 5. Cost Estimate (back-of-envelope)

Assumption: Parquet+Snappy on repetitive LLM request/response JSON
compresses ~3.5× versus raw text (typical for this shape of data).

- Bronze, compressed: 5 TB/day ÷ 3.5 ≈ **1.43 TB/day**
- Silver, compressed + deduped (~0.9× of Bronze): ≈ **1.29 TB/day**
- Rolling 7-day hot window (steady state — partitions drop as new ones
  land, so this doesn't grow past 7 days of data):
  (1.43 + 1.29) TB/day × 7 days ≈ **19.0 TB**
- Storage: 19,000 GB × $0.023/GB-month (S3 Standard) ≈ **$437/month**
- Gold, 1-year retention: ~5,000 tenants × 288 five-min buckets/day × 365
  days ≈ 526M rows/year × ~200 B/row ≈ 105 GB uncompressed → **~$1/month**
  compressed. Even multi-year Gold retention is pocket change next to hot
  storage.
- Streaming compute: sustained ~57.9 MB/s (11.6K req/s × 5 KB) needs a
  modest persistent Structured Streaming cluster — call it **$3,000-3,500/
  month**, the dominant line item.
- Scheduled maintenance jobs (compaction, partition-drop, reconciliation):
  **~$300/month**.

**Total ≈ $4,050-4,250/month**, under the $5K cap with ~$750-950/month of
headroom for the PII vault KV store and dashboard BI compute.

---

## 6. What I'd Build First (1-week MVP)

Single-tenant slice, replayed from a static log instead of live 1B/day
traffic: Kafka → Structured Streaming → Bronze (tokenized — with a
trivially-reversible mock tokenizer, not the real KMS-backed vault — clustered
by `tenant_id`, partitioned by `date`) → Silver (dedup + CDF) → Gold (5-min
cost/latency via streaming MERGE). Backdate synthetic partitions to compress
a week of retention into an hour of wall-clock time, and prove the
partition-drop gate actually blocks a drop when Gold's watermark hasn't
caught up (Failure Mode 3) — that gate is the one piece of this design that
is silently wrong if untested. Explicitly out of scope for week one: the
real PII vault, multi-tenant scale, and the audited incident-review read
path.
