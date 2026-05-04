# ARCHITECTURE — LLM Observability at 1B req/day

**Topic:** A. LLM observability ở quy mô 1B requests/ngày

**Author:** (Generated draft)

---

## 1) Problem statement (≤200 words)

A foundation-model API receives 1 billion requests per day (~5 KB per request → ~5 TB/day raw). Requirements:
- Real-time dashboards by tenant (cost & latency) refreshed every 5 minutes.
- Full request/response retention for 7 days for incident review; thereafter keep aggregates for 1 year.
- PII must be redacted/tokenized before any human can read raw content.
- Storage budget cap ≤ $5,000/month.

Challenges: ingesting 5 TB/day reliably, storing high-TPS metadata for fast OLAP (5-minute freshness), balancing raw retention vs FinOps (tiering + compaction), ensuring tokenization before any downstream access, and supporting time-travel/restore for incident investigations.

---

## 2) Architecture diagram (single dense view — ASCII)

Bronze (raw landing) <-- Streaming Ingest (Kafka/HTTP) --> Bronze Bucket (Delta on S3)
                                 |
                                 v
                           Tokenization Service (stateless workers)
                                 |
                                 v
              Silver (PII-redacted, parquet/delta partitioned by date, tenant)
                                 |
                                 v
               Gold (aggregates: p50/p95 latency, cost_usd, error_rate — daily)

Control plane: Catalog (Delta + Glue/REST), Metadata store (Postgres), Lineage (OpenLineage)
Ops: Lambda/Batch optimize jobs, cold tier lifecycle to Glacier, monitoring dashboard (Superset/Quicksight)

Query patterns:
- Hot: last 7 days, tenant-filtered dashboards → DuckDB/Deltalake or Presto on object store with Z-ordering
- Warm: 7–90 days → warehouses with partition pruning
- Cold: >90 days → archived aggregates only

---

## 3) Core design decisions and alternatives (5+)

1. Table format: **Choose Delta Lake on S3**.
   - Rationale: ACID, time-travel, strong ecosystem (delta-rs, delta-spark), supports schema evolution and RESTORE needed for incidents.
   - Rejected: Iceberg — good but Delta has simpler ACID+time-travel semantics for our tooling. Rejected: Proprietary DB (too costly at scale).

2. Ingest: **Streaming ingest via Kafka + exactly-once producers → write to Bronze Delta**.
   - Rationale: 1B/day requires buffering and backpressure. Kafka supports partitioning by tenant for fanout and ordering.
   - Rejected: Direct HTTP-to-object-store writes (no buffering, retries harder). Rejected: Only micro-batches (higher lateness).

3. PII handling: **Tokenize at Bronze landing using stateless workers + deterministic reversible tokenization with HSM-backed key**.
   - Rationale: Must redact before any human can read; deterministic tokens allow join and dedup while protecting data.
   - Rejected: Client-side tokenization (can't enforce across clients). Rejected: Naive hashing (collision/compliance issues).

4. Partitioning & clustering: **Partition by ingestion_date (day) + Z-order by (tenant, model_id, request_id)**.
   - Rationale: Tenant is primary filter for dashboards; Z-order helps file pruning for tenant-scoped queries.
   - Rejected: Heavy sharding by tenant (too many small partitions); Rejected: No clustering (poor IO for per-tenant queries).

5. Compaction/Optimize cadence: **Daily compaction + weekly Z-order optimize; immediate micro-commit for metadata**.
   - Rationale: Keeps small-file problem manageable while limiting compute cost.
   - Rejected: Continuous compaction (expensive); Rejected: No compaction (query slow).

6. Retention & lifecycle: **Raw raw-tier for 7 days (S3 Standard), warm-tier aggregates for 1 year (S3 IA/Glacier Deep Archive for >90d), cold raw snapshots kept only when needed (on-demand restore)**.
   - Rationale: Meets compliance & cost cap.
   - Rejected: Keep raw indefinitely (too costly). Rejected: Keep only aggregates (breaks incident reviews).

7. Catalog & metadata: **Unified Delta tables registered in Glue Catalog + lightweight Postgres for operational metadata + OpenLineage for lineage**.
   - Rationale: Cross-tool interoperability (Presto, DuckDB), governance, and lineage queries.
   - Rejected: No catalog (hard to discover tables); Rejected: Single-vendor catalog lock-in.

---

## 4) Failure modes (≥3) + detection + rollback

1. Failure: Tokenization service bug leaks PII into Silver.
   - Detect: Daily automated sampling + compliance unit tests that verify token format and sample counts; alert if unredacted fields found.
   - Rollback: Use Delta time-travel to RESTORE Silver to prior version (time-travel) and reprocess Bronze after fixing tokenization; for safety, revoke human access until re-validated.

2. Failure: Bad schema write (client sends malformed model schema causing downstream failures).
   - Detect: Write-time schema enforcement at Bronze (Delta) rejects incompatible writes; alert stream of write failures.
   - Rollback: Fix client schema / run opt-in mergeSchema path with manual review; use time-travel + metadata audit to revert broken commit.

3. Failure: Accidental bulk delete or corruption during maintenance.
   - Detect: Lineage & checksum monitoring; anomaly alert for sudden row-count drops or large tombstone counts.
   - Rollback: RESTORE affected table to last-good version (Delta) within minutes; re-apply legitimate commits after replay.

4. Failure: Cost spike due to runaway ingestion from a tenant.
   - Detect: Budget guardrails using streaming cost estimator; real-time alert when projected monthly cost for tenant exceeds threshold.
   - Mitigation: Throttle tenant ingestion at API gateway; automatically move older raw data to cheaper tier; notify tenant owner.

At least one failure ties to Day18 concepts: we rely on Delta time-travel + RESTORE for rollback and history auditing; schema enforcement blocks bad writes.

---

## 5) Back-of-envelope cost estimate (monthly)

Assumptions:
- Raw: 5 TB/day → 150 TB/month incoming. We keep full raw 7 days → ~35 TB raw stored.
- Silver (redacted): after dedup/compression assume 60% of raw → 21 TB for 7-day window.
- Aggregates (Gold & 1-year warm): ~1 TB hot + 5 TB warm indexes.

Storage pricing (S3-like averages):
- S3 Standard: $0.023/GB-month → $23/TB-month
- S3 IA / Infrequent: $0.0125/GB-month → $12.5/TB-month
- Glacier Deep Archive (cold): $0.00099/GB-month → $1/TB-month (approx)

Storage cost:
- Raw 35 TB @ Standard (7-day window): 35 TB * $23 = $805/month
- Silver 21 TB @ Standard: 21 * $23 = $483/month
- Gold + warm 6 TB @ IA: 6 * $12.5 = $75/month
- Cold snapshots & backups (assume 25 TB archived in Glacier): 25 * $1 = $25/month
Total storage ≈ $1,388/month

Compute + other costs (ingest brokers, tokenization workers, query engines): estimate ≈ $2,000/month (lightweight fleet + Presto/Trino   autoscale)

Monitoring, catalog, and egress budget: ≈ $500/month

Grand total ≈ $3,888/month — comfortably under $5,000 cap with margin for traffic growth.

Notes: aggressive compression or storing Silver in IA after 7 days reduces cost further.

---

## 6) MVP (one-week slice)

Goal: Show core feasibility with minimal components in 1 week.

MVP scope:
1. Implement streaming ingest stub that writes sample events to Bronze Delta (local `_lakehouse/bronze/llm_raw`).
2. Implement tokenization worker that reads Bronze commits, applies deterministic tokenization, writes to Silver.
3. Produce a small Gold aggregation job computing p50/p95 latency by tenant for the past day.
4. Add a simple dashboard snapshot (CSV) and a script demonstrating `delta.history()` and `delta.restore()` for a simulated bad commit.

Deliverables: `submission/bonus/ARCHITECTURE.md` + `submission/bonus/poc/tokenization_spike.py` (spike showing tokenization and reprocessing flow).

---



