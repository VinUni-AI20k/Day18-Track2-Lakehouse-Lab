# Bonus Challenge — Topic A: LLM Observability at 1B Requests/Day

## 1. Problem statement

A foundation-model API team logs every request/response: **1B req/day,
~5 KB/req → 5 TB/day raw**. Four hard constraints, in tension with each
other: (1) a per-tenant cost & latency dashboard that refreshes every
5 minutes; (2) full prompt/response text kept 7 days for incident review,
then **only aggregates** for 1 year; (3) PII must be redacted **before any
human reads it**, not just before it's "final"; (4) total storage spend
≤ **$5K/month**. The tension: (1) wants fast, cheap reads over a
5 TB/day firehose; (2) + (4) together forbid keeping raw text past a week
(5 TB × 365 ≈ 1.8 PB/year is not a $5K/month problem); (3) means redaction
can't be a batch job that runs "eventually" — it has to sit in the write
path, which adds latency and failure surface to every ingest. This is a
medallion-layout, retention-policy, and PII-tokenization problem stacked
on top of a small-files problem, because 1B req/day arrives as millions
of tiny streaming commits.

## 2. Architecture diagram

```
 Tenant SDKs (1B req/day, 5 KB avg)
        │  gRPC/HTTP, ~11.6M req/min avg, ~3x peak
        ▼
 ┌────────────────────┐
 │ Ingest Gateway      │  rate-limit per tenant · buffer to ephemeral
 │ + PII Scrubber      │  landing (TTL 1h) · tokenize phone/email/ID
 └─────────┬──────────┘  BEFORE first Delta commit (constraint 3)
           │ micro-batch, 30-60s
           ▼
 ┌────────────────────────────── BRONZE (Delta) ───────────────────────┐
 │ llm_calls_raw   partitioned by date/hour                            │
 │ tokenized prompt/response, schema-enforced (blocks malformed SDKs)  │
 │ hourly OPTIMIZE (small-file job — NB2 lesson)                       │
 └─────────┬─────────────────────────────────────────────────────────┘
           │ MERGE dedup (retry-safe) + parse tenant_id/model/cost/latency
           ▼
 ┌────────────────────────────── SILVER (Delta) ───────────────────────┐
 │ llm_calls        partitioned by date, Z-ORDER by tenant_id          │
 │ full tokenized text · retention = 7 days (partition-drop, not DELETE)│
 └─────────┬─────────────────────────────────────────────────────────┘
           │ incremental 5-min aggregation job (foreachBatch/MERGE)
           ▼
 ┌────────────────────────────── GOLD (Delta) ─────────────────────────┐
 │ tenant_5min_metrics   partitioned by date                           │
 │ p50/p95 latency, cost_usd, error_rate, req_count · retained 1 year  │
 └─────────┬─────────────────────────────────────────────────────────┘
           │ read-only, MB-scale
           ▼
   BI / dashboard (5-min refresh, reads Gold ONLY — never scans Silver)

 Side path: audit table logs every Silver read that touches tokenized
 fields (who, when, which tenant) — governance requirement, constraint 3.
```

## 3. Key decisions, with alternatives rejected

1. **Table format: Delta Lake.** Rejected **Iceberg** — hidden partitioning
   is attractive, but this workload is streaming-write-heavy with frequent
   `MERGE` (dedup, late retries) and needs Change Data Feed for the
   redaction-backfill failure path (§4); Delta's streaming/CDF story is more
   mature than pyiceberg's for a Python-first ingest team. Rejected **raw
   Parquet + Hive tables** — no ACID means a concurrent streaming writer can
   leave a dashboard reader mid-write; no schema enforcement means one bad
   tenant SDK corrupts the shared table (this lab's NB1 §3 is exactly that
   failure, reproduced deliberately).

2. **Partition/cluster key: date+hour partitions, Z-ORDER by `tenant_id`.**
   Rejected **partition by `tenant_id`** — with thousands of tenants this
   creates thousands of tiny partitions per micro-batch, i.e. the NB2
   small-files problem multiplied by tenant count. Z-ORDER gives the
   "filter by tenant" speedup NB2 measured (7× wall-clock / 55× file-pruning
   in this lab) without the partition-count explosion, and date-partitioning
   is what makes the 7-day retention job a partition-drop, not a row scan.

3. **PII handling: tokenize at the gateway, before the first Bronze commit.**
   Rejected **encrypt-at-rest, decrypt-on-read** — technically "protects"
   the data but literally violates constraint 3 (anyone with table access
   still *reads* PII, just via a decrypt call); also blocks column pruning
   in Silver/Gold since every reader needs the key. Rejected **redact only
   at Gold** — Silver (used for incident review) would still expose raw
   PII to the review team, and audits require proving PII never reached
   durable storage un-tokenized, not just that it was cleaned up later.

4. **Retention mechanism: whole-partition drop at day=7, not row-level
   `DELETE`.** Rejected **row-level `DELETE WHERE date < now()-7d`** —
   at 1B rows/day this is a full-table rewrite of tombstones daily, and
   this lab's NB6 measured exactly why that's expensive: `VACUUM` only
   reclaims what the log has tombstoned, and a huge tombstone set inflates
   the log itself. Dropping the whole `date=YYYY-MM-DD` partition is one
   small commit. Rejected **S3 lifecycle → Glacier after 7 days** instead of
   deleting — Parquet files in Glacier are not queryable and the row data
   still legally exists past its retention window; tiering is not deletion.

5. **Dashboard reads Gold only, never Silver.** Rejected **ad-hoc query
   engine (Trino) scanning Silver every 5 min per tenant** — Silver is a
   35 TB rolling window; scanning any slice of that every 5 minutes across
   thousands of tenant dashboards blows the compute half of the $5K/month
   budget by itself. Pre-aggregating to Gold (MB-scale, §5) turns "refresh
   every 5 min" into a cache-hit, not a scan.

6. **Catalog: one governed catalog per environment, tenant as a row-level
   ACL dimension.** Rejected **catalog-per-tenant** — clean isolation, but
   at thousands of tenants it makes cross-tenant FinOps rollups (the
   dashboard's whole purpose) require a fan-out query across thousands of
   catalogs; a single catalog with tenant-scoped grants gives the same
   isolation guarantee with one place to run maintenance jobs (NB6's Job
   1-5) and one lineage graph.

## 4. Failure modes

1. **3 AM — malformed SDK update.** A tenant ships a client that sends
   `latency_ms` as a string. *Detect:* schema enforcement rejects the write
   (NB1 mechanism) → dead-letter-queue depth alarm fires within one
   micro-batch interval (~1 min). *Rollback:* route rejects to a DLQ table,
   page on-call; if the field is a legitimate new type, evolve schema with
   `schema_mode="merge"` after review — never widen the schema
   automatically from an untrusted producer.

2. **Redaction gap.** The scrubber's regex misses a new national-ID format
   for 3 hours before a compliance canary (a scheduled job that samples
   Bronze for known-PII patterns post-tokenization) catches it. *Detect:*
   canary alert. *Rollback:* use `history()` to bound the exact commit
   range affected (Day 18 time-travel concept), targeted-delete those Silver
   rows, re-run the fixed scrubber against the still-in-TTL ephemeral raw
   buffer to re-tokenize and re-land them, rotate/revoke any credentials the
   exposed text may have contained, file the incident report with the exact
   version range as evidence.

3. **Small-file storm.** An outage causes one tenant's SDK to retry
   aggressively, producing hundreds of thousands of near-empty commits in an
   hour. Hourly OPTIMIZE falls behind; Gold's 5-min aggregation job (which
   reads recent Silver) slows from p95 200 ms to 12 s, breaching the
   dashboard SLA. *Detect:* `numFiles`-per-partition metric alarm (this is
   the exact NB2 measurement, alarmed instead of eyeballed). *Rollback:*
   emergency out-of-band `OPTIMIZE`, rate-limit the noisy tenant at the
   gateway; longer-term, add per-tenant backpressure.

4. **Retention job off-by-one.** The nightly partition-drop job has a bug
   and drops `date=T-6` instead of `date=T-8`, destroying a day of data
   still inside the 7-day compliance window mid-incident-review. *Detect:*
   pre-delete row-count assertion comparing the planned partition list
   against `history()` before the commit executes; a mismatch blocks the
   job. *Rollback:* Delta `RESTORE` to the version immediately before the
   erroneous drop (Day 18 time-travel/RESTORE, the same mechanism NB3
   exercises) — safe because each day's drop is its own isolated commit,
   so the blast radius is exactly one version.

## 5. Cost back-of-envelope (target: ≤ $5K/month)

**Storage** (Parquet+snappy on LLM text compresses ~2× conservatively):

| Layer | Volume | Rate | Cost/mo |
|---|---|---|---|
| Bronze+Silver hot (7-day rolling, 5 TB/day raw → ~2.5 TB/day compressed × 7) | ~17.5 TB steady-state | $0.023/GB-mo (S3 Standard) | ≈ $412 |
| Gold (2,000 tenants × 5 models × 288 5-min buckets/day × 365 days × ~30 B/row compressed) ≈ 31 GB | 31 GB | $0.023/GB-mo | ≈ $1 |
| `_delta_log` + checkpoints (bounded by Job 5 checkpointing, else this line grows unbounded) | ~10 GB | $0.023/GB-mo | ≈ $1 |
| **Storage subtotal** | | | **≈ $414** |

**Compute** (spot-priced, ≈$0.08/vCPU-hr):

| Job | Size | Cadence | vCPU-hr/mo | Cost/mo |
|---|---|---|---|---|
| Streaming ingest + scrub (24/7) | 32 vCPU | continuous, 730h | 23,360 | ≈ $1,869 |
| Hourly OPTIMIZE (Bronze/Silver) | 16 vCPU | 15 min × 24/day × 30d | 2,880 | ≈ $230 |
| 5-min Gold aggregation | 8 vCPU | 2 min × 288/day × 30d | 2,304 | ≈ $184 |
| Compliance canary + audit job | 4 vCPU | 10 min/hr × 24 × 30d | 480 | ≈ $38 |
| Dashboard serving (reads Gold only) | small | — | — | ≈ $30 |
| **Compute subtotal** | | | | **≈ $2,351** |

**Total ≈ $2,765/month** — 45% headroom under the $5K cap for egress,
retries, and tenant-count growth. The single biggest lever if the cap gets
tight is the always-on ingest cluster ($1,869/mo of the $2,765); it's the
first thing to right-size or move to reserved capacity.

## 6. What ships first (1-week MVP)

Smallest slice that proves the architecture, not the whole thing: replay
**one real tenant's traffic at 1% sample** (≈10M req/day) through gateway
→ tokenized Bronze → deduped/Z-ordered Silver (7-day partition-drop
retention wired up for real, not simulated) → 5-min Gold rollup → a
single DuckDB-backed dashboard reading Gold only. Success = three things
proven end-to-end: (a) a known-PII test string never appears un-tokenized
in Silver, (b) dropping a day-partition removes exactly that day's rows and
`RESTORE` recovers from a deliberately-wrong drop, (c) the dashboard query
against Gold stays under 2 s. If those three hold at 1% scale, the
architecture is right and the rest is capacity, not design.
