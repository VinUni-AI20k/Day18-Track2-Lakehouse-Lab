# Architecture Brief — LLM Observability at 1B Requests/Day

**Topic A.** Team: foundation-model API platform, logging team.
**Role assumed:** architect on-call, defending this in a design review.

---

## 1. Problem statement

We log every request/response for a foundation-model API: **1B req/day,
~5 KB/req → 5 TB/day raw**. Requirements: (1) per-tenant cost & latency
dashboards refreshed every 5 min; (2) full prompt/response retained 7 days
for incident review, then aggregates only for 1 year; (3) PII redacted
before any human reads a row; (4) total **storage** spend ≤ $5K/month.

The hard part isn't storing 5 TB/day — it's storing it so that (a) a human
can never see raw PII even during an incident, (b) the 7-day cutover from
"full detail" to "aggregate only" is automatic and auditable, and (c) a
5-minute dashboard SLA survives write-amplification from 1B small,
bursty, per-request writes without blowing past $5K/month. Naively landing
1B individual writes/day produces a small-files catastrophe (see NB2)
before storage cost is even the binding constraint.

---

## 2. Architecture diagram

```
                              INGESTION PATH
 ┌────────────┐   ┌───────┐   ┌─────────────────────┐   ┌──────────────────┐
 │ API gateway│──▶│ Kafka │──▶│ Spark Structured     │──▶│ BRONZE (Delta)   │
 │ (per req)  │   │(topic │   │ Streaming            │   │ 24h rolling buf. │
 └────────────┘   │ per   │   │ foreachBatch→MERGE   │   │ RAW, PII intact  │
                   │ shard)│   │ micro-batch: 30s     │   │ access: pipeline │
                   └───────┘   └──────────┬───────────┘   │ svc-account only│
                                           │                └────────┬─────────┘
                                           │ same batch job           │ tokenize+
                                           │ (single hop, no          │ hash PII
                                           │ separate re-read)         ▼
                                           │                ┌──────────────────┐
                                           └───────────────▶│ SILVER (Delta)   │
                                                             │ 7-day rolling    │
                                                             │ PII TOKENIZED    │
                                                             │ cluster: Z-order │
                                                             │ on tenant_id     │
                                                             └────────┬─────────┘
                                              nightly batch job:      │
                                              1) OPTIMIZE+Z-ORDER     │
                                              2) drop partitions      │
                                              3) roll up → Gold       ▼
                                                             ┌──────────────────┐
                                                             │ GOLD (Delta)     │
                                                             │ p50/p95/cost/err │
                                                             │ per tenant×model │
                                                             │ ×5-min bucket    │
                                                             │ retained 1 year  │
                                                             └────────┬─────────┘
                              QUERY PATH                              │
 ┌──────────────┐   ┌───────────┐   ┌──────────────┐   ┌──────────────┴──────┐
 │ Grafana      │◀──│  Trino /  │◀──│  Gold table  │◀──│ 5-min micro-batch    │
 │ dashboards   │   │  DuckDB   │   │  (hot path)  │   │ append (not full     │
 │ (per-tenant) │   │           │   │              │   │ nightly rewrite)     │
 └──────────────┘   └───────────┘   └──────────────┘   └──────────────────────┘

 Incident review (rare, audited):
 on-call engineer ──▶ REST catalog ACL check ──▶ query SILVER (tokenized)
                       │
                       └─ if raw value needed: separate de-tokenization
                          service, logs (who, which row, why) to an
                          append-only audit Delta table — never a direct
                          Bronze read.
```

One diagram, but two paths worth calling out explicitly: the **write path**
never lets Bronze (raw PII) reach a query engine — Bronze is redacted in
the *same* streaming batch that lands it, not a later job, so there is no
window where raw PII sits queryable. The **read path** for dashboards never
touches Silver at all — Gold is small enough to be the only thing BI tools
see, which is also what keeps the 5-minute SLA cheap.

---

## 3. Key decisions, with rejected alternatives

**1. Table format: Delta Lake for Bronze/Silver, plain Delta for Gold too
(not a dual-format stack).**
I chose **Delta** end-to-end. I rejected **Iceberg** because its main edge
here — hidden-partition pruning for ad-hoc analytical queries — doesn't
pay for itself: our query path is 95% "read Gold, which is already tiny,"
and the other 5% is rare, ACL-gated incident review, not a BI workload that
needs partition-transform ergonomics. I rejected **Hudi** because the
team has no existing Hudi compaction-service operators, and at 1B req/day
the ingestion path (Spark Structured Streaming `foreachBatch`+`MERGE`) is
Delta's best-supported connector — introducing a second table format only
to get incremental gains on a query path we've already made cheap is not
worth the operational surface area.

**2. Where PII gets redacted: at Bronze landing, in the same streaming
batch, not later.**
I chose **tokenize/hash PII inside the same `foreachBatch` micro-batch**
that writes Bronze, so Silver is written in the identical pass. I rejected
**redact-at-query-time via a view** (a masking view over raw Bronze) because
it means raw PII is durably stored and queryable by anyone with Bronze
access — a permissions bug away from a real leak, and it fails requirement
(3) literally ("PII redact **before** any human reads a row" — a view is
not "before," it's "at read time"). I rejected **redact only in Gold**
because Gold is aggregate-only; that would mean Silver (used for the
7-day incident-review window, read by humans) still carries raw PII —
exactly the surface we're trying to close.

**3. Retention enforcement: automated partition-drop + `VACUUM`/expiry
cron, not manual purge.**
I chose a **scheduled job that drops date-partitions older than 7 days from
Silver and runs `VACUUM`** on a fixed retention. I rejected **manual
quarterly purge** — at 5 TB/day raw, a missed quarter is a compliance
incident, not a backlog item. I rejected **soft-delete flags kept forever
with app-level filtering** because NB6 showed directly that `VACUUM` only
reclaims what's *tombstoned in the log* — a soft-delete flag doesn't
tombstone anything, so "logically retired" rows keep costing storage
forever unless someone remembers a second cleanup step. Automating the
partition-drop is what actually turns 7-day retention into a storage
number instead of a policy on paper.

**4. Partitioning: by `date` only; cluster (Z-order) by `tenant_id` within
each date partition.**
I chose **date-partitioned, tenant-clustered**. I rejected **partition by
`tenant_id`** — tenant cardinality is in the thousands and highly skewed
(a handful of tenants dominate volume), so tenant-partitioning produces
either a small-files explosion for low-volume tenants or wildly uneven
partition sizes for high-volume ones — the exact failure mode NB2 measured
(200 small files → 55 after compaction). I rejected **partition by
`(date, tenant_id)`** for the same reason at higher cardinality. Z-order
clustering gets us "filter by tenant" pruning (the actual hot query
pattern for per-tenant dashboards) without paying the partition-count cost.

**5. Streaming engine: Spark Structured Streaming, not Flink or raw Kafka
Connect.**
I chose **Spark Structured Streaming with `foreachBatch` → Delta MERGE**.
I rejected **Kafka Connect S3 sink writing raw parquet directly** because
it has no ACID write semantics and no hook for the mandatory PII-redaction
step — files land, then a *separate* job would have to redact, reopening
the exact "raw PII briefly queryable" window decision 2 rejected. I
rejected **Flink** — its Delta connector is less mature than Spark's for
`MERGE`-based upserts (needed for the dedup pattern NB4 relies on), and
adding a second streaming platform the team doesn't already operate is not
justified when Spark already covers the batch compaction jobs too.

**6. Compaction/clustering cadence: scheduled batch (hourly compaction,
nightly Z-order), not auto-optimize on every write.**
I chose **decoupled scheduled maintenance jobs**. I rejected
**auto-optimize on every micro-batch write** — at 30-second micro-batches
and 1B req/day, compacting after every batch means near-continuous
rewrite I/O competing with ingestion for the same cluster; the cost of
compaction would scale with write frequency instead of with data volume.
I rejected **no compaction at all** — NB2's own numbers (200 files →
55 after `OPTIMIZE`+`ZORDER`, 13.1× speedup) are a direct, measured preview
of what an uncompacted 30-second-microbatch table looks like at this
scale: the dashboard's 5-minute SLA would degrade within hours, not days.

---

## 4. Failure modes

**(a) Redaction job crashes mid-batch, committing un-redacted rows to
Silver before the crash is caught.** *Ties to Day 18 time travel.*
Detect: a continuous PII-pattern scanner (regex/entropy check for
phone/email/ID shapes) runs against every new Silver commit within
minutes; any hit pages on-call. Rollback: Delta **`RESTORE` to the last
version before the bad commit** (exactly NB3's mechanism), then replay
the redaction step from the Bronze Kafka offset checkpoint — Bronze still
has the raw data for the 24h buffer window, so replay is possible without
data loss.

**(b) Traffic spike (3–5× during a viral event) produces a small-files
explosion in Silver, degrading the 5-minute dashboard SLA.**
Detect: track average file size / files-written-per-batch as a metric;
alert if avg file size drops below a threshold (the NB2 lesson, applied as
a live guardrail instead of a one-time notebook exercise). Mitigate:
trigger an out-of-band compaction job immediately rather than waiting for
the nightly schedule, and temporarily widen the micro-batch interval
(30s → 2min) to trade dashboard freshness for write amplification until
the spike subsides.

**(c) A client SDK update adds a new request field (e.g., `tool_calls`),
breaking strict Bronze schema enforcement mid-stream.** *Ties to Day 18
schema enforcement/evolution.*
Detect: schema-enforcement rejections spike in the streaming job's
dead-letter sink; alert on dead-letter volume, not just job failure (a
naive `mergeSchema=true` everywhere would silently accept the field and
mask the SDK change from anyone). Rollback/fix: schema changes go through
a **canary tenant** first — the field is added via `overwriteSchema` in a
controlled release, not blind auto-merge in production, so we control
*when* the schema changes rather than reacting to whichever tenant's SDK
updated first.

**(d) Retention job has an off-by-one on the UTC day boundary and drops
the wrong Silver partition, destroying a day of source data before it
rolled up to Gold.**
Detect: a daily row-count anomaly monitor on both Silver and Gold catches
an unexpected drop same-day. Rollback: Gold retention is 1 year and Gold
is cheap (aggregates, not raw), so Gold is replicated cross-region as a
second copy specifically because it's the layer we can least afford to
lose; Silver's lost partition, if caught within the 7-day window, can
still be re-derived from Bronze's most recent data if the crash landed
within the 24h raw buffer — otherwise it's accepted as unrecoverable,
which is why the drop-job runs against a computed partition list
double-checked by a row-count assertion *before* deleting, not a raw date
arithmetic call.

---

## 5. Cost back-of-envelope

**Storage (the capped line — target ≤ $5K/month):**

| Layer | Retention | Raw size | After ~5× zstd (JSON/text) | Cost @ $0.023/GB-mo (S3 Standard) |
|---|---|---|---|---|
| Bronze (raw, PII intact) | 24h rolling buffer | 5 TB | ~1 TB | ~$23/mo |
| Silver (tokenized, full detail) | 7-day rolling | 35 TB | ~7 TB | ~$161/mo |
| Gold (aggregates only) | 1 year | negligible (rows = tenants × models × 288 five-min buckets/day × 365) | < 1 GB | ~$0.02/mo |
| **Total steady-state** | | | **~8 TB** | **~$185/month** |

That clears the $5K/month cap with ~27× headroom — which is the point:
the cap is easy *if* Bronze never accumulates past its 24h buffer and
Silver never accumulates past 7 days. The actual risk to the budget isn't
steady-state storage, it's a **stuck retention job** silently letting
Bronze or Silver grow unbounded (failure mode d) — worth a budget alert
independent of the anomaly-row-count monitor, since a slow storage-growth
trend won't page anyone on its own.

**Compute (not capped by the brief, estimated for completeness):**
Streaming cluster sized for ~11.6K req/sec average (1B/day), ~40–60K
req/sec peak: a modestly-sized always-on Structured Streaming job,
reserved-instance pricing, ballparked at **$3–5K/month**. Nightly
compaction/Z-order batch jobs, spot-priced, a few hundred $/month. Total
infra (storage + compute) lands around **$3.5–5.5K/month** — the storage
line is a rounding error against compute at this request volume, which is
the realistic finding: FinOps pressure at 1B req/day is a compute problem
first, storage second.

---

## 6. What I'd build first (1-week MVP)

Not the streaming pipeline — the **redaction + retention mechanism**,
proven end-to-end for one tenant, offline:

1. Batch-ingest one day of sample request/response JSON into Bronze
   (Delta), including a couple of synthetic PII-shaped fields.
2. Write the tokenize-at-landing step as a pure function + a `MERGE` into
   Silver, with the PII-pattern scanner running against the Silver output
   as an automated check (not a demo — an assert, same spirit as NB6/NB7's
   pass-criteria blocks).
3. Roll Silver up into a Gold daily-aggregate table (p50/p95/cost/error
   rate) — the exact shape NB4 already builds.
4. One dashboard panel (even DuckDB + a notebook chart) reading only Gold.

This proves the two things the whole design bets on — PII never reaches a
human-readable row, and Gold stays small enough to be the only thing a
dashboard touches — without yet building Kafka ingestion, multi-tenant
scale, or the incident-review de-tokenization service. Those are additive
once the core mechanism is trusted; getting the core mechanism wrong is
the failure mode that actually matters.
