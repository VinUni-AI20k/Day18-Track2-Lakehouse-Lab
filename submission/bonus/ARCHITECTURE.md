# Bonus — Topic C: Ride-hailing CDC → Lakehouse under Decree 13/2023/NĐ-CP

*Architecture decision record. Written to be defended in a design review, not
to be admired.*

---

## 1. Problem statement

A Vietnamese ride-hailing operator runs bookings on a production Oracle
cluster. Analytics currently reads a nightly dump; the business now wants
operational analytics without touching the OLTP box.

**Scale.** 100 M trips/year ≈ 274 K trips/day. Trip lifecycle emits ~8 CDC rows
per trip (requested → matched → arrived → started → ended → paid → rated →
corrections) ≈ 2.2 M rows/day. GPS telemetry dominates volume: one ping / 4 s
over a ~20-minute median trip ≈ 300 pings/trip ≈ **82 M pings/day**. Peak
concentration during two rush hours and rain events drives **30 K writes/sec**
against a ~1 K/s daily mean — a **30× peak factor**, which is the number that
actually sizes the system.

**Constraints.**
* Dashboards must reflect a source commit within **60 s**.
* Ad-hoc analyst queries **p95 < 1 s**.
* Driver and rider **PII** — phone, national ID (CCCD), GPS traces — falls under
  **Decree 13/2023/NĐ-CP**: consent, purpose limitation, data-subject access and
  erasure, and a cross-border transfer dossier for anything leaving Vietnam.
* **Late-arriving events are normal**, not exceptional: 4G coverage in remote
  provinces drops, and the driver app buffers and replays hours later.

**Why it is hard.** The three constraints fight each other. 60-second freshness
wants tiny frequent writes; p95 < 1 s wants large clustered files; Decree 13
wants row-level erasure inside an immutable, time-travelling log. Any design
that satisfies two and ignores the third is the wrong answer.

---

## 2. Architecture

```
 SOURCE                 INGEST                     LAKEHOUSE (Delta, S3-compatible, VN region)              SERVE
 ──────                 ──────                     ────────────────────────────────────────────             ─────
                                                   ┌──────────────────────────────────────────┐
┌────────────┐  redo   ┌──────────┐   Kafka       │ BRONZE  append-only, no updates ever      │
│  Oracle    │ ─logs─▶ │ Debezium │ ─3 topics──┐  │  trips_cdc/     dt=YYYY-MM-DD/hh          │
│  (OLTP)    │         │ XStream  │  keyed by  │  │  gps_pings/     dt=…/hh                   │
│  prod      │         └──────────┘  trip_id   │  │  payments_cdc/  dt=…/hh                   │
└────────────┘                                 │  │  ▲ tokenize-on-landing: phone/CCCD/name   │
                                               │  │  │ replaced by HMAC token BEFORE first    │
┌────────────┐   MQTT   ┌──────────┐           │  │  │ write. Raw PII never lands on disk.    │
│ Driver app │ ───────▶ │ Gateway  │ ──────────┤  ├──┼───────────────────────────────────────┤
│ GPS + late │  buffer  │ +schema  │           │  │ SILVER  MERGE, 30 s micro-batch           │
│  replay    │  replay  │ registry │           │  │  trips           part: trip_end_date      │
└────────────┘          └──────────┘           │  │                  cluster: (city_id,       │
                                               │  │                            driver_token)  │
     ┌──────────────┐                          │  │  driver_dim      SCD-2 (valid_from/to)    │
     │  KMS         │ token key kv2 ───────────┼─▶│  gps_tracks      part: dt, cluster: trip  │
     │ (VN, HSM)    │ per-row token_key_v      │  │  quarantine      > 7-day watermark        │
     └──────────────┘                          │  ├───────────────────────────────────────────┤
                                               │  │ GOLD  60 s refresh, pre-aggregated        │
     ┌──────────────┐                          │  │  trip_metrics_5m   city × 5-min bucket    │
     │ token vault  │ token → ciphertext       │  │  driver_daily      driver_token × day     │
     │ (crypto-     │ (only path back to PII;  │  │  surge_liveboard   city × 1-min bucket    │
     │  shred on    │  separate ACL, every     │  └──────────────────────────────────────────┘
     │  erasure)    │  read audited)                    │                    │
     └──────────────┘                                   ▼                    ▼
                                                  ┌───────────┐        ┌───────────┐
     ┌──────────────────────────────────────┐     │ Trino     │        │ Superset  │
     │ REST catalog (Polaris) + audit table │◀────│ ad-hoc    │        │ dashboards│
     │ every PII-column read logged, 2 yr   │     │ p95 < 1 s │        │ 60 s      │
     └──────────────────────────────────────┘     └───────────┘        └───────────┘

 MAINTENANCE (own team, own on-call):  compaction hourly · Z-ORDER/cluster nightly ·
 checkpoint every 100 commits · VACUUM 30 d + orphan sweep (set-diff, 24 h age guard)
```

---

## 3. Key decisions, with rejected alternatives

### D1 — Table format: **Delta Lake**

*Rejected **Iceberg**:* our write pattern is a high-rate keyed upsert with
late-arriving corrections. Iceberg v2 merge-on-read handles the write rate, but
equality deletes read-amplify exactly where our p95 < 1 s budget lives, and the
delete-file compaction cadence becomes a second tuning problem we would own at
3 AM. Delta's deletion vectors give us the same write-side cheapness with a
read path our engines already optimise for.

*Rejected **Hudi**:* technically the best fit for 30 K writes/sec — MOR plus the
record-level index is precisely this workload. It loses on operations: the
timeline service, compaction scheduling and cleaner policies are a third
control plane, and we have four data engineers, not fourteen.

*What would flip this:* if we needed Snowflake and BigQuery to read the same
tables natively, Iceberg's catalog reach would outweigh the delete-file cost,
and I would re-run this decision.

### D2 — Ingestion: **Debezium → Kafka → Spark Structured Streaming, 30 s micro-batch**

*Rejected **Debezium lakehouse sink connector** (straight to object storage):*
no replay buffer. When Silver's MERGE fails at 03:00, Kafka's 72-hour retention
is what lets us reprocess; a direct sink means the only replay source is Oracle
redo logs, which are retained for hours and whose re-read the DBA team will veto.

*Rejected **hourly batch snapshot off a read replica**:* misses the 60 s SLA by
60×, and snapshots lose intermediate states — a trip that was cancelled and
rebooked inside the hour appears as one row, which silently corrupts the
cancellation metrics the ops team steers on.

*Rejected **continuous / 1-second trigger**:* this is the anti-pattern the lab
measured. At 30 K events/s a 1 s trigger produces 86,400 commits/day; NB6 showed
200 commits of that shape yielding 51.5 KB average files and a request bill 50×
the compacted equivalent. 30 s is chosen as the largest interval that still fits
the 60 s SLA with the Gold refresh inside it.

### D3 — Partitioning: **Bronze by `dt`/hour; Silver by `trip_end_date` + clustering on `(city_id, driver_token)`**

*Rejected **partition by `driver_id`**:* ~200 K active drivers ⇒ 200 K
directories, each holding kilobytes. This is the small-file anti-pattern
promoted to a schema decision, and it is unfixable by compaction because the
partition boundary forbids merging across drivers.

*Rejected **partition Silver by event hour**:* late events land into old hours,
so every province network recovery rewrites dozens of historical partitions and
starves compaction. Partitioning on `trip_end_date` (a business-closed date,
not an arrival time) bounds the blast radius of late data to one directory.

*Rejected **no partitioning, clustering only**:* clustering handles the hot
predicates but cannot express a retention or deletion boundary. Decree 13
erasure and the 3-year retention job both want a physical unit to drop.

### D4 — Late-arriving events: **MERGE with `src.ts > tgt.ts` guard + 7-day watermark + quarantine**

```sql
MERGE INTO silver.trips t USING staged s ON t.trip_id = s.trip_id
WHEN MATCHED AND s.src_ts > t.src_ts THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

*Rejected **last-write-wins**:* a phone that reconnects after 4 hours replays a
`started` event over an already-`paid` trip and reopens a settled booking. The
timestamp guard is one predicate and prevents a class of financial incident.

*Rejected **append-only + dedup at query time**:* pushes an eight-row window
function into every dashboard query. p95 < 1 s dies, and the correctness of the
answer becomes a property of each analyst's SQL rather than of the table.

*Rejected **unbounded watermark**:* state and rewrite cost grow without limit.
Beyond 7 days events go to `quarantine` and are reprocessed by a deliberate
backfill that a human triggers, because at that age the row is an incident, not
a record.

### D5 — PII: **deterministic HMAC-SHA256 tokenization at Bronze landing, key in KMS, key version stamped per row**

*Rejected **encryption at rest only**:* satisfies the storage checkbox and
nothing else — every analyst with table access still reads plaintext phone
numbers. Decree 13 wants purpose limitation and minimisation, which is an access
question, not a disk question.

*Rejected **unsalted SHA-256**:* Vietnamese mobile numbers are a ~10⁹ space.
A rainbow table over that is minutes of laptop time, so an unkeyed hash is
plaintext with extra steps.

*Rejected **format-preserving encryption reversible in the pipeline**:* the
pipeline would hold a key that reverses every token, so compromising a Spark
worker compromises the whole subject population. The token vault keeps
reversal on a separate service with its own ACL and an audit row per read.

Determinism is required, not incidental: joins and driver dedup happen on
`driver_token`. Every row carries `token_key_v` so a key rotation is
detectable and repairable rather than a silent cardinality bug (see F3).

### D6 — Erasure vs time travel: **crypto-shred the vault + `DELETE` + a 30-day retention that is a written decision**

*Rejected **"we support time travel"** with a default retention:* time travel
means version *v−1* still contains the rows we just erased. Unbounded history is
an unbounded erasure violation. NB8 makes this tension explicit and NB6 shows
the mechanism that resolves it — VACUUM is what finally makes a delete real.

*Rejected **rewriting all history on each request**:* at 3.4 TB of Silver, an
erasure SLA measured in minutes cannot involve a full rewrite. Crypto-shredding
the vault entry makes the token unresolvable immediately; the `DELETE` plus the
30-day vacuum removes the row itself on a schedule.

### D7 — Catalog & governance: **REST catalog (Polaris) + append-only audit table**

*Rejected **Hive Metastore**:* no column-level authorisation and no lineage.
"Analyst may read `city_id` but not `phone_token`" is not expressible, so the
policy would live in a wiki page.

*Rejected **object-store IAM alone**:* file-level grants cannot express column
or row policy, and every new derived table silently re-grants whatever the
prefix allows.

*Rejected **a managed vendor catalog**:* a cross-border transfer dossier for the
control plane is real Decree 13 paperwork; a self-hosted catalog in a Vietnamese
region avoids the question entirely.

---

## 4. Failure modes

### F1 — 03:00: the DBA adds a `NOT NULL` column to `TRIPS`
Debezium emits a new field. Bronze accepts it (schema evolution is opt-in and
enabled at Bronze *only*); Silver's `UPDATE SET *` then fails on schema mismatch,
or worse succeeds and writes NULLs into a column the ML team is about to train on.

* **Detect:** schema-drift check comparing the Bronze commit's `metaData` against
  the registered contract, fired on every commit; plus a Silver freshness alarm
  at 5 minutes (the SLA is 60 s, so 5 min is already an incident).
* **Roll back:** Silver is pinned to its last good version; stop the stream,
  `RESTORE` Silver to that version, extend the contract, replay from the Kafka
  offset recorded in the same commit's `userMetadata`. Bronze is untouched
  because Bronze never updates.
* *Day 18 concept:* schema evolution as an opt-in per layer — permissive at
  Bronze, contract-enforced at Silver.

### F2 — 03:00: Debezium restarts and re-snapshots the whole `TRIPS` table
100 M rows replay into Bronze. Row counts explode; every Gold metric that is a
`count` triples.

* **Detect:** Bronze hourly row count against a 7-day baseline (alert at 3σ),
  plus SCN monotonicity — a snapshot restart shows the SCN going backwards, which
  is unambiguous and fires in seconds, not after the dashboards look wrong.
* **Roll back:** `RESTORE` Bronze to the version before the flood — NB3 measured
  this as a metadata-only operation (30 ms; 1 file removed, 0 rewritten), so the
  rollback is not itself an outage. Silver needs no rollback: the MERGE is
  idempotent on `(trip_id, src_ts)`, which is exactly why D4's guard is written
  as a `>` and not a `>=`.
* *Day 18 concept:* time travel as an operational tool, not a demo.

### F3 — 03:00: token key rotation is applied to half the workers
The same phone yields two different tokens. Driver dedup breaks, `driver_dim`
grows phantom subjects, and a rider's erasure request silently misses the rows
written under the other key — a compliance failure disguised as a data-quality bug.

* **Detect:** daily distinct-`driver_token` cardinality against the baseline, and
  a hard invariant that a single `dt` partition contains exactly one
  `token_key_v`. The second check is a cheap `SELECT DISTINCT` on a partitioned
  column and catches it within one micro-batch.
* **Roll back:** the affected window is bounded by `token_key_v`; re-tokenize
  those partitions with the correct key version and re-run the MERGE. The vault
  keeps both key versions until the repair is verified, then shreds the orphan.

### F4 — A province comes back online and dumps 6 hours of buffered GPS
Millions of late pings arrive at once, rewriting historical `gps_tracks`
partitions; compaction backlogs and the liveboard goes stale.

* **Detect:** watermark-lag metric (event time vs processing time p99) and a
  compaction-queue depth alarm.
* **Roll back / mitigate:** the burst is routed to `quarantine` on a rate trigger
  rather than into the hot MERGE path, then drained by a throttled backfill.
  Freshness for *current* trips is preserved, which is the SLA that matters at
  07:00; the backfill lands before the daily close.

### F5 — Compaction quietly stops and nobody notices for three weeks
The job's exit code is green because it silently skipped a locked table.

* **Detect:** alert on **file count and average file size per partition**, not on
  job status. NB6's numbers are the thresholds: average file below ~64 MB, or
  file count per partition above ~50, is a page.
* **Roll back:** nothing to roll back — but note that VACUUM will not save you
  here. NB6 measured that `deltalake`'s vacuum only reclaims **tombstoned**
  files; the files a crashed compaction leaves behind were never committed and
  are invisible at every retention setting. The orphan sweep must be an explicit
  set-difference job (on-disk − in-log, 24-hour age guard), and the Iceberg
  lesson is the same: `expire_snapshots` moved 20 → 3 snapshots and deleted
  **zero** bytes until a sweep was chained behind it.

---

## 5. Cost, back of envelope

Region: Vietnam / `ap-southeast-1`-class. S3-compatible object storage
**$0.025/GB-month**, PUT **$0.005/1,000**, GET **$0.0004/1,000**, compute
**$0.10/vCPU-hour**.

**Data volume per day**

| Stream | Rows/day | Bytes/row (Parquet, sorted + delta-encoded) | GB/day |
|---|---:|---:|---:|
| GPS pings | 82 M | 40 B | 3.28 |
| Trip lifecycle CDC | 2.2 M | 400 B | 0.88 |
| Payments / ratings | 1.0 M | 300 B | 0.30 |
| **Total Bronze** | **85 M** | | **≈ 4.5 GB/day** |

**Storage**

| Layer | Retention | Size | $/month |
|---|---|---:|---:|
| Bronze | 30 days | 4.5 × 30 = 135 GB | $3.38 |
| Silver | 3 years, ×0.75 after dedup + compaction | 4.5 × 365 × 3 × 0.75 = **3.70 TB** | $92.40 |
| Gold | 3 years, aggregates | ~8 GB | $0.20 |
| Vault + audit | 2 years | ~15 GB | $0.38 |
| | | | **≈ $96/mo** |

**Requests.** 30 s micro-batch = 2,880 commits/day × ~6 files = 17,280 PUT/day
→ 0.52 M PUT/month → **$2.60/mo**. Hourly compaction rewrites ~1,200 files/day
→ negligible. Dashboard reads hit Gold (small, clustered): 60 s refresh × 12
panels × 730 h = 0.5 M GET/month → **$0.20/mo**.

> Contrast, and this is the whole FinOps point: at the *uncompacted* file size
> NB6 measured (51.5 KB average), the same Silver would be ~72 M files. A single
> full-scan pass costs 72 M GET = **$28.80**, and the daily dashboard fan-out
> would be four figures a month. The request bill is driven by **file count**,
> not by data volume.

**Compute**

| Component | Sizing | $/month |
|---|---|---:|
| Kafka | 3 brokers × 4 vCPU, 24/7 | $876 |
| Spark Structured Streaming | 3 workers × 4 vCPU, 24/7 (sized for the 30 K/s peak, not the 1 K/s mean) | $876 |
| Maintenance (compaction, cluster, vacuum, sweep) | 8 vCPU × 1.5 h/day | $36 |
| Trino ad-hoc | 2 × 8 vCPU × 12 h/day | $584 |
| Catalog + vault services | 2 × 2 vCPU, 24/7 | $292 |
| | | **≈ $2,664/mo** |

**Total ≈ $2,760/month.** Storage is **3.5%** of it. The bill is *always-on
compute sized for a 30× peak* — so the first optimisation is not a cheaper
storage class, it is autoscaling the streaming tier off the daily curve
(≈ −$400/mo) and putting Trino on spot with a warm pool (≈ −$300/mo).

At 10× growth (1 B trips/year), storage rises linearly to ~$960/mo and compute
sublinearly to roughly $8–10 K/mo; the ratio gets *worse* for compute, which is
the argument for pushing more of the dashboard load onto pre-aggregated Gold
rather than onto a bigger Trino.

---

## 6. What I would build first — a one-week MVP

**Scope: one city (Đà Nẵng), one table (`trips`), one Gold metric.** Everything
else is deliberately excluded.

| Day | Deliverable |
|---|---|
| 1 | Debezium on a *staging* Oracle → one Kafka topic. Prove SCN ordering and 72 h retention. |
| 2 | Bronze writer with **tokenization on the landing path** and `token_key_v` stamped per row. Assert: `grep -r` over the Bronze parquet finds **zero** raw phone patterns. |
| 3 | Silver MERGE with the `src.ts > tgt.ts` guard, 30 s trigger. |
| 4 | **Late-data test — the hard part.** Replay a 6-hour-old `started` event against an already-`paid` trip and assert the trip is unchanged. Then replay a genuinely newer correction and assert it applies. |
| 5 | Gold `trip_metrics_5m` + one Superset panel. Measure actual commit-to-panel latency against the 60 s budget. |
| 6 | **Rollback drill.** Deliberately double-ingest, detect it with the row-count monitor, `RESTORE` Bronze, re-run Silver, confirm Gold returns to the correct numbers. Time it. |
| 7 | **Erasure drill.** Pick a subject, crypto-shred the vault entry, `DELETE`, vacuum a shortened retention, prove the rows are unrecoverable at *every* accessible version. Write down the retention decision as a decision, not a default. |

**Why this slice.** It proves the three things that would kill the design if they
were wrong: (a) PII never touches disk in the clear, (b) out-of-order events
cannot corrupt settled state, and (c) a bad ingest is recoverable in minutes by
one on-call engineer. Throughput is *not* on this list — Spark at 30 K events/s
is a known quantity, and scale problems are the ones you can buy your way out of
later. Correctness and compliance problems are not.

**Explicitly out of MVP scope:** GPS telemetry (10× the volume, none of the
risk), SCD-2 `driver_dim`, multi-city clustering, the Polaris catalog (start on
the metastore we have), autoscaling.

---

## Appendix — where the numbers came from

Every rule of thumb above is a measurement from this lab, not a vendor claim:

| Claim | Source |
|---|---|
| 51.5 KB average files from a naive streaming writer; 200 → 11 after compaction | NB6 Job 1 |
| Request cost tracks file count, not bytes ($4.00/day → $0.08/day) | NB6 baseline |
| VACUUM does not reclaim never-committed orphans | NB6 Job 4 |
| `expire_snapshots` reclaimed 0 bytes until an orphan sweep was chained | NB6 Job 3+4 |
| RESTORE is metadata-only (0.03 s, 0 files rewritten) | NB3 |
| A forgotten partition predicate costs $220/day at 10 K queries | NB5 |
| Time travel and erasure are in direct conflict without a written retention | NB8 |
