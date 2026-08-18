# Ride-hailing CDC → Lakehouse under Nghị định 13

**Bonus challenge — Topic C.** Architecture brief, written to be defended in a
design review.

Numbers below are derived, not asserted; where I assume something the assumption
is named. Several trade-offs are settled with measurements from this lab's own
notebooks — cited as `NB<n>` and reproducible via `make run-all`.

---

## 1. Problem statement

Production Oracle → Debezium CDC → lakehouse for analytics, at a Vietnamese
ride-hailing operator. **100M trips/year** (274K/day), **30K writes/second at
peak**. Driver and passenger PII — phone, CCCD/CMND, GPS traces — is in scope for
**Nghị định 13/2023/NĐ-CP**, which requires consent records, a purpose-bound
processing basis, disclosure logs, and erasure on request. Analysts need
dashboards **≤ 60 s behind the source commit** and ad-hoc **p95 < 1 s**.
Late-arriving events are routine: drivers lose connectivity in rural provinces
and their handsets replay hours of buffered state on reconnect.

What makes it hard is not the volume. It is that three requirements pull against
each other: **freshness** wants tiny frequent writes, **p95 < 1 s** wants large
clustered files, and **erasure** wants the ability to delete a row that
**time travel** is contractually obliged to still remember. Any design that
satisfies two of these and hand-waves the third fails in month three.

*(198 words)*

### 1.1 The first number worth checking

"30K writes/s" and "100M trips/year" cannot both describe the same stream.
100M trips/year at ~25 state changes per trip (requested → matched → arrived →
started → completed, plus fare and payment updates) is **6.8M business events/day
≈ 79/s average, ~1K/s at peak**. The missing 29K/s is one table: a
`driver_current_location` row hammered with `UPDATE`s — ~200K online drivers
pinging every 4 s.

That decomposition is the single highest-leverage finding in this design, and it
is arithmetic, not architecture. It drives Decision 1.

---

## 2. Architecture

```
 ORACLE (OLTP)                    INGEST                         LAKEHOUSE (S3 + Delta)                    CONSUMERS
 ─────────────                    ──────                         ──────────────────────                    ─────────
                                                    ┌──────────────────────────────────────────┐
 ┌───────────────┐  redo   ┌────────────┐  ~1K/s    │ BRONZE  (append-only, 7-day retention)   │
 │ TRIPS         │────────▶│ Debezium   │──────────▶│  cdc_raw           partition: ingest_date│
 │ PAYMENTS      │  logs   │ (Oracle    │  Kafka    │  ├─ tokenize @ landing ── HMAC-SHA256 ───┼──┐
 │ DRIVERS       │         │  connector)│  20 topics│  │   phone, cccd  → 128-bit token        │  │
 │ RATINGS       │         └────────────┘           │  ├─ raw payload NEVER lands in plaintext │  │
 └───────────────┘              │                   │  └─ key_version stamped per row ─────────┼┐ │
                                │ signal table      └──────────────────┬───────────────────────┘│ │
 ┌───────────────┐              │ (incremental      micro-batch 30 s   │ MERGE                   │ │
 │ driver_       │  ✗ EXCLUDED  │  snapshot)        guarded by         ▼                         │ │
 │ current_      │─ ─ ─ ─ ─ ─ ─ ┘                   src.ts > tgt.ts ┌──────────────────────────┐ │ │
 │ location      │                                                  │ SILVER (90-day hot)      │ │ │
 └───────┬───────┘                                                  │  trips_current  (SCD1)   │ │ │
         │ 29K/s of "latest value only"                             │  trips_history  (SCD2)   │ │ │
         │                                                          │  payments, drivers       │ │ │
         ▼                                                          │  part: trip_date         │ │ │
 ┌───────────────┐   Kafka    ┌──────────────────┐                  │  cluster: city_id,       │ │ │
 │ Mobile SDK    │───────────▶│ GPS trajectory   │─────────────────▶│           driver_token   │ │ │
 │ (direct)      │  1.44B/day │ compaction job   │  hourly          │  late > 7d → quarantine ─┼─┼─┼──▶ daily
 └───────────────┘            └──────────────────┘                  └────────────┬─────────────┘ │ │    backfill
                                                                                 │ aggregate      │ │
                              ┌──────────────────┐                  ┌────────────▼─────────────┐  │ │
                              │ CONTROL PLANE    │                  │ GOLD (365-day, tiered)   │  │ │   ┌──────────┐
                              │ Unity Catalog OSS│◀── column tags ──│  trip_metrics_hourly     │──┼─┼──▶│ Dashboard│
                              │  · pii tags      │    lineage       │  driver_earnings_daily   │  │ │   │  ≤ 60 s  │
                              │  · lineage       │                  │  city_supply_demand      │  │ │   └──────────┘
                              │  · grants        │                  └──────────────────────────┘  │ │   ┌──────────┐
                              └────────┬─────────┘                                                │ │   │ Ad-hoc   │
                                       │ query history export                                     │ │   │ p95 < 1s │
                              ┌────────▼─────────┐   ┌─────────────────────┐                      │ │   └──────────┘
                              │ access_audit     │   │ KMS / HSM           │◀── key_version ──────┘ │
                              │ (who read whose  │   │  per-column keys    │    crypto-shred ───────┘
                              │  PII, when, why) │   │  rotation → v2, v3  │
                              └──────────────────┘   └─────────────────────┘

 MAINTENANCE (cron, all tables):  compact 128–512 MB ·  Z-ORDER hot partitions ·  vacuum 35d
                                  ·  RECONCILE files-on-disk vs files-in-log  ← NB6's lesson
```

---

## 3. Decisions, and what I rejected

### D1 — `driver_current_location` does not go through CDC

**Chosen:** exclude it from the Debezium whitelist; GPS goes app → Kafka →
hourly-compacted trajectory table.

* **Rejected — CDC every table uniformly.** Operationally simplest, and wrong.
  It puts 29K/s (97% of the write volume) of pure last-value-wins churn into the
  connector, where it competes for redo-log read throughput with the trips and
  payments streams that actually carry the SLA. The business value of 900 stale
  location rows per driver per hour is zero; only the latest matters, and the
  latest is already in Oracle.
* **Rejected — Oracle GoldenGate for the whole estate.** Solves connector
  throughput with money, not with thinking: the same 29K/s still lands in Bronze
  and still has to be compacted and paid for.

**Consequence:** the CDC path is a ~1K/s peak problem, which is what makes
copy-on-write Delta MERGE viable (see D2). Take this decision away and D2
changes.

### D2 — Delta Lake, not Iceberg, not Hudi

**Chosen:** Delta, with Change Data Feed enabled on Silver.

* **Rejected — Iceberg.** Hidden partitioning is genuinely the better design:
  NB5 measured **10× pruning** filtering on `ts` rather than a derived `ts_day`,
  and it structurally removes the forgotten-predicate bug class (the same
  notebook prices that mistake at **$220/day** for 10K queries at 512 MB/file).
  I still rejected it, for a reason specific to *this* problem: erasure has to
  fan out to every derived copy, which needs a reliable row-level change feed,
  and NB6 measured that Iceberg's Python maintenance path leaves the storage
  cleanup to me — `expire_snapshots` dropped 20 snapshots to 3 while deleting
  **zero** files — manifest avro count held at 40 — and *grew* metadata
  343 → 351 KB. On a table with a
  legal retention clock, "expiry is metadata-only" is a compliance trap, not a
  performance footnote.
* **Rejected — Hudi MOR.** The record-level index is the textbook fit for
  high-rate CDC upserts, and had D1 gone the other way I would have chosen it.
  At ~1K/s peak the merge-on-read complexity (compaction scheduling, timeline
  service, a second operational surface) buys nothing, and our query engines
  (Trino, DuckDB) support it least well.

**Accepted cost:** we give up hidden partitioning, so every Silver query must
carry a `trip_date` predicate. That is enforced in the semantic layer, not by
hope — see F3.

### D3 — Deterministic HMAC tokenization at Bronze landing

**Chosen:** `token = HMAC-SHA256(key[col][version], normalize(value))[:16]`,
key in KMS, `pii_key_version` stamped on every row. Plaintext PII never lands.

* **Rejected — AES-GCM encryption in-column.** Reversible, therefore still
  personal data under Decree 13 (pseudonymized, not anonymized). Analysts who
  need to join on phone number would need decrypt rights, so the population
  holding effective PII access *grows* instead of going to zero.
* **Rejected — plaintext + column-level ACLs in the catalog.** One
  misconfigured `GRANT` is a reportable incident, and every downstream copy
  inherits the full obligation. Governance you can misconfigure is not a control.
* **Rejected — random surrogate + lookup table.** Makes the lookup table the
  most sensitive asset in the company and forces a join at query time — directly
  against the p95 < 1 s budget.

**Why deterministic:** joins, dedup and per-subject aggregation keep working on
tokens. **Bonus property:** destroying a key version crypto-shreds every row
written under it — which is how D6 resolves the erasure/time-travel conflict.

### D4 — Late data: guarded MERGE, bounded watermark

**Chosen:** `WHEN MATCHED AND src.event_ts > tgt.event_ts THEN UPDATE`, with a
**7-day** acceptance window into live partitions; older events route to
`late_arrivals` and are folded in by a daily backfill.

* **Rejected — blind `when_matched_update_all()`.** A handset that buffered six
  hours offline replays `in_progress` *after* the trip completed; a blind upsert
  reverts the row and the fare job then bills wrong. This corrupts revenue
  silently, which is the worst failure class there is.
* **Rejected — append-only + `qualify row_number()` view.** Writes get trivially
  cheap and every read pays forever; the dedup window grows with history and
  p95 < 1 s dies quietly over a quarter.
* **Rejected — unbounded lookback.** An unbounded MERGE target means rewriting
  arbitrarily old partitions at 03:00, with copy-on-write amplification, on a
  cluster sized for one day.

### D5 — Partition by day, cluster inside it

**Chosen:** `partition_by=trip_date`; `ZORDER BY (city_id, driver_token)`;
compaction to the 128–512 MB band.

* **Rejected — partition by (city, hour).** 63 provinces × 24 h = 1,512
  partitions/day, 552K/year. NB6 measured what that does: 200 micro-batch files
  averaging **51.5 KB**, **$4.00/day in GET requests alone** for one table
  versus $0.08 compacted, and in the managed-compaction model **24% of the bill
  is per-object, not per-GB** — file count, not data volume.
* **Rejected — no partitioning, clustering only.** A "yesterday" dashboard would
  scan a 365-day table. Partition pruning is the cheap 365× win; clustering is
  the fine-grained one on top.

Sizing check: ~1 GB/day/table lands one day-partition squarely in the target
file band after compaction — no sub-partitioning needed. NB6 measured
compaction at **200 → 11 files (18×)** and clustering at a **90% file-skip
rate** for a point query; those are the two numbers this decision is buying.

### D6 — Retention is a written decision, not a default

**Chosen:** Bronze 7 days · Silver time travel 35 days · Gold 365 days (legal
minimum) with tiering. Erasure = delete + crypto-shred the subject's key
material; the 35-day window is documented in the DPIA as the interval within
which an erasure is *completed*, not *begun*.

* **Rejected — unlimited time travel** ("storage is cheap"). NB8 makes the
  conflict concrete: after an erasure the table moved v0 → v1, and **v0 still
  contains the erased rows**. Unlimited history means unlimited retention of
  data a data subject has asked you to destroy.
* **Rejected — vacuum aggressively to 0 days.** Then F2's rollback path does not
  exist, and RESTORE is the only tool that reverses a bad batch quickly.

35 days is where those two curves cross for us: long enough for the rollback
drills we actually run, short enough to state a completion SLA to a regulator.

---

## 4. Failure modes

### F1 — Debezium lag from a bulk Oracle job (03:00, freshness SLA)

A fare-recalculation batch `UPDATE`s 2M rows. The connector emits 2M events for
data no analyst asked for; consumer lag climbs, the 60 s freshness SLA breaks,
and if lag outlives redo-log retention the connector needs a re-snapshot —
hours, not minutes.

* **Detect:** alarm on `now - source.ts_ms` p99 > 45 s (15 s of headroom), plus
  a separate alarm on redo-retention margin. Lag on the *source timestamp*, not
  on consumer offsets — offsets look healthy while the data ages.
* **Roll back:** topic prioritization — pause ratings/driver-profile consumers so
  trips and payments drain first (the SLA is per-table, and only two tables carry
  it). If the redo gap is already lost, use Debezium's **signal table** for an
  incremental snapshot scoped to the affected table rather than a full
  re-snapshot of the estate.
* **Prevent:** bulk maintenance jobs declare themselves via the signal table and
  are filtered at the connector.

### F2 — Late replay corrupts completed trips *(Day 18 tie: MERGE + time travel)*

The guard in D4 is one predicate. Someone "simplifies" the merge, or a new table
is onboarded from a copy-paste template without it, and a night of buffered rural
traffic reverts completed trips to `in_progress`.

* **Detect:** a per-micro-batch assertion on illegal state transitions —
  `count(completed → in_progress) > 0` must be zero — plus a revenue-delta alarm
  against the prior batch. Assertion runs *before* Gold publishes.
* **Roll back:** `RESTORE VERSION AS OF <pre-batch>` on Silver, then replay from
  Bronze with the guard restored. NB3 measured RESTORE completing in under a
  second on 100K rows and, importantly, **RESTORE is itself commit v4** — the
  corrupt state stays in the log and is auditable afterwards. Bronze's 7-day
  append-only retention is what makes replay possible at all; this is why Bronze
  is not "the layer we can skip."
* **Cost of the drill:** rollback + replay of one day is ~1 GB rewritten.

### F3 — A query without a partition predicate

Consequence of D2's accepted trade-off. An analyst writes
`WHERE city_id = 79` with no `trip_date`, and scans 365 days.

* **Detect:** query-history rule flagging Silver scans without a `trip_date`
  predicate; alarm on bytes-scanned per query above a threshold.
* **Roll back:** the semantic layer injects a default 7-day window; raw table
  access is granted per-team, not by default. NB5 priced the forgotten-predicate
  mistake at **$220/day** at 10K queries/day — this control has a number attached.

### F4 — Maintenance that reports success and reclaims nothing

The storage bill rises while every maintenance job is green.

* **Detect:** monthly reconcile of parquet files on disk against `file_uris()`;
  alert if the gap exceeds 1%. This is not paranoia — NB6 measured **5 files on
  disk that the log never tombstoned**, invisible to `VACUUM` at *any* retention,
  because a crashed writer's output was never committed and therefore never
  tombstoned. A dry-run reported 211 files and found none of the 5.
* **Roll back:** age-guarded orphan sweep (≥ 24 h old only — without the guard
  you delete a concurrent writer's uncommitted files and corrupt the table).

### F5 — Erasure and rollback in direct conflict

F2's rollback resurrects rows erased under Decree 13 between the restore point
and now.

* **Detect:** the erasure ledger re-verifies subject absence after *any* RESTORE;
  this check is part of the rollback runbook, not a separate job.
* **Mitigate:** crypto-shredding (D3) means restored rows carry tokens whose key
  version no longer exists — unresolvable, therefore not re-identifiable. Any
  erasure requests landing between the restore point and now are re-applied from
  the ledger before the table is unfrozen.

---

## 5. Cost, back of the envelope

Assumptions stated so they can be attacked: 6.8M business events/day at ~0.30 KB
compressed (≈ 2 GB/day); GPS 1.44B pings/day at ~12 B packed with delta+RLE
encoding (≈ 17 GB/day); Silver SCD2 ≈ 3 versions/trip; AWS ap-southeast-1
list prices.

**Storage** (steady state, year 1 — ~20 GB/day total):

| Tier | Window | Volume | $/GB-mo | $/mo |
|---|---|---|---|---:|
| S3 Standard | 0–30 d | 600 GB | 0.023 | 13.80 |
| S3 IA | 31–90 d | 1.2 TB | 0.0125 | 15.36 |
| Glacier IR | 91–365 d | 5.5 TB | 0.004 | 22.53 |
| | | | | **≈ $52** |

Storage is **not** the problem — a useful result, because it kills the instinct
to spend the design budget on compression.

**Requests:** 2,880 micro-batches/day × ~20 tables ≈ 58K PUTs/day ≈ **$9/mo**.
GETs are where file count bites: at 30 s batches with no compaction, one table
accumulates ~1M files/year, and a full-year scan costs
`1M / 1000 × $0.0004 = $0.40` **per query** — $4,000/day at 10K queries.
Compacted to the 128–512 MB band it is cents. This is D5 restated in money.

**Compute:**

| Item | Math | $/mo |
|---|---|---:|
| Streaming ingest + MERGE | 4 nodes × $0.20/h × 730 h | 584 |
| Maintenance (compact/Z-order) | 600 GB/mo × $0.05 + 1.7M obj × $0.004/1K | 37 |
| Query (Trino/Athena) | 10K queries/day × 2 GB pruned × $5/TB × 30 | 3,000 |
| | | **≈ $3,620** |

**Total ≈ $3,700/month**, of which **81% is query compute**. The lever is
pruning and clustering, not storage class — and the failure mode that blows the
budget is F3, not data growth. Without pruning the same 10K queries scan the
full 6 TB and the line item is ~$900K/month, which is the real argument for D5.

---

## 6. What I build in week one

**One table, end to end: `TRIPS`.** Not the estate, not GPS, not governance
tooling.

1. Debezium on `TRIPS` only → one Kafka topic.
2. Bronze append with HMAC tokenization of `phone` + `cccd` at landing,
   `pii_key_version` stamped.
3. Silver `trips_current` (SCD1) + `trips_history` (SCD2) via the **guarded**
   MERGE, 30 s micro-batches.
4. One Gold table: completed trips + revenue by city by hour.
5. Freshness instrumentation: `source.ts_ms` → Gold commit timestamp, p95 charted.

**Done means four measurements, not four green checkmarks:**

* freshness p95 **< 60 s** under a replayed peak-hour load;
* a synthetic 6-hour-late batch **does not** revert a completed trip (and the
  same batch *does* revert it with the guard removed — prove both directions);
* an automated scan finds **zero** plaintext phone/CCCD anywhere downstream of
  Bronze landing;
* a RESTORE rollback drill completes in **< 5 minutes**, wall-clock, run by
  someone who did not build the pipeline.

Explicitly **not** in week one: the other ~19 tables, the GPS path, catalog
governance, tiering, the backfill job for the >7-day quarantine. Each is
scheduled only after the four measurements above hold, because every one of them
is cheaper to add to a pipeline whose semantics are already proven than to debug
inside a pipeline that was built wide before it was built correct.

---

## 7. Proof of concept

[`poc/late_arrival_merge.py`](poc/late_arrival_merge.py) — 146 lines, runs
offline from a clean checkout with the lab's own venv:

```bash
.venv/bin/python submission/bonus/poc/late_arrival_merge.py
```

It demonstrates the part of this design I consider hardest to get right, and the
part most likely to be silently broken in production: **D4's late-arrival guard**
(blind MERGE vs guarded MERGE, run side by side on the same input so the
divergence is measured, not argued), plus **D3's deterministic tokenization**,
**SCD2 history**, and **D6's crypto-shred** erasure path. Measured output is in
[`poc/OUTPUT.txt`](poc/OUTPUT.txt).
