<!-- ═══════════════════════════════════════════════════════════════════
     ⚠️  BẢN NHÁP — đọc kỹ và sửa thành giọng của bạn trước khi nộp.
     Bonus được nhận xét viết tay về *phán đoán*; bạn sẽ bị hỏi lại về
     các con số và các phương án đã loại. Chỗ nào bạn không bảo vệ được
     thì đổi, đừng giữ.
     Giả định giá cloud ở §5 là giá niêm yết công khai — kiểm lại trước khi nộp.
     ═══════════════════════════════════════════════════════════════════ -->

# Ride-hailing CDC → Lakehouse under Decree 13

**Topic C** · Day 18 Bonus Challenge · Trần Tiến Dũng

---

## 1. Problem statement

A Vietnamese ride-hailing operator runs its transactional core on Oracle.
Analytics today is a nightly `expdp` dump into a warehouse — 14 hours stale,
and it breaks whenever a DBA adds a column. We must replace it with a CDC
pipeline into a lakehouse.

Scale: **100 M trips/year** (~274 K/day), and GPS telemetry at one ping per
5 s per active trip — **~66 M pings/day**, ~24 B rows/year. Peak write rate
**30 K/s** at Friday-evening surge, roughly 40× the daily average.

Constraints that make it hard:

* **Freshness.** Dashboards must reflect a source commit within **60 s**;
  ad-hoc queries **p95 < 1 s**.
* **Late data is normal, not exceptional.** Drivers in mountainous provinces
  lose signal for minutes; trip-completion events arrive out of order and
  occasionally hours late.
* **Decree 13/2023/NĐ-CP** covers driver and rider phone numbers, national ID,
  and GPS traces. Data subjects can demand erasure. Cross-border transfer
  requires an impact-assessment dossier, so the practical answer is to keep
  everything in-country.
* The analytics team is Python-first and has **no JVM operators on call**.

*(172 words)*

---

## 2. Architecture

```
        ┌──────────────────── IN-COUNTRY BOUNDARY (Decree 13) ────────────────────┐
        │                                                                          │
 Oracle │  ┌──────────┐   ┌───────────────────────────────┐                        │
 OLTP ─────▶│ Debezium │──▶│ Kafka · 3-day retention       │  ← replay boundary     │
 30K/s  │  └──────────┘   │ trip.* · driver.* · gps.*     │                        │
        │                 └───────────────┬───────────────┘                        │
        │                                 │ Flink, exactly-once                     │
        │                 ┌───────────────▼───────────────┐                         │
        │                 │ TOKENISE AT LANDING            │                        │
        │                 │ phone, nat_id → HMAC(k@KMS) ───┼──┐                     │
        │                 │ raw GPS      → geohash-7 (~150m)│  │                    │
        │                 └───────────────┬───────────────┘  │                     │
        │                                 │                   ▼                     │
        │  ┌──────────────────────────────▼────┐   ┌────────────────────────────┐   │
        │  │ BRONZE  delta · append-only · 30 d │   │ PII VAULT  delta · TTL 30 d│   │
        │  │ raw CDC envelope (op/before/after) │   │ token → raw · 4 people ACL │   │
        │  │ partition: date                    │   │ never joined in Silver/Gold│   │
        │  └──────────────────────────────┬────┘   └────────────────────────────┘   │
        │                                 │                                          │
        │        MERGE … WHEN MATCHED AND src.event_ts > tgt.event_ts                │
        │        (7-day late window, watermark-driven)                               │
        │  ┌──────────────────────────────▼──────────────────────────────────────┐   │
        │  │ SILVER  delta · CDF enabled · 3 y                                    │   │
        │  │ trips_scd2 · driver_dim (SCD-2) · gps_geohash                        │   │
        │  │ partition: date   ·   Z-ORDER: (city_tier, driver_token)             │   │
        │  └──────────────────────────────┬──────────────────────────────────────┘   │
        │                                 │ incremental via CDF, never a full rescan │
        │  ┌──────────────────────────────▼─────┐  ┌─────────────────────────────┐   │
        │  │ GOLD  delta · 3 y                   │  │ MAINTENANCE (cron, not      │   │
        │  │ trip_daily · driver_earnings        │  │ optional — see §4.3)        │   │
        │  │ surge_5min                          │  │ 1 compact  2 cluster        │   │
        │  └───────────┬─────────────────┬──────┘  │ 3 expire   4 ORPHAN SWEEP   │   │
        │              │                 │          │ 5 checkpoint                │   │
        └──────────────┼─────────────────┼──────────┴─────────────────────────────┘   │
                       ▼                 ▼
             Trino → dashboards    DuckDB → ad-hoc
             ≤ 60 s freshness      p95 < 1 s
```

---

## 3. Key decisions, with rejected alternatives

**3.1 Table format → Delta Lake.**
I rejected **Hudi**, and this is the choice I am least comfortable with: Hudi's
Merge-on-Read is genuinely the better shape for 30 K upserts/s, because it defers
the rewrite cost to compaction instead of paying it on the write path. I rejected
it on people, not technology — the local hiring pool for Hudi operators is thin,
and a format nobody on call can debug at 3 AM is a liability that outweighs its
write-path advantage. I rejected **Iceberg** despite measuring 5× partition
pruning from hidden partitioning in NB5, because Delta's Change Data Feed is what
makes Silver→Gold incremental (§3.5), and `delta-rs` gives the Python team a
reader with **no JVM at all** — which matters when there are no JVM operators
on call.

**3.2 CDC transport → Debezium into Kafka, not Debezium Server straight to object storage.**
Kafka costs an extra system to run. It buys the **replay boundary**: when a Silver
transform ships a bug, I re-consume from a stored offset rather than re-extracting
from the production Oracle. Writing straight to storage saves that system and
removes replay entirely. I also rejected **Oracle GoldenGate** — it works, but its
licence cost is real money spent to avoid an open-source connector we can staff.

**3.3 Partitioning → `date`, with Z-ORDER on `(city_tier, driver_token)`.**
I rejected **partitioning by `driver_id`**: ~200 K active drivers × 365 days is
millions of directories holding a few KB each — the exact pathology NB6 reproduced,
where 200 files holding 10 MB cost \$4.00/day in GET requests against \$0.08 for
the same data compacted. I rejected **`date/hour/city`**: hour-granularity gives
8,760 partitions/year per city and pushes us back toward the same small-file
regime. Date partitions plus clustering gave a **90 % file-skip rate** for a
point query in NB6; that is the right lever.

**3.4 PII → deterministic tokenisation at Bronze landing.**
Phone and national ID become `HMAC-SHA256(value, key)` with the key in a KMS the
analytics account cannot read; raw values live only in a separate PII vault table
with a 30-day TTL and a four-person ACL. Deterministic (not random) tokens matter
because fraud detection must still join on "same phone, different account".
I rejected **column masking at query time**: raw PII would still sit on disk, still
in scope, and — the part people miss — **still readable through time travel**.
I rejected **envelope-encrypting the columns**: it breaks those joins outright.

**3.5 Silver→Gold → incremental via Change Data Feed.**
Gold reads only changed rows since the last watermark. I rejected **full daily
recompute**: at 24 B GPS rows/year the rescan cost grows without bound and cannot
hold a 60 s freshness SLA. I rejected **Kafka→Gold directly**, bypassing Silver:
it is faster to build and loses the auditable typed layer that every Decree 13
question gets answered from.

**3.6 Driver dimension → SCD Type 2.**
Commission tier changes over a driver's lifetime, and every earnings dispute is a
question about *what the tier was at trip time*. I rejected **overwrite-in-place**:
it makes those disputes unanswerable. I rejected **full event-sourcing of the
dimension**: correct, but it pushes reconstruction logic into every consumer.

**3.7 Catalog → self-hosted REST catalog, in-country.**
I rejected **AWS Glue**: it anchors metadata to an AWS region and drags a
cross-border transfer dossier into scope for what is only metadata. I rejected
**Hive Metastore**: no atomic multi-table commit, which is precisely the control-plane
weakness NB5 was built to demonstrate.

---

## 4. Failure modes

**4.1 — 03:12. A DBA adds a column to `trips` in Oracle.**
*Detect:* schema-registry incompatibility alert; Bronze row-count for the topic
drops to zero within one batch.
*Response:* nothing to roll back — **Delta schema enforcement rejects the write**,
exactly as NB1 measured when the `age="thirty"` row was blocked. The pipeline
halts instead of silently corrupting Silver. We review the column, append it with
`schema_mode="merge"` (opt-in, never automatic), replay from the Kafka offset.
*Why halting is the correct default:* a pipeline that auto-accepts upstream schema
drift will eventually accept a type change that silently truncates fares.

**4.2 — 02:40. A bad Silver transform writes negative fares for three hours.**
*Detect:* Gold `error_rate` and a revenue anomaly check against the same
weekday-hour last week.
*Response:* `RESTORE VERSION AS OF <last good>`. NB3 measured this end to end —
`history()` shows ≥ 5 versions *including the RESTORE row*, and the `score < 0`
count went to 0 afterwards. RTO is minutes, and the restore is itself a commit,
so the incident stays in the audit trail rather than being erased by the fix.

**4.3 — 04:00. Nobody scheduled compaction.**
The streaming job commits every 30 s. After a fortnight each date partition holds
tens of thousands of files; dashboard p95 slides from 0.8 s to 12 s and the object
-storage bill rises on **requests**, not bytes.
*Detect:* an SLO alarm on files-per-partition (> 200), not on latency — latency is
the lagging indicator.
*Response:* the compaction job. NB6 measured 200 → 11 files (18×) and a 90 % skip
rate after clustering. This is the anti-pattern I named in my `REFLECTION.md`, and
it is the one I expect us to hit first.

**4.4 — A Decree 13 erasure request arrives, and the deletion does not delete.**
This is the failure mode I would not have believed before running NB6.
*Detect:* a quarterly erasure audit that re-queries the subject's token at older
`versionAsOf` snapshots **and** diffs the file listing against the transaction log.
*Response:* erasure is a three-step chain, not one `DELETE`:
delete → expire snapshots/vacuum past retention → **orphan sweep**.
NB6 measured why each step alone is insufficient: `VACUUM` reclaimed 16.1 MB but
left 5 parquet files on disk that the log had never recorded (15 on disk, 10 in
the log), and Iceberg's `expire_snapshots` went 20 → 3 snapshots while deleting
**zero** avro files and *growing* metadata from 328.4 KB to 335.7 KB. A team that
runs only expiry will report erasure as complete while the bytes are still there.

---

## 5. Cost back-of-envelope

*Assumption: in-country object storage at \$0.022/GB-month; compute at
\$0.35/hr for an 8-vCPU/32 GB instance. Verify against a current price sheet.*

**Storage** (steady state, year 3):

| Layer | Retention | Daily (Parquet+zstd) | Steady size | \$/mo |
|---|---|---|---|---|
| Bronze (raw CDC envelope) | 30 d | 3.0 GB | 90 GB | 1.98 |
| Silver (typed, SCD-2, CDF) | 3 y | 1.0 GB | 1,095 GB | 24.09 |
| Gold (aggregates) | 3 y | 0.05 GB | 55 GB | 1.21 |
| **Total** | | | **1,240 GB** | **\$27.28** |

**Compute:**

| Job | Shape | \$/mo |
|---|---|---|
| Flink streaming ingest (24/7, sized for 30 K/s peak) | 3 × 8-vCPU | 766 |
| Maintenance (compact/cluster/expire/sweep, 2 h/day) | 1 × 16-vCPU | 42 |
| Trino serving layer (dashboards + ad-hoc) | 2 × 16-vCPU | 1,022 |
| **Total** | | **\$1,830** |

**The finding worth stating out loud: storage is 1.5 % of this bill.**
Compute is 64× storage, so any FinOps effort spent on tiering Silver to cold
storage is optimising the wrong line. The real variable cost is **requests**, and
it is governed entirely by whether maintenance runs. Extrapolating NB6's measured
figure — 200 files serving 10 MB cost \$4.00/day in GETs versus \$0.08 compacted —
an uncompacted month across our three Silver tables reaches roughly **\$3,000/mo
in GET requests alone**, larger than the entire compute bill. The compaction job
that costs \$42/mo is the highest-ROI line item in this table.

---

## 6. What I would build first (one week)

Not the medallion — that part is well understood and NB4 already proves the shape.
The week-one slice targets the two things that would kill this design if they turn
out to be false:

1. **One table, end to end:** `trips` only. Debezium → Kafka → Bronze append →
   Silver `MERGE` with the 7-day late window → Gold daily aggregate. GPS and the
   driver dimension are deliberately out of scope.
2. **A late-data correctness test, in CI.** Replay a fixed tape of events with
   three deliberately reordered completions and one arriving 6 days late; assert
   the Silver row matches the source-of-truth fare. If the `MERGE` predicate is
   wrong, I want to find out in week one, not in month three from a driver's
   earnings complaint.
3. **An erasure drill, in CI.** Insert a synthetic subject, run the erasure chain,
   then assert **zero** hits at every historical version *and* zero orphan files
   left behind. NB8 measured this pattern working; I want it green before real
   PII lands, not after.
4. **The files-per-partition alarm, from day one.** It is three lines of code and
   it is the detector for §4.3.

If those four are green, the rest is scale-out. If the erasure drill is red, the
architecture is not legal to operate and nothing else matters.
