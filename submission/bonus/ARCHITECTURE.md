# Bonus Challenge: CDC Ride-Hailing Lakehouse (Decree 13 Compliant)

## 1. Problem Statement (<=200 words)

We need a production lakehouse for a Vietnamese ride-hailing platform that ingests Oracle CDC into analytics with strict privacy and low latency constraints.

Constraints:
- Source: Oracle OLTP + Debezium CDC.
- Scale: ~100M trips/year, peak 30K writes/sec, sustained ~2.5K writes/sec across trip, driver, payment, and support tables.
- SLA:
  - Dashboard freshness: <=60 seconds from source commit.
  - Analyst ad-hoc query: p95 <1 second for curated metrics.
- Privacy and compliance: driver and rider PII (phone, ID, GPS) are covered by Decree 13/2023/ND-CP.
- Data quality reality: late events and out-of-order updates are common due mobile network instability.

Why this is hard:
- We must satisfy near-real-time analytics while enforcing PII controls before analyst access.
- We need deterministic rollback when CDC pipelines introduce bad data.
- We need cross-engine consumption (Spark for ETL, Trino/DuckDB for BI and local analysis) without duplicating data formats.
- We must keep storage and compute predictable while handling bursty writes.

This design optimizes for correctness first (ACID + replayability), then latency, then cost.

## 2. Architecture Diagram

```text
                +-------------------+
                | Oracle OLTP       |
                | trips, drivers... |
                +---------+---------+
                          |
                          | redo logs
                          v
                +-------------------+
                | Debezium Connect  |
                | (Kafka topics)    |
                +---------+---------+
                          |
                          | CDC events (insert/update/delete)
                          v
 +-------------------------------------------------------------------+
 | Bronze Landing (Delta)                                            |
 | table: bronze.cdc_raw                                              |
 | columns: table_name, op, commit_ts, pk, raw_payload, pii_token... |
 | controls: schema registry check, tokenization, quarantine on fail  |
 +----------------------+------------------------------+--------------+
                        |                              |
                        | Delta CDF                    | audit events
                        v                              v
 +--------------------------------+       +---------------------------+
 | Silver Core (Delta)            |       | Security/Audit Delta      |
 | - silver.trips_current (SCD2)  |       | - pii_read_audit          |
 | - silver.drivers_current (SCD2)|       | - policy_change_log       |
 | - silver.payments_clean        |       +-------------+-------------+
 | merge rule: src.ts > tgt.ts    |                     |
 +------------------+-------------+                     |
                    |                                   |
                    | batch/stream agg                  |
                    v                                   |
 +--------------------------------+                     |
 | Gold Serving (Delta)           |<--------------------+
 | - gold.trip_kpis_5m            |
 | - gold.city_daily_metrics      |
 | - gold.driver_safety_flags     |
 +------------------+-------------+
                    |
                    | SQL
        +-----------+------------+
        | Spark / Trino / DuckDB |
        +------------------------+

Object storage (S3-compatible): Delta tables + checkpoints + versioned logs
Catalog + governance: unified metastore, column policies, lineage metadata
```

## 3. Key Decisions With Rejected Alternatives

### Decision A: Table format = Delta Lake

Chosen:
- Delta Lake for ACID, MERGE semantics, change data feed, and operational restore/time travel.

Rejected alternative 1:
- Plain Parquet folders.
- Rejected because no transaction log and no safe concurrent upserts; rollback and audit become manual and brittle.

Rejected alternative 2:
- Apache Iceberg for this specific workload.
- Rejected not because it is weak, but because team already runs Delta-native MERGE/CDF tooling and runbooks; migration cost and operational retraining are higher than the marginal gain right now.

### Decision B: Medallion layout with strict PII boundary in Bronze

Chosen:
- Bronze stores tokenized PII fields and encrypted raw payload only in a tightly scoped retention zone.
- Silver exposes analyst-safe, typed columns only.

Rejected alternative 1:
- Parse and clean directly into one curated table.
- Rejected because incident replay and forensic traceability are lost.

Rejected alternative 2:
- Keep full raw PII broadly accessible in Bronze.
- Rejected for Decree 13 risk and blast radius.

### Decision C: Late-arrival handling via MERGE predicate on event time

Chosen:
- Use `MERGE ... WHEN MATCHED AND src.ts > tgt.ts THEN UPDATE` plus idempotent dedup key.
- Keep SCD2 on entities where historical states matter (driver status, fare policy).

Rejected alternative 1:
- Last-writer-wins by ingestion timestamp.
- Rejected because network delays would overwrite newer business state with stale rows.

Rejected alternative 2:
- Full table overwrite per batch.
- Rejected due latency/cost and inability to meet <=60s freshness.

### Decision D: Gold design = pre-aggregated serving tables by SLA window

Chosen:
- Build 5-minute and daily aggregates in Gold keyed by `(window_start, city_id, service_type)` and `(date, city_id)`.
- Partition by date/hour and cluster by high-selectivity columns used in dashboards.

Rejected alternative 1:
- Query Silver directly for every dashboard request.
- Rejected because p95 <1s is not stable under peak concurrency.

Rejected alternative 2:
- Push all BI acceleration to proprietary warehouse copies.
- Rejected to avoid duplicating governance and lineage controls outside the lakehouse contract.

### Decision E: Governance model = deny-by-default + audited PII access

Chosen:
- Column-level access policies, masked views, and mandatory audit writes on every PII query path.
- Service accounts for pipelines; human access through approved roles only.

Rejected alternative 1:
- Trust-based access with periodic manual review.
- Rejected because it is not enforceable at real-time scale.

Rejected alternative 2:
- Per-team isolated copies for sensitive tables.
- Rejected because copies multiply compliance surface and drift from source-of-truth.

### Decision F: Cost strategy = compaction cadence + tiering policy

Chosen:
- Bronze hot 30 days on Standard tier, then IA; Silver 90 days hot; Gold 365 days hot for fast BI.
- Scheduled compaction and Z-order only for high-value tables/windows.

Rejected alternative 1:
- Aggressive OPTIMIZE on every micro-batch.
- Rejected because compute burn is high with little query gain.

Rejected alternative 2:
- No compaction.
- Rejected because small-file accumulation degrades query and metadata performance.

## 4. Failure Modes, Detection, and Rollback

### Failure mode 1: Bad CDC schema change breaks Silver MERGE

Symptoms:
- Stream job fails after upstream DDL change, or silently maps wrong types.

Detection:
- Schema contract check at Bronze ingest.
- Alert on mismatch ratio >0.1% over 5-minute window.

Rollback:
1. Pause Silver consumer.
2. Time travel read at last good Bronze version.
3. Patch mapping rules and replay CDC from checkpoint offset.
4. Validate row counts + null drift before resuming.

Day-18 concept tie:
- ACID + time travel recovery from known-good version.

### Failure mode 2: Duplicate event storm causes inflated KPIs

Symptoms:
- Sudden jump in trip count/revenue without matching upstream business signals.

Detection:
- Duplicate ratio monitor on `(table_name, pk, commit_ts_bucket)`.
- Data quality rule in Silver: unique keys after dedup.

Rollback:
1. Stop Gold refresh.
2. Recompute Silver for affected watermark interval with dedup logic.
3. Rebuild Gold windows from corrected Silver snapshots.

Day-18 concept tie:
- Medallion isolation: rebuild Gold from Silver without touching Bronze provenance.

### Failure mode 3: Out-of-order updates overwrite fresh state

Symptoms:
- Entity state regresses (for example driver status flips to old value).

Detection:
- Late-arrival histogram and "new row older than current row" counter.

Rollback:
1. Enforce `src.ts > tgt.ts` merge guard.
2. Reprocess impacted key range from Bronze CDF.
3. Compare SCD2 intervals for overlap anomalies.

Day-18 concept tie:
- MERGE semantics + deterministic replay.

### Failure mode 4: Unauthorized PII read path

Symptoms:
- Query against protected columns outside approved role path.

Detection:
- Policy engine denies query and emits security event.
- Daily audit reconciliation between query logs and `pii_read_audit`.

Rollback/containment:
1. Immediate session revoke.
2. Rotate service credentials if token compromise suspected.
3. Incident replay using immutable audit log + table history.

Day-18 concept tie:
- Governance + lineage + auditability on Delta artifacts.

## 5. Cost Back-of-Envelope (Monthly)

Assumptions:
- Sustained 2,500 writes/sec, peak 30,000 writes/sec.
- Average compressed Bronze event payload: 420 bytes/event.
- Bronze volume/day: `2,500 * 86,400 * 420 / 1e12 ~= 0.091 TB/day`.
- Monthly Bronze ingest: `0.091 * 30 ~= 2.73 TB/month`.
- Silver after projection/dedup: ~65% of Bronze size.
- Gold aggregates: ~8% of Silver size.

Storage retained by tier:
- Bronze hot (30d Standard): 2.73 TB.
- Bronze warm (next 335d IA equivalent monthly footprint): `2.73 * (335/30) ~= 30.49 TB`.
- Silver hot (90d Standard): `2.73 * 0.65 * 3 ~= 5.32 TB`.
- Gold hot (365d Standard): `2.73 * 0.65 * 0.08 * 12.17 ~= 1.73 TB`.

Unit prices (illustrative):
- Standard: $23/TB-month.
- IA: $12.5/TB-month.

Storage cost:
- Bronze hot: `2.73 * 23 = $62.79`
- Bronze warm: `30.49 * 12.5 = $381.13`
- Silver hot: `5.32 * 23 = $122.36`
- Gold hot: `1.73 * 23 = $39.79`
- Total storage ~= **$606/month**

Compute + platform estimate:
- CDC connectors + streaming jobs + scheduled optimize: ~$2,200/month.
- BI query compute (autoscaled SQL engines): ~$1,100/month.
- Metadata/governance/monitoring overhead: ~$500/month.
- Total compute/platform ~= **$3,800/month**.

Grand total estimated monthly run-rate:
- **~$4,400/month**.

This leaves budget headroom for peak-season overage and incident reprocessing.

## 6. One-Week MVP Slice

Goal:
- Prove correctness + compliance + freshness on one critical domain (`trips`).

Scope (7 days):
1. Build Bronze `cdc_raw` ingest for `trips` only with tokenization and schema checks.
2. Implement Silver `trips_current` with SCD2 and late-arrival MERGE guard (`src.ts > tgt.ts`).
3. Build Gold `trip_kpis_5m` for dashboard freshness tests.
4. Add three monitors:
   - freshness lag (source commit -> Gold availability),
   - duplicate ratio,
   - policy-denied PII attempts.
5. Add rollback runbook:
   - reproduce bad load,
   - restore to prior version,
   - replay from Bronze checkpoint.

Acceptance criteria:
- p95 freshness <=60s for 95% of windows.
- Gold count/revenue parity within 0.5% against controlled oracle snapshot.
- Unauthorized PII query is blocked and logged.
- Restore + replay for a bad 15-minute window completes in <=20 minutes.

## 7. Why This Design Is Defensible

This architecture is intentionally conservative where mistakes are expensive:
- correctness through ACID and replayable medallion flow,
- privacy through enforced boundary and audit,
- performance through serving-layer aggregates,
- cost through explicit tiering and compaction policy.

The design can evolve later (catalog migration, additional engines, feature-store extraction), but the core operational contract is already production-grade: every record has provenance, every change has a rollback path, and every sensitive read is traceable.
