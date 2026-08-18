# Vietnamese Ride-Hailing CDC to Lakehouse

## 1. Problem Statement

A Vietnamese ride-hailing platform streams production Oracle changes into a lakehouse for analytics and compliance. Scale is 100 million trips per year, with peak writes around 30,000 events per second. The data contains regulated PII under Decree 13/2023/ND-CP: rider and driver phone numbers, national IDs, pickup/dropoff GPS, payment metadata, and support notes.

The business needs dashboards refreshed within 60 seconds of source commit and ad-hoc analyst queries at p95 under 1 second for common slices such as city, driver segment, and day. Late events are common because mobile devices and provincial networks disconnect often. The hard part is combining low-latency CDC, privacy controls, reproducible history, and operational maintenance without turning the lakehouse into a pile of stale Parquet files.

## 2. Architecture Diagram

```text
Oracle OLTP
  |
  | Debezium CDC: insert/update/delete, source commit ts, schema version
  v
Kafka topics: trips, drivers, riders, payments
  |
  | streaming ingest, idempotent by source_txn_id + row_pk + op_seq
  v
Bronze Delta tables
  - raw_cdc_events partitioned by ingest_date
  - PII tokenized at landing
  - encrypted raw payload retained 7 days only
  |
  | MERGE, late-event rule: src.event_ts > tgt.event_ts
  v
Silver Delta tables
  - trips_current
  - trips_scd2
  - drivers_scd2
  - riders_scd2
  - pii_access_audit
  |
  | compaction, Z-order by city_id/trip_date/tenant_id
  | quality checks + lineage events
  v
Gold tables
  - city_revenue_5min
  - driver_utilization_daily
  - cancellation_funnel
  - compliance_extracts

Query paths:
  Dashboards -> Gold aggregates, refresh every 5 min
  Analysts   -> Silver/Gold via governed SQL endpoint
  Compliance -> SCD2 + audit + time travel version pins
```

## 3. Key Decisions and Rejected Alternatives

### Decision 1: Table Format

I chose **Delta Lake** for Bronze, Silver, and Gold. CDC workloads need reliable `MERGE`, time travel, schema enforcement, and Change Data Feed semantics.

I rejected **raw Parquet** because it cannot safely express row-level updates, deletes, or rollback after a bad CDC batch. I rejected **plain Kafka retention as the system of record** because Kafka is excellent for transport but weak for long-term analyst queries, compaction history, and reproducible compliance extracts.

### Decision 2: CDC Ingestion

I chose **Debezium into Kafka, then streaming writes into Bronze Delta**. Each row carries `source_txn_id`, `row_pk`, `op_seq`, `source_commit_ts`, `event_ts`, and `schema_version`.

I rejected **nightly Oracle dumps** because the dashboard SLA is 60 seconds, not 24 hours. I rejected **direct analyst reads from Oracle replicas** because high-cardinality ad-hoc queries would couple analytics load to the production system.

### Decision 3: PII Handling

I chose **tokenization at Bronze landing**. Phone numbers, IDs, and precise GPS are tokenized before normal analysts can read them. The original encrypted payload is retained for 7 days for incident response, then deleted.

I rejected **PII masking only in BI dashboards** because raw tables would still leak sensitive fields to notebooks or ad-hoc SQL. I rejected **dropping all PII immediately** because compliance and support investigations sometimes need short-lived, tightly audited recovery.

### Decision 4: Late Data and SCD2

I chose **MERGE with event-time guards** for current tables and **SCD Type 2** for entities whose history matters. The merge rule is `WHEN MATCHED AND src.event_ts > tgt.event_ts`, preventing stale provincial reconnects from overwriting newer state.

I rejected **last-write-wins by ingest time** because a delayed mobile event could corrupt current trip status. I rejected **append-only Silver tables only** because every dashboard would need expensive dedup logic at query time.

### Decision 5: Partitioning and Clustering

I chose **date-based partitions plus clustering/Z-order on city_id, trip_date, and tenant/partner id**. Most dashboards filter by recent time windows and city. Compaction runs hourly for hot partitions and daily for older partitions.

I rejected **partitioning by driver_id or rider_id** because it would create many tiny partitions. I rejected **only partitioning by date without clustering** because city dashboards would scan too many files inside busy days.

### Decision 6: Governance and Catalog

I chose **a central governed catalog with table ownership, column tags, and PII access logging**. Every read of sensitive columns writes to `silver.pii_access_audit` with user, purpose, query id, and timestamp.

I rejected **folder-based access control only** because it cannot express column-level policy. I rejected **manual spreadsheet lineage** because risk teams need answers in minutes, not after a week of interviews.

## 4. Failure Modes

### Failure Mode 1: Debezium connector replays events after restart

Detection: spike in duplicate `source_txn_id + row_pk + op_seq` counts at Bronze.

Rollback: Bronze is append-safe, and Silver MERGE is idempotent. Re-run the affected micro-batches from the last good checkpoint and compare Silver row counts against the transaction audit table.

### Failure Mode 2: Bad schema evolution adds an unsafe column

Detection: schema registry blocks incompatible changes, and Delta schema enforcement rejects unexpected writes unless the pipeline explicitly approves schema evolution.

Rollback: use Delta time travel to restore Silver to the last approved version, then replay Bronze CDC excluding the bad schema version. This ties directly to the Day 18 time-travel and schema-enforcement concepts.

### Failure Mode 3: Small files make dashboard p95 exceed 1 second

Detection: file count per hot partition and query p95 cross thresholds. For example, more than 2,000 files in the current-day trip partition triggers an alert.

Rollback: run targeted compaction on the affected partitions, then Z-order city-heavy tables. If a compaction job corrupts layout, revert to the previous Delta version and retry with smaller batches.

### Failure Mode 4: Unauthorized PII access path appears

Detection: compare catalog query logs to `pii_access_audit`; any sensitive column read without an audit row pages the data owner.

Rollback: revoke the offending grant, rotate the tokenization key if needed, and rebuild the exposed Silver/Gold tables from Bronze with corrected policy.

## 5. Cost Back-of-Envelope

Assume 100 million trips/year and 5 CDC mutations per trip on average. That is 500 million CDC events/year. At 2 KB compressed per Bronze event, Bronze is roughly:

```text
500M events * 2 KB = 1 TB/year
```

Silver and Gold add approximately 2x because SCD2, indexes, aggregates, and audit tables duplicate useful columns:

```text
Bronze 1 TB + Silver/Gold 2 TB = 3 TB active analytical data
```

Storage estimate on S3-like object storage:

```text
Hot 90 days: 0.75 TB * $23/TB-month = $17.25/month
Warm remainder: 2.25 TB * $12.5/TB-month = $28.13/month
Raw encrypted PII payload: 7 days only, about 0.02 TB * $23/TB-month = $0.46/month
```

The storage bill is not the main cost. Compute dominates:

```text
Streaming CDC jobs: 2 small workers * $0.30/hour * 24 * 30 = $432/month
Maintenance jobs: 2 hours/day * $1.00/hour * 30 = $60/month
Dashboard SQL warehouse: 8 hours/day * $2.00/hour * 30 = $480/month
```

Estimated monthly total:

```text
Storage ~$46 + compute ~$972 = ~$1,018/month
```

This estimate leaves room for higher event size, cross-region replication, and governance tooling while staying operationally realistic.

## 6. One-Week MVP

The first shippable slice is one CDC entity: `trips`.

Day 1: Create Debezium topic contract and Bronze Delta table with tokenized rider/driver identifiers.

Day 2: Build idempotent Bronze ingestion keyed by `source_txn_id + row_pk + op_seq`.

Day 3: Build Silver `trips_current` using MERGE with the late-event guard `src.event_ts > tgt.event_ts`.

Day 4: Build `trips_scd2` and prove time travel by replaying a bad update and restoring.

Day 5: Build one Gold dashboard table: `city_revenue_5min`.

Day 6: Add PII audit logging and a blocked unauthorized-read test.

Day 7: Add compaction for hot partitions and publish evidence: p95 query latency, duplicate replay behavior, and rollback from a bad schema version.

This MVP proves the hard parts: CDC correctness, late-data handling, privacy control at Bronze, Delta rollback, and maintenance discipline.
