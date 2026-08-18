# Bonus Architecture Brief — LLM Observability 1B Requests/Day

## 1. Problem Statement

Foundation-model API team ghi log 1B requests/ngay, trung binh 5 KB/request,
tuc khoang 5 TB raw/ngay. He thong phai cap nhat dashboard cost, latency,
error theo tenant moi 5 phut; giu prompt/response day du 7 ngay de incident
review; sau do chi giu aggregate 1 nam. PII khong duoc de con nguoi doc truc
tiep truoc khi redact/tokenize. Storage budget la hard cap 5,000 USD/thang.

Kho o day khong chi la volume. Hot path query la `tenant_id + time window`, nen
layout sai se bien dashboard 5 phut thanh scan hang chuc TB. Raw prompt/response
co PII va retention ngan, nen neu Bronze cho analyst doc truc tiep thi governance
hong ngay tu dau. Ngoai ra incident review can replay dung request trong 7 ngay,
trong khi FinOps yeu cau het 7 ngay phai xoa payload that, khong chi an bang
policy.

Gia cloud trong brief la gia public US East uoc tinh tai ngay 2026-08-18:
S3 Standard xap xi 23 USD/TB-month, Standard-IA xap xi 12.5 USD/TB-month,
Glacier Deep Archive xap xi 1 USD/TB-month. So thuc te phai pin theo region khi
go production.

## 2. Architecture Diagram

```text
API Gateway / Model Serving
        |
        | JSON event, request_id, tenant_id, model, tokens, latency, pii flags
        v
Kafka / MSK topic: llm_requests_raw  (24h retention, keyed by tenant_id)
        |
        v
Flink redaction job
  - deterministic tokenization: email, phone, account id
  - redact prompt/response spans
  - reject malformed schema to quarantine
        |
        +------------------------------+
        |                              |
        v                              v
Bronze Delta: bronze.llm_raw_7d        Bronze Delta: bronze.quarantine_30d
partition: ingest_date, hour           partition: ingest_date, error_type
cluster: tenant_id, request_id         access: security only
full redacted payload, 7-day TTL
        |
        v
Silver Delta: silver.llm_requests
partition: event_date, hour
cluster/Z-order: tenant_id, model, request_id
dedup by request_id, schema enforced, CDF enabled
payload pointer kept only while event_age <= 7d
        |
        +---------------------+--------------------------+
        |                     |                          |
        v                     v                          v
Gold Delta               Incident Review              Governance
gold.tenant_5m           7-day payload lookup          audit.pii_access
gold.tenant_daily        by tenant/request_id          OpenLineage events
gold.model_daily         time travel <= 7d             catalog policies
        |
        v
Trino/Spark SQL + dashboard cache
refresh every 5 min, p95 dashboard query < 10 s

Lifecycle:
Bronze/Silver payload columns: delete after 7 days via VACUUM after retention.
Gold aggregates: Standard-IA after 30 days, keep 365 days.
Audit/lineage metadata: Glacier Deep Archive after 90 days, keep 7 years.
```

## 3. Key Decisions and Rejected Alternatives

### Decision 1 — Table Format: Delta Lake for Bronze/Silver/Gold

I chose **Delta Lake** because the system needs high-rate append, MERGE-based
deduplication by `request_id`, Change Data Feed for downstream aggregate repair,
time travel during the 7-day incident window, and predictable VACUUM semantics.
The lab already showed that Delta gives ACID commits, schema enforcement,
OPTIMIZE/Z-order and CDF in one operational surface.

I rejected **plain Parquet on object storage** because it cannot protect schema
evolution, duplicate retries, or partial writes. At 1B requests/day, one bad
writer can poison dashboards before anyone notices.

I rejected **Iceberg as the first format** for this workload because the hot
problem is write-heavy observability with MERGE/CDF and Delta has a more direct
fit for streaming repair. Iceberg is still acceptable for long-lived aggregate
sharing if another engine ecosystem requires it, but not as the day-one path.

### Decision 2 — Catalog and Governance: Unity/Polaris-Compatible Catalog

I chose **a central REST-compatible catalog with table-level and column-level
policies**. Every table is registered; direct path access is blocked. PII columns
in Bronze are invisible to humans; only the redacted/tokenized Silver view is
queryable by analysts. All PII break-glass reads write to `audit.pii_access`.

I rejected **filesystem/path conventions only** because they depend on every
team remembering the rules. That fails when incident response is rushed.

I rejected **dashboard-level masking only** because ad-hoc SQL, exports, and
notebook users would bypass it. Governance has to live at the catalog and table
layer, not just the BI layer.

### Decision 3 — Medallion Layout and Retention

I chose **Bronze 7-day redacted payload, Silver 30-day request facts with payload
pointer TTL, Gold 365-day aggregates**. Bronze is append-only except quarantine
reprocessing. Silver deduplicates retries and normalizes tenant/model metadata.
Gold stores 5-minute and daily aggregates only.

I rejected **keeping full prompt/response for 365 days** because 5 TB/day raw is
150 TB/month before compression. Even compressed 3:1, one year would be about
608 TB, which breaks the storage cap and expands breach radius.

I rejected **aggregates-only from day zero** because incident review needs the
exact prompt/response for recent failures. Seven days is the compromise between
debuggability and data minimization.

### Decision 4 — Partitioning and Clustering

I chose **partition by `event_date` and `hour`, then cluster/Z-order by
`tenant_id`, `model`, `request_id`**. The partition bounds the time window; the
clustering makes per-tenant dashboard and incident lookups skip files. Target
file size is 256-512 MB after compaction to avoid both tiny files and slow
single-file reads.

I rejected **partition by tenant** because large enterprise tenants would create
skew and small tenants would create millions of tiny partitions.

I rejected **date-only partitioning without clustering** because the dashboard
hot path is not "all tenants for a day"; it is "one tenant for the last 5-60
minutes." Date-only layout would scan too much data.

### Decision 5 — Ingestion: Kafka + Flink Redaction Before Bronze

I chose **Kafka/MSK keyed by tenant_id plus Flink stateful redaction** before
writing Bronze. Redaction must happen before any lake reader sees data. The job
emits deterministic tokens for joins and masks prompt spans. Kafka retention is
24 hours, enough to replay a broken redaction deployment.

I rejected **batch upload every hour** because the dashboard SLA is 5 minutes and
late detection would be too slow.

I rejected **writing raw unredacted events to Bronze then redacting later**
because it violates the security constraint. Once raw PII lands in a broadly
readable lake path, the design has already failed.

### Decision 6 — Compression and Lifecycle

I chose **Parquet + ZSTD for Bronze/Silver, aggregate tables in Standard-IA after
30 days, audit metadata to Deep Archive after 90 days**. Payload text compresses
well; expected full-payload compression is 3:1. Lifecycle is enforced by table
maintenance jobs plus object-store lifecycle rules for immutable audit archives.

I rejected **Snappy everywhere** because CPU is cheap compared with keeping
hundreds of TB of text. Snappy is acceptable for short-lived Kafka spill, not
for lake payload.

I rejected **Glacier for 7-day payload** because incident review requires
interactive retrieval. Archive classes are for audit/lineage metadata, not hot
debug payload.

### Decision 7 — Aggregate Serving

I chose **Gold Delta aggregates queried by Trino/Spark plus a 5-minute dashboard
cache**. `gold.tenant_5m` is small enough to refresh continuously and isolates BI
from raw payload tables.

I rejected **querying Silver directly for every dashboard tile** because tenant
dashboards would compete with ingestion and maintenance on the same large table.

I rejected **precomputing every possible dimension** because tenant, model,
region, status and feature flags can explode cardinality. Gold keeps required
SLO dimensions; deeper analysis uses Silver with stricter quotas.

## 4. Failure Modes and Rollback

### Failure Mode 1 — Redaction Regression Leaks PII to Silver

Detection: Great Expectations-style checks scan Silver micro-batches for regex
PII patterns and compare redaction coverage to the previous 7-day baseline. Any
batch with leak score above threshold is stopped before Gold update.

Rollback: use Delta time travel to restore Silver and Gold to the last clean
version, replay Kafka from the safe offset with the previous redaction container,
and keep the bad batch in `bronze.quarantine_30d`. This ties directly to Day 18
time travel and schema/governance concepts.

### Failure Mode 2 — Small-File Storm After Traffic Spike

Detection: maintenance metrics alert when average file size drops below 64 MB or
file count per `event_date/hour` exceeds threshold. Dashboard latency rising
with unchanged record count confirms file-list overhead.

Rollback: pause dashboard refresh on Silver, run OPTIMIZE on affected hourly
partitions, then resume. If compaction creates bad files, restore table version
before OPTIMIZE and re-run with smaller parallelism.

### Failure Mode 3 — Late or Duplicate Events Distort Cost

Detection: Gold reconciliation compares `sum(cost_usd)` from Silver CDF against
Gold increments every 5 minutes. Drift above 0.1% pages the on-call.

Rollback: rebuild affected Gold windows from Silver versions using CDF. Because
Silver is deduped by `request_id`, replaying the window is idempotent.

### Failure Mode 4 — Schema Evolution Breaks Consumers

Detection: catalog contract tests run on every writer release. Additive columns
are allowed; type changes and dropped fields fail CI. Runtime schema mismatch
alerts include writer app version and table version.

Rollback: block the new writer, restore the table to the previous version if a
bad commit landed, and publish a compatibility view while downstream teams adapt.

### Failure Mode 5 — Lifecycle Job Misses Payload Deletion

Detection: daily retention audit checks max `event_date` for payload columns and
object count under payload prefixes older than 7 days. Any nonzero count is a
security alert, not just a cost alert.

Rollback: run targeted DELETE on expired payload rows, VACUUM after retention,
then verify object inventory. If orphan files remain, run an explicit directory
minus transaction-log sweep, matching the Day 18 lesson that VACUUM may not see
uncommitted orphans.

## 5. Back-of-Envelope Cost

### Storage

Assumptions:

- 1B requests/day x 5 KB raw = 5 TB/day raw.
- Parquet ZSTD compression for text payload: 3:1, so 1.67 TB/day stored.
- Full redacted payload kept 7 days: 11.7 TB hot.
- Silver request facts without payload after TTL: 0.5 KB/request compressed,
  about 0.5 TB/day; keep 30 days = 15 TB hot/warm.
- Gold aggregates: 5-minute tenant/model/status metrics, about 200 GB/month,
  kept 12 months = 2.4 TB.
- Audit and lineage metadata: 50 GB/day compressed, 90 days Standard then Deep
  Archive for 7 years.

Monthly storage estimate:

```text
Bronze/Silver 7-day payload:
11.7 TB x 23 USD/TB-month = 269 USD/month

Silver facts 30 days:
15 TB x 23 USD/TB-month = 345 USD/month

Gold aggregates, 2.4 TB in Standard-IA:
2.4 TB x 12.5 USD/TB-month = 30 USD/month

Audit/lineage, 4.5 TB Standard + 123 TB Deep Archive:
(4.5 TB x 23) + (123 TB x 1) = 226.5 USD/month

Storage subtotal ~= 871 USD/month
Add 30% for metadata, requests, object overhead, failed files:
~= 1,132 USD/month
```

This is below the 5,000 USD/month storage cap. The cap is achievable because
full prompt/response payload is deleted after 7 days, not archived forever.

### Compute and Streaming

Compute is not part of the stated storage cap, but it is part of the operating
budget:

```text
Kafka/MSK: 6 x kafka.m7g.large
6 x 744 h x 0.204 USD/h ~= 911 USD/month
Broker storage/buffer: 10 TB x 100 USD/TB-month ~= 1,000 USD/month

Flink redaction: 12 workers x 0.20 USD/h x 744 h ~= 1,786 USD/month

Spark maintenance and Gold aggregation:
8 workers x 4 h/day x 30 days x 0.35 USD/h ~= 336 USD/month

Trino dashboard/query pool:
4 workers x 12 h/day x 30 days x 0.35 USD/h ~= 504 USD/month

Compute subtotal ~= 4,537 USD/month
```

Total operating estimate: about 5,700 USD/month, of which storage is about
1,100 USD/month. The primary cost risk is not S3 capacity; it is streaming and
always-on query capacity.

## 6. One-Week MVP Slice

The first shippable slice should prove the risky path end to end, not build the
whole platform.

Day 1: create catalog namespaces and Delta tables:
`bronze.llm_raw_7d`, `bronze.quarantine_30d`, `silver.llm_requests`,
`gold.tenant_5m`, `audit.pii_access`.

Day 2: implement a local Kafka-compatible producer or file replay with 10M
synthetic events, including duplicate `request_id`, tenant skew, and seeded PII.

Day 3: build the Flink/Spark structured-streaming redaction job. It must fail
closed: unparseable events go to quarantine, not Silver.

Day 4: implement Silver MERGE/dedup and CDF-enabled Gold 5-minute aggregation.
Dashboard query must filter `tenant_id` and last hour from Gold.

Day 5: run OPTIMIZE/Z-order on Silver and measure query files scanned before and
after. Target at least 10x file-pruning improvement for one tenant.

Day 6: implement retention proof: delete payload older than 7 simulated days,
VACUUM, then verify no expired payload files remain outside the transaction log.

Day 7: run a failure drill: inject a redaction bug, detect PII leak in Silver,
restore to previous Delta version, replay from Kafka offset, and regenerate Gold.

Acceptance criteria:

- dashboard aggregate refresh < 5 minutes on the synthetic stream;
- zero unredacted seeded PII visible in analyst role;
- duplicate requests do not double-count cost;
- incident lookup works for payload age <= 7 days and fails intentionally after
  lifecycle deletion;
- documented storage estimate remains below 5,000 USD/month under the stated
  assumptions.

