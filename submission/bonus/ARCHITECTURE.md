# Architecture Brief: Lakehouse for 1B LLM Requests per Day

## 1. Problem Statement

We operate an LLM API platform serving approximately **1 billion requests per day**. Each request produces roughly **5 KB** of observability data including request metadata, prompt, response, token counts, latency, model version, tenant information, errors, and billing information.

This produces approximately:

```text
1B requests/day × 5 KB ≈ 5 TB/day
≈ 150 TB/month raw
```

The platform has four major requirements:

1. Cost and latency dashboards must refresh within **5 minutes**.
2. Full prompt and response data must remain available for incident investigation for **7 days**.
3. Personally identifiable information must be removed or tokenized before analysts can access the data.
4. Total storage cost should remain below approximately **$5,000/month**.

The difficulty is that the same dataset has two conflicting personalities. Recent data must be fast and highly queryable, while historical raw payloads become economically dangerous at this scale.

The architecture must therefore combine streaming ingestion, Medallion processing, ACID tables, aggressive lifecycle management, data clustering, privacy enforcement, and compact long-term aggregates.

---

# 2. Architecture

```text
                         ┌─────────────────────┐
                         │   LLM API Gateway   │
                         │  ~1B requests/day   │
                         └──────────┬──────────┘
                                    │
                                    │ events
                                    ▼
                         ┌─────────────────────┐
                         │ Kafka / Event Bus   │
                         │ partition: tenant   │
                         └──────────┬──────────┘
                                    │
                         streaming micro-batch
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         BRONZE                                      │
│ Delta Lake                                                          │
│                                                                     │
│ Raw request metadata                                                │
│ tokenized user identifiers                                          │
│ encrypted prompt/response                                           │
│ ingestion timestamp                                                 │
│ model/version                                                       │
│                                                                     │
│ partition: event_date                                               │
│ retention: 7 days for full payload                                  │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               │ validation
                               │ PII redaction
                               │ deduplication
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SILVER                                      │
│                                                                     │
│ Sanitized request records                                           │
│ schema enforced                                                     │
│ tenant_id tokenized                                                 │
│ normalized model metadata                                           │
│ latency / token / cost fields                                       │
│                                                                     │
│ partition: event_date                                               │
│ cluster: tenant_id, model                                           │
└──────────────────────────────┬──────────────────────────────────────┘
                               │
                               │ continuous aggregation
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          GOLD                                       │
│                                                                     │
│ tenant_5min_metrics                                                  │
│ model_daily_metrics                                                  │
│ cost_daily                                                           │
│ latency_percentiles                                                  │
│ error_rate                                                           │
│                                                                     │
│ retention: 1 year                                                   │
└───────────────┬──────────────────┬──────────────────────────────────┘
                │                  │
                ▼                  ▼
        ┌──────────────┐    ┌──────────────┐
        │ Dashboards   │    │ FinOps / SLA │
        │ p50 / p95    │    │ monitoring   │
        │ cost / error │    │ alerts       │
        └──────────────┘    └──────────────┘

                 Object Storage Lifecycle

       Hot                     Warm                     Deleted
   0 ───────── 7 days ───────────── 365 days
   Full payload         Aggregates only
```

The catalog acts as the control plane for table discovery, schema evolution, permissions, and version tracking.

---

# 3. Architecture Decisions

## Decision 1: Delta Lake as the primary table format

I choose **Delta Lake** for Bronze, Silver, and Gold tables.

The main reason is the workload's dependence on continuous writes, schema enforcement, MERGE operations, time travel, and operationally simple maintenance.

The Bronze and Silver pipelines are effectively streaming tables rather than archival datasets.

### Alternative rejected: plain Parquet

Plain Parquet would minimize table-format complexity, but it provides no native transaction log.

At 1B requests/day, partial writes and concurrent readers are unavoidable. A dashboard reading a directory while a job is writing hundreds of files could observe an inconsistent snapshot.

Delta gives readers a transactionally consistent table version.

### Alternative rejected: Iceberg

Iceberg would also be a technically valid choice and has excellent catalog interoperability and hidden partitioning.

However, this platform's primary workloads are streaming ingestion, MERGE-heavy sanitation, retention, and operational observability rather than cross-engine analytical federation.

Delta therefore minimizes implementation complexity for this specific system.

Iceberg would become more attractive if Trino, Snowflake, Spark, and external partners all needed first-class access to the same tables.

---

# Decision 2: Medallion architecture

I choose a **Bronze → Silver → Gold** layout.

Bronze represents ingestion truth.

Silver represents policy-compliant analytical truth.

Gold represents query-optimized business metrics.

### Bronze

Contains:

* request_id
* tenant_id token
* model
* model_version
* request timestamp
* response timestamp
* token counts
* status
* error
* encrypted prompt
* encrypted response
* ingestion metadata

Bronze exists primarily for replay and incident investigation.

### Silver

Silver performs:

* schema validation
* deduplication
* PII redaction
* invalid-event quarantine
* token-cost calculation
* latency derivation
* model normalization

Analysts never query Bronze directly.

### Gold

Gold contains precomputed metrics such as:

```text
tenant_id
window_start
model
request_count
input_tokens
output_tokens
cost_usd
p50_latency
p95_latency
p99_latency
error_rate
```

### Alternative rejected: a single large request table

One universal table appears simpler initially but mixes incompatible requirements.

Incident investigations need raw payloads.

Dashboards need tiny aggregates.

Security teams need strict access boundaries.

Storing all three semantics in the same table would create unnecessary scans and much broader access to sensitive content.

---

# Decision 3: Partition by date, cluster by tenant

I choose:

```text
PARTITION BY event_date
CLUSTER / Z-ORDER BY tenant_id, model
```

Date partitioning maps naturally to lifecycle management.

A seven-day raw retention policy can therefore remove complete date partitions rather than issuing row-level deletes over hundreds of billions of records.

The dominant interactive query is expected to resemble:

```sql
SELECT ...
FROM silver_requests
WHERE tenant_id = ?
  AND event_ts >= ?
  AND event_ts < ?
```

Clustering by tenant improves file skipping within each daily partition.

### Alternative rejected: partition by tenant

There may be thousands or millions of tenants.

Partitioning by tenant would therefore create a huge number of small partitions and dramatically increase metadata and small-file pressure.

### Alternative rejected: partition by model

The number of models is manageable, but model is not the dominant lifecycle dimension.

Deletion policies operate on time.

Model partitioning would therefore complicate retention while giving relatively little benefit.

---

# Decision 4: Tokenize PII before Silver

I choose deterministic tokenization for identifiers such as:

```text
user_id
account_id
email
phone
```

Sensitive values are transformed before they enter the general analytical path.

The mapping between tokens and original identities is stored in a separate restricted system rather than inside the lakehouse.

Analysts therefore see:

```text
tenant_75fa...
user_814d...
```

instead of direct identifiers.

Prompts and responses remain encrypted in Bronze and are accessible only to a restricted incident-response role.

### Alternative rejected: redact during queries

Query-time masking leaves raw PII broadly accessible inside storage.

A misconfigured engine, export job, notebook, or permission could bypass the masking layer.

### Alternative rejected: permanently hash everything

Irreversible hashing is attractive for privacy but prevents authorized investigations that legitimately need identity recovery.

Tokenization gives controlled reversibility while maintaining analytical joins.

---

# Decision 5: Seven-day raw payload retention

I choose to retain complete prompt and response payloads for **7 days**.

After seven days:

```text
prompt            DELETE
response          DELETE
request metadata  DELETE or aggressively compact
Gold aggregates   KEEP 365 days
```

The architecture intentionally avoids storing full LLM traffic for a year.

At 5 TB/day:

```text
5 TB/day × 365
= 1,825 TB
≈ 1.8 PB/year
```

That scale would rapidly dominate the budget.

With seven-day raw retention:

```text
5 TB/day × 7
= 35 TB
```

which is dramatically easier to manage.

### Alternative rejected: retain raw traffic for one year

Incident value falls sharply as data ages while storage cost remains linear.

The product requirement only requires long-term aggregates.

Keeping raw payloads would therefore be economically unjustified.

### Alternative rejected: retain only aggregates

This would minimize storage but make production incidents difficult to investigate because individual request and response trajectories would disappear.

Seven days provides a useful operational debugging window.

---

# Decision 6: Continuous Gold aggregation

I choose **5-minute micro-batch aggregation**.

Every batch updates:

```text
tenant_5min_metrics
model_5min_metrics
platform_5min_metrics
```

Dashboards query Gold instead of scanning billions of Silver rows.

For example:

```sql
SELECT
    tenant_id,
    SUM(request_count),
    SUM(cost_usd),
    MAX(p95_latency)
FROM tenant_5min_metrics
WHERE event_date = CURRENT_DATE
GROUP BY tenant_id;
```

### Alternative rejected: dashboard queries directly against Silver

Even with aggressive clustering, repeatedly scanning hundreds of millions or billions of request records is unnecessary work.

### Alternative rejected: pure real-time per-event OLAP pipeline

A sub-second streaming OLAP stack could provide lower latency but would increase system complexity.

The requirement is a **5-minute dashboard refresh**, so micro-batching provides a better cost-to-complexity tradeoff.

---

# Decision 7: Mandatory maintenance jobs

The lakehouse will operate scheduled maintenance rather than treating it as optional housekeeping.

## Compaction

Streaming ingestion produces small files.

Every hour, recent partitions are compacted toward approximately:

```text
256 MB to 1 GB/file
```

depending on workload.

## Clustering

Hot partitions are periodically clustered on:

```text
tenant_id
model
```

This improves file skipping.

## Vacuum / snapshot expiration

Old table versions are removed according to the operational rollback window.

## Orphan detection

Storage objects are compared against files referenced by the transaction log.

This matters because an uncommitted file created by a failed writer may never become visible to normal table vacuum logic.

## Checkpoint maintenance

Transaction logs are checkpointed to prevent replaying an excessive number of commits when tables are opened.

### Alternative rejected: run maintenance only when queries become slow

By that time the small-file and metadata problem may already involve millions of objects.

Maintenance is therefore part of the architecture, not emergency repair.

---

# 4. Query Strategy

There are three major query classes.

## Operational dashboard

Reads Gold.

Typical latency target:

```text
< 2 seconds
```

Data freshness:

```text
≤ 5 minutes
```

## Incident investigation

Reads recent Silver records and, under restricted permission, encrypted Bronze payloads.

Example:

```sql
SELECT *
FROM silver_requests
WHERE tenant_id = 'tenant_123'
  AND request_id = 'req_xyz';
```

## FinOps

Reads daily Gold tables.

Example questions:

* cost per tenant
* cost per model
* tokens per successful request
* cost per million tokens
* abnormal tenant growth
* budget forecast

---

# 5. Data Lifecycle

The architecture intentionally applies different retention to different information classes.

| Data                       |        Retention |
| -------------------------- | ---------------: |
| Full prompt / response     |           7 days |
| Sanitized request metadata |       30–90 days |
| 5-minute aggregates        |          90 days |
| Daily aggregates           |         365 days |
| Compliance audit logs      | policy dependent |

Deletion is partition-oriented wherever possible.

For example:

```text
bronze/event_date=2026-08-01/
bronze/event_date=2026-08-02/
...
```

Once the retention boundary passes, entire old partitions can be removed and subsequently vacuumed.

---

# 6. Cost Estimate

The workload produces:

```text
5 TB/day raw
```

Seven days of uncompressed traffic would require:

```text
35 TB
```

Assume Parquet compression reduces stored bytes by approximately **3×** for metadata-heavy request records:

```text
35 TB / 3
≈ 11.7 TB
```

Suppose hot object storage costs approximately:

```text
$23/TB/month
```

Then raw seven-day storage costs approximately:

```text
11.7 TB × $23
≈ $269/month
```

Silver metadata may represent approximately 20% of the original payload volume.

Assume:

```text
1 TB/day logical Silver
÷ 3 compression
≈ 0.33 TB/day
```

For 30 days:

```text
0.33 × 30
≈ 10 TB
```

Cost:

```text
10 TB × $23
≈ $230/month
```

Gold aggregates are tiny by comparison.

Suppose they consume 2 TB across the full yearly retention:

```text
2 TB × $23
≈ $46/month
```

Base object storage is therefore approximately:

```text
$269
+ $230
+ $46
≈ $545/month
```

Even allowing several multiples for replication, metadata, temporary files, logs, and lifecycle overlap gives substantial room under the **$5,000/month storage ceiling**.

The more significant cost risk is compute.

Assume streaming and maintenance compute averages:

```text
20 workers × $0.40/hour
× 24 hours × 30 days

≈ $5,760/month
```

Compute therefore requires more aggressive optimization than storage.

Gold aggregation, partition pruning, and compaction are important not merely for latency but also for controlling repeated scan costs.

---

# 7. Failure Modes

## Failure Mode 1: Bad schema deployment

A producer accidentally changes:

```text
tokens: INT
```

to:

```text
tokens: STRING
```

### Detection

Schema enforcement rejects the write and raises an ingestion alert.

Bad records enter quarantine rather than Silver.

### Recovery

Rollback the producer.

Replay the affected Bronze events using the previous schema.

If a bad schema was accidentally committed through explicit schema evolution, use Delta history to identify the previous table version and restore it.

This directly uses the Day 18 **schema enforcement and time-travel** mechanisms.

---

# Failure Mode 2: Streaming writer crashes and leaves orphan files

A Spark executor writes Parquet objects but crashes before the Delta transaction commits.

Those files consume storage but are invisible to table readers.

### Detection

A daily orphan scanner calculates:

```text
physical_storage_files
-
transaction_log_referenced_files
```

Any old unreferenced files become candidates for cleanup.

### Recovery

Files younger than a safety interval are ignored.

Older confirmed orphan objects are deleted.

This avoids assuming that `VACUUM` alone catches every storage leak.

---

# Failure Mode 3: Incorrect PII-redaction rule

A new prompt field begins carrying an email address but the Silver redactor does not recognize it.

### Detection

A PII canary scanner samples Silver records and runs deterministic detection rules.

Any detected clear-text PII blocks publication of that batch.

### Recovery

Stop Silver-to-Gold promotion.

Patch the redaction function.

Reprocess Bronze using time travel from the last known clean input version.

The compromised Silver versions are removed after the security retention process completes.

---

# Failure Mode 4: Aggressive vacuum destroys rollback window

An operator accidentally configures retention shorter than the intended incident recovery window.

### Detection

The maintenance job checks:

```text
requested_retention >= minimum_retention
```

before executing.

Policy violations make the job fail closed.

### Recovery

Restore from replicated object storage if files were physically removed.

Metadata-only time travel cannot recover a file that has already been permanently deleted, so backup and retention policies must align.

---

# Failure Mode 5: Tenant query becomes slow despite partition pruning

One tenant generates disproportionately large traffic and its files become poorly clustered.

### Detection

Monitor:

```text
files scanned/query
bytes scanned/query
p95 dashboard latency
```

If tenant-specific queries scan an increasing fraction of a date partition, clustering quality has degraded.

### Recovery

Run targeted OPTIMIZE / clustering against hot date partitions rather than rewriting the entire table.

---

# 8. Governance

Access is divided by data layer.

## Bronze

Accessible only to:

```text
platform ingestion service
security
incident-response role
```

## Silver

Accessible to:

```text
data engineering
approved analytics
ML teams
```

Direct PII has already been tokenized.

## Gold

Accessible broadly to:

```text
product analytics
finance
operations
tenant dashboards
```

Every privileged Bronze access produces an immutable audit event containing:

```text
user
role
table
timestamp
query purpose
request / incident reference
```

The catalog therefore acts as the control plane rather than simply a table directory.

---

# 9. Observability for the Lakehouse Itself

A production observability lakehouse must also observe itself.

We track:

```text
ingestion_lag_seconds
bronze_rows_per_minute
silver_rejected_rows
small_file_count
average_file_size
orphan_file_bytes
gold_refresh_lag
bytes_scanned_per_dashboard_query
storage_bytes_by_layer
cost_usd_by_layer
```

Critical alerts include:

```text
Gold freshness > 5 minutes
Silver rejection rate > threshold
PII canary failure
Orphan storage growth
Average file size < target
Dashboard scan bytes unexpectedly increase
```

---

# 10. One-Week MVP

The first week should prove the architecture's highest-risk assumptions rather than attempt the complete 1B-request system.

## Day 1

Generate or replay approximately:

```text
10–100 million synthetic LLM request records
```

with realistic tenant skew.

Create Bronze Delta ingestion.

## Day 2

Implement:

```text
schema enforcement
PII tokenization
deduplication
Silver transformation
```

## Day 3

Create 5-minute Gold aggregation containing:

```text
request_count
cost_usd
p50 latency
p95 latency
error_rate
```

## Day 4

Benchmark tenant filtering before and after clustering.

Record:

```text
files scanned
bytes scanned
query latency
```

## Day 5

Implement lifecycle and maintenance:

```text
compaction
clustering
vacuum
orphan scan
checkpointing
```

## Day 6

Simulate failures:

1. invalid schema
2. duplicate events
3. abandoned file
4. bad Silver transformation

Verify replay and time travel.

## Day 7

Produce a dashboard and architecture review containing measured results.

The MVP succeeds if it proves:

```text
Gold freshness <= 5 minutes

Silver never exposes the test PII fields

tenant queries demonstrate meaningful file skipping

raw data can be deleted by lifecycle policy

a bad transformation can be rolled back and replayed

maintenance reduces small-file count substantially
```

---

# 11. Key Trade-Off Summary

The architecture deliberately favors operational simplicity over maximal flexibility.

I choose:

```text
Delta
over plain Parquet or Iceberg

Medallion
over one universal table

date partitioning + tenant clustering
over tenant partitions

tokenization before analytics
over query-time masking

7-day raw retention
over indefinite raw history

5-minute Gold micro-batches
over per-event analytical updates

scheduled maintenance
over reactive cleanup
```

These choices are driven by the actual constraints:

```text
1B requests/day
5 TB/day
5-minute freshness
7-day forensic history
1-year aggregate retention
PII protection
$5K/month storage cap
```

The central architectural principle is that **not all data deserves the same lifetime or performance tier**.

Recent request-level data is operationally valuable but expensive.

Historical aggregates are cheap and analytically valuable.

The Lakehouse succeeds by allowing both to coexist while ACID transactions, versioning, lifecycle management, governance, and maintenance keep the boundary controlled.
