# Bonus Challenge — LLM Observability Lakehouse at 1B Requests/Day

## 1. Problem statement

A foundation-model API platform receives about **1 billion LLM requests per day**. At roughly **5 KB per request**, the raw observability stream is about **5 TB/day** before compression. The platform needs near-real-time cost, latency, and error dashboards by tenant with a **5-minute refresh SLA**. Full prompts and responses must remain available for incident review for **7 days**, but only aggregate metrics should be retained for **1 year**. PII must be redacted or tokenized before any analyst or dashboard user can read the data. The hard constraint is cost: total storage should stay near or below **$5K/month**.

This is difficult because the system has conflicting needs: cheap long-term retention, fast tenant-level dashboards, secure incident forensics, and high-volume streaming ingestion. The architecture must separate raw, sensitive, short-lived data from cleaned analytical data and long-lived aggregates while preserving auditability, schema evolution, and rollback options.

## 2. Architecture diagram

```text
                         ┌──────────────────────────────┐
                         │ Foundation Model API Gateway │
                         │ request_id, tenant_id, model │
                         │ prompt, response, token use  │
                         └──────────────┬───────────────┘
                                        │ streaming logs
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ Ingestion Layer                                                             │
│ Kafka / Kinesis topics partitioned by event_time + tenant hash              │
│ - validates envelope schema                                                 │
│ - encrypts payload in transit                                               │
│ - writes append-only landing files every 1–5 minutes                        │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ BRONZE: raw_llm_requests_delta                                              │
│ Storage: object storage, Delta Lake, partitioned by date/hour               │
│ Data: raw request/response + metadata                                       │
│ Security: PII tokenization/redaction before table becomes readable          │
│ Retention: 7 days full payload, then lifecycle delete                       │
└──────────────────────────────┬──────────────────────────────────────────────┘
                               │ structured streaming + schema enforcement
                               ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│ SILVER: llm_requests_clean                                                  │
│ Data: parsed JSON, tenant_id, model, status, latency_ms, token counts,      │
│       pii_tokens, redacted_prompt, redacted_response                        │
│ Layout: partition by date/hour, clustered/Z-ordered by tenant_id + model    │
│ Governance: row/column policies, PII read audit table, data quality checks  │
│ Retention: 7 days for redacted request-level records                        │
└───────────────┬───────────────────────────────┬─────────────────────────────┘
                │ 5-minute incremental aggregate│ controlled incident access
                ▼                               ▼
┌─────────────────────────────────────┐   ┌───────────────────────────────────┐
│ GOLD: llm_tenant_5min_metrics       │   │ Incident Review Path              │
│ Data: tenant/model/window metrics   │   │ Break-glass role, audited reads   │
│ p50/p95 latency, errors, token cost │   │ Time travel to exact bad version  │
│ Layout: date + 5-min window,        │   │ 7-day payload lookup only         │
│ clustered by tenant_id              │   └───────────────────────────────────┘
│ Retention: 1 year aggregates        │
└─────────────────┬───────────────────┘
                  │
                  ▼
        ┌─────────────────────┐
        │ Dashboard / Alerts  │
        │ tenant cost, p95,   │
        │ error rate, budget  │
        └─────────────────────┘
```

## 3. Key decisions and rejected alternatives

### Decision 1 — Table format: Delta Lake for the primary observability lakehouse

I choose **Delta Lake** for Bronze, Silver, and Gold because the workload needs ACID writes, streaming ingestion, MERGE-friendly incremental processing, time travel for incident rollback, Change Data Feed for incremental aggregate refresh, and practical OPTIMIZE/Z-order support for the tenant hot path.

I reject **raw Parquet on object storage** because it is cheap but not operationally safe at 1B requests/day. Without a transaction log, readers can see partial writes, schema drift is harder to control, rollback is manual, and incident review becomes a file-hunting problem.

I also considered **Apache Iceberg**. Iceberg is strong for multi-engine interoperability, hidden partitioning, and vendor-neutral catalogs. I reject it for the first implementation because this team is Spark-first and needs the lowest-friction path for streaming MERGE, Z-order-like clustering, and Delta CDF. Iceberg remains a future migration option if Trino/Snowflake interoperability becomes the primary constraint.

I considered **Apache Hudi** for mutation-heavy streams. I reject it because the core workload is append-heavy observability with short request-level retention, not low-latency record-level updates. Hudi's MOR model adds read complexity that is not needed for the first version.

### Decision 2 — Medallion layout: separate raw payload, cleaned records, and aggregates

I choose a **Bronze → Silver → Gold** medallion layout. Bronze is append-only and short-lived; Silver is parsed, redacted, schema-enforced, and request-level; Gold is small, query-optimized, and retained for one year.

I reject a **single wide table** containing raw payload, parsed fields, PII, and aggregates because it mixes security domains and retention policies. It would either keep sensitive prompt/response data too long or force premature deletion of useful aggregates.

I reject a **warehouse-only design** because 5 TB/day raw data would be expensive to retain and slow to load continuously. The warehouse or BI engine should query Gold, not own the raw system-of-record.

I reject a **log-only stack** such as Kafka plus Elasticsearch as the durable record because it is optimized for recent search, not one-year cost governance, schema evolution, or reproducible analytical datasets.

### Decision 3 — Ingestion: streaming append with micro-batches every 1–5 minutes

I choose **Kafka/Kinesis ingestion with Spark Structured Streaming micro-batches**. Events are partitioned by event time and tenant hash, then written into Bronze every 1–5 minutes. This matches the dashboard SLA while keeping file sizes large enough after compaction.

I reject **daily batch ingestion** because it cannot meet the 5-minute dashboard refresh requirement and would delay incident detection.

I reject **one file per request or per tiny batch** because it creates a small-file problem. The target is to write reasonably sized micro-batch files and run scheduled compaction. The lab result showed how small files hurt and how compaction/Z-order can restore performance.

I reject **direct synchronous writes from the API service to the lakehouse** because it couples API latency to storage availability. The API should emit to a durable stream, and the ingestion layer should absorb downstream backpressure.

### Decision 4 — Partitioning and clustering: date/hour partitions plus tenant clustering

I choose **partition by date and hour**, then **cluster/Z-order by tenant_id and model** in Silver and Gold. The common dashboard filter is tenant plus time window, so clustering by tenant_id makes file skipping effective while date/hour partitions bound the scan.

I reject **partitioning directly by tenant_id** because a large API platform can have thousands or millions of tenants. That would create high-cardinality partitions, tiny directories, and expensive metadata operations.

I reject **partitioning only by date** because 5 TB/day is still too much to scan for a 5-minute dashboard. Date-only layout would work for daily reports but not for low-latency tenant dashboards.

I reject **random/unclustered file layout** because query engines would scan too many files for tenant-specific dashboard panels. The hot path is predictable enough to justify clustering.

### Decision 5 — Retention and lifecycle: short raw retention, long aggregate retention

I choose **7-day retention for full prompt/response payloads** and **1-year retention for Gold aggregates**. Bronze and Silver request-level records are lifecycle-deleted after the incident-review window. Gold is compact, non-sensitive, and cheap enough to retain for the annual reporting window.

I reject **keeping full raw payload for one year** because 5 TB/day raw means 150 TB/month before compression and 1.8 PB/year. Even with compression, the storage and compliance risk are too high for a $5K/month cap.

I reject **deleting everything after 7 days** because finance, reliability, and tenant-success teams need long-term cost, latency, and error trends. The correct compromise is to retain aggregates, not raw prompts.

I reject **VACUUM 0 HOURS** as a cost shortcut. It destroys rollback and time-travel safety for concurrent readers. A minimum safe retention window is required, with lifecycle policies doing the long-term cleanup.

### Decision 6 — Security and governance: PII tokenization before analyst access

I choose **PII tokenization/redaction before Silver becomes readable**. Raw Bronze is accessible only to the ingestion service and a break-glass incident role. Silver contains redacted prompt/response fields and separate token references. Gold contains no prompt/response payload.

I reject **redacting only at dashboard time** because raw PII would still be broadly readable by analysts or accidental SQL queries. Security must be enforced before data reaches the general analytical layer.

I reject **dropping all prompt/response text immediately** because incident review needs short-term forensic access for model bugs, abuse investigation, and customer support escalations.

I reject **manual access review via tickets only** because 1B requests/day requires systematic governance. Every PII read must be logged to an audit table with user, purpose, query id, tenant, columns accessed, and timestamp.

### Decision 7 — Gold serving path: query optimized lakehouse tables, not raw logs

I choose to serve dashboards from **Gold 5-minute aggregate tables** with one row per tenant, model, and 5-minute window. These tables are compact, clustered by tenant_id, and designed for p50/p95 latency, token cost, error rate, and request volume.

I reject **dashboard queries directly on Bronze** because raw JSON parsing plus 5 TB/day scan volume would break the 5-minute SLA and expose sensitive payloads.

I reject **precomputing every possible dashboard dimension** because the cube would explode in size. Gold should contain the stable business dimensions: tenant, model, status, region, and time window. Rare exploratory analysis can query Silver with stricter controls.

I reject **vector database or search index as the observability system-of-record**. Search indexes can support incident text lookup, but the authoritative data should remain in the lakehouse where retention, lineage, access control, and time travel are manageable.

## 4. Failure modes

### Failure mode 1 — PII redaction bug leaks sensitive text into Silver

At 3:00 AM, a new prompt pattern bypasses the redaction function and phone numbers or emails appear in `llm_requests_clean`.

Detection:
- Data quality job samples Silver every micro-batch and runs PII detectors on `redacted_prompt` and `redacted_response`.
- Any match creates a Sev-1 alert and records the bad Delta version, batch id, tenant ids, and affected columns.
- PII audit table is checked to identify whether any user queried the contaminated version.

Rollback:
- Immediately revoke general analyst access to the affected Silver and Gold tables.
- Use Delta time travel to identify the last clean Silver version.
- Patch the tokenizer/redactor, replay Bronze data for the affected window, and overwrite or MERGE corrected Silver records.
- Recompute affected Gold windows from corrected Silver.
- VACUUM is not run until the incident is closed, so the team can still inspect versions safely.

Day 18 concept tied: schema enforcement, time travel, and controlled retention make the rollback auditable instead of destructive.

### Failure mode 2 — Schema evolution from the API breaks streaming ingestion

The API team deploys a new field or changes a type, for example `latency_ms` becomes a string in some events or token counters move under a nested object. Bronze can accept the raw envelope, but Silver parsing fails.

Detection:
- Structured Streaming checkpoint reports failed micro-batch id.
- Bronze continues to land raw events, so data is not lost.
- A schema-drift monitor compares observed JSON keys and types against the approved contract.

Rollback:
- Stop only the Bronze-to-Silver job, not the API or Bronze ingestion.
- Add a compatible schema evolution rule or quarantine malformed records into `silver_quarantine_bad_schema`.
- Replay the failed micro-batch from Bronze after the parser is fixed.
- If the bad deployment continues, the ingestion contract blocks the producer version and alerts the API owner.

Day 18 concept tied: Bronze append-only storage plus schema evolution keeps the lakehouse recoverable even when Silver validation fails.

### Failure mode 3 — Small files make tenant dashboard p95 latency miss the SLA

A traffic spike or bad micro-batch setting writes too many tiny files. Dashboard queries on Gold or Silver begin scanning thousands of files and p95 latency rises above the target.

Detection:
- Table health monitor tracks file count, median file size, and query p95 by table.
- Alert when average file size drops below target or query p95 exceeds the 5-minute dashboard SLA.
- Delta history is checked to identify which job version caused the file explosion.

Rollback:
- Temporarily route dashboards to the previous healthy Gold version if the latest version is slow or inconsistent.
- Run OPTIMIZE/compaction on affected partitions.
- Re-run Z-order or clustering by `tenant_id` and `model`.
- Change streaming trigger/file sizing config before restarting the writer.

Day 18 concept tied: the lab's small-file and OPTIMIZE exercise directly maps to this failure mode.

### Failure mode 4 — Incorrect cost formula corrupts Gold metrics

A pricing update changes cost per input/output token for a model, but the Gold aggregation job still uses old prices. Dashboards under-report spend for some tenants.

Detection:
- Daily reconciliation compares Gold cost totals against billing-system totals by tenant and model.
- Alert if variance exceeds 1 percent.
- Lineage metadata records which pricing table version was used by each Gold run.

Rollback:
- Fix the pricing dimension table.
- Time travel Gold to the last known correct version for dashboard fallback.
- Recompute affected 5-minute windows from Silver for the impacted date/model range.
- Publish a correction note with before/after totals for finance and tenant-success users.

Day 18 concept tied: time travel and lineage allow the team to know which aggregate version and pricing version produced the bad dashboard.

### Failure mode 5 — Lifecycle policy deletes request-level data too early

A misconfigured lifecycle rule deletes Bronze or Silver request-level records before the 7-day incident-review window is over.

Detection:
- Retention monitor checks minimum and maximum event dates per table daily.
- Object storage inventory is compared against Delta transaction logs.
- Incident-review queries fail fast if expected 7-day partitions are missing.

Rollback:
- Disable the bad lifecycle policy immediately.
- Restore from object-storage versioning or replication bucket if available.
- If only Gold remains, declare request-level forensic data unavailable for the missing interval and preserve all audit evidence.
- Add a lifecycle canary table and require policy changes to pass a dry-run before production.

Day 18 concept tied: Delta logs can show what files should exist, but storage lifecycle must be aligned with Delta retention and VACUUM rules.

### Failure mode 6 — Unauthorized or excessive PII access through break-glass role

An engineer uses the incident role too broadly, scanning more tenants or columns than needed.

Detection:
- Every read of Bronze raw payload or PII token table writes to `pii_access_audit`.
- Alert when a query touches more tenants, rows, or columns than the approved incident ticket allows.
- Weekly governance report joins audit logs with IAM role assumptions and ticket ids.

Rollback:
- Revoke the session and rotate temporary credentials.
- Freeze affected audit logs in an immutable bucket.
- Notify security and tenant success if policy requires.
- Add a row-level policy or query guardrail to limit future break-glass reads to approved tenant ids and time windows.

Day 18 concept tied: governance and auditability are part of the lakehouse design, not an afterthought.

## 5. Back-of-envelope cost estimate

This estimate is intentionally back-of-envelope. I use planning prices rather than exact vendor quotes:

| Item | Planning assumption |
|---|---:|
| Raw ingest volume | 1B requests/day × 5 KB = 5 TB/day raw |
| Compression ratio | 4:1 for JSON-like logs after Parquet/Delta compression |
| Compressed request-level data | 5 TB/day ÷ 4 = 1.25 TB/day |
| Full payload retention | 7 days |
| Gold aggregate retention | 1 year |
| Hot object storage | $23/TB-month |
| Infrequent-access object storage | $12.5/TB-month |
| Cold archive object storage | $4/TB-month |
| Spark compute | $0.50/vCPU-hour planning rate |

### Storage math

Bronze + Silver request-level retention:

- Compressed request-level data: `1.25 TB/day`.
- Keep full payload for 7 days: `1.25 TB/day × 7 = 8.75 TB`.
- Store hot because it is used for incident review and replay.
- Cost: `8.75 TB × $23/TB-month = $201.25/month`.

Silver may duplicate part of Bronze, but it stores redacted parsed fields rather than the full raw envelope. I budget another 50 percent overhead for Silver request-level records, Delta logs, checkpoints, quarantine tables, and temporary replay data:

- Overhead: `8.75 TB × 50% = 4.375 TB`.
- Cost: `4.375 TB × $23/TB-month = $100.63/month`.

Gold aggregate retention:

Assume Gold has one row per tenant, model, region, status, and 5-minute window. Even with 100K active tenants, 10 models, 4 regions, and 3 status groups, the row count is large but each row is tiny compared with raw payload. I budget **10 TB/year compressed** for Gold aggregates and derived reporting tables.

- Hot recent 30 days: `1 TB × $23 = $23/month`.
- Warm 11 months: `9 TB × $12.5 = $112.50/month`.
- Gold storage total: about `$135.50/month`.

Audit, lineage, and metadata tables:

- PII access audit, Delta logs, table health metrics, data quality results: budget `2 TB`.
- Cost: `2 TB × $23/TB-month = $46/month`.

Storage subtotal:

| Storage component | TB-month | Unit cost | Monthly cost |
|---|---:|---:|---:|
| 7-day Bronze full payload | 8.75 | $23 | $201.25 |
| 7-day Silver/checkpoint/quarantine overhead | 4.375 | $23 | $100.63 |
| Gold hot aggregates | 1 | $23 | $23.00 |
| Gold warm aggregates | 9 | $12.5 | $112.50 |
| Audit/lineage/metadata | 2 | $23 | $46.00 |
| **Storage subtotal** |  |  | **$483.38/month** |

This is comfortably below the $5K/month storage cap because the design does not retain full prompt/response payloads for a year. The dominant saving comes from the retention decision: full payload lives for 7 days, while long-term history is aggregate-only.

### Compute math

Ingestion and transformation compute:

- Bronze writer: 24 vCPU always on.
- Silver parser/redactor: 48 vCPU always on.
- Gold 5-minute aggregation: 16 vCPU always on.
- Total steady streaming capacity: `88 vCPU`.

Monthly compute:

- `88 vCPU × 24 hours/day × 30 days × $0.50/vCPU-hour`
- `= 88 × 720 × $0.50`
- `= $31,680/month`.

This compute estimate is much higher than storage. That is acceptable because the problem statement caps storage, not total platform cost. If total cost were capped at $5K/month, the design would need stronger sampling, cheaper spot/preemptible workers, serverless autoscaling, or a lower refresh SLA.

Optimization and maintenance compute:

- OPTIMIZE/Z-order on hot partitions: 64 vCPU for 2 hours/day.
- Monthly cost: `64 × 2 × 30 × $0.50 = $1,920/month`.
- Backfill/replay reserve: 128 vCPU for 10 hours/month.
- Monthly cost: `128 × 10 × $0.50 = $640/month`.

Compute subtotal:

| Compute component | Formula | Monthly cost |
|---|---:|---:|
| Streaming Bronze/Silver/Gold | 88 × 720 × $0.50 | $31,680 |
| Daily OPTIMIZE/Z-order | 64 × 2 × 30 × $0.50 | $1,920 |
| Backfill/replay reserve | 128 × 10 × $0.50 | $640 |
| **Compute subtotal** |  | **$34,240/month** |

### Why the storage cap is still plausible

The critical cap is **storage ≤ $5K/month**. This design is around **$500/month storage** under the assumptions above, leaving large safety margin for higher compression overhead, object-versioning, logs, and cloud-provider variance.

A bad alternative, retaining full compressed payload for 1 year, would be:

- `1.25 TB/day × 365 = 456.25 TB`.
- At hot object storage: `456.25 × $23 = $10,493.75/month`.

That breaks the $5K/month storage cap before adding Silver, Gold, audit logs, or versioning. This is why 7-day payload retention plus 1-year aggregate retention is the core FinOps decision.

## 6. One-week MVP slice

The one-week MVP should prove the riskiest parts of the architecture without trying to build the full 1B-request/day system.

### Goal

Build a small but realistic LLM observability lakehouse that demonstrates:

1. Streaming-like ingestion into Bronze.
2. PII tokenization/redaction before Silver is readable.
3. Silver schema enforcement and deduplication by `request_id`.
4. Gold 5-minute tenant/model metrics.
5. Time-travel rollback for a bad Silver or Gold version.
6. Basic audit logging for sensitive reads.

### Scope

Use synthetic data at **10–50 million rows/day equivalent**, not full 1B/day. The MVP can run on a small Spark cluster or local Docker stack. The important outcome is correctness of layout, governance boundaries, and recovery mechanics, not final production throughput.

### Day-by-day plan

| Day | Build slice | Success criteria |
|---|---|---|
| Day 1 | Generate synthetic LLM request logs with tenant_id, model, latency, tokens, status, prompt, response, and seeded PII | Bronze input has realistic schema, duplicate request_ids, and at least 7 days of timestamps |
| Day 2 | Write Bronze Delta table from micro-batches | Append-only Bronze table exists, partitioned by date/hour, with Delta history visible |
| Day 3 | Implement PII redaction/tokenization and Silver parser | Silver contains redacted prompt/response, parsed metrics, and no raw PII in analyst-readable columns |
| Day 4 | Add data quality checks and quarantine | Bad schema records are isolated; Silver job can continue or fail safely with clear batch id |
| Day 5 | Build Gold 5-minute metrics | Gold has tenant/model/window p50 latency, p95 latency, error rate, token totals, and cost_usd |
| Day 6 | Demonstrate failure recovery | Introduce a bad Silver write, use time travel to identify last clean version, fix and replay affected batch |
| Day 7 | Package dashboard query examples and design review notes | Example tenant dashboard query returns quickly from Gold; README documents tradeoffs and known gaps |

### MVP deliverables

- `bronze.raw_llm_requests_delta`
- `silver.llm_requests_clean`
- `silver.silver_quarantine_bad_schema`
- `gold.llm_tenant_5min_metrics`
- `governance.pii_access_audit`
- One notebook or script showing:
  - raw to Bronze
  - Bronze to Silver with redaction
  - Silver to Gold aggregation
  - time travel rollback
  - a sample audited PII access event

### Non-goals for week one

- No full 1B/day load test.
- No production IAM integration.
- No real customer PII.
- No multi-region disaster recovery.
- No complete BI dashboard.
- No exact cloud bill optimization.

The MVP is successful if a reviewer can see the end-to-end control flow: raw sensitive logs enter Bronze, analyst-safe records enter Silver, dashboards query Gold, and the team can detect and recover from a bad version.

## 7. Optional PoC

I include a small PoC at `submission/bonus/poc/pii_redaction_poc.py`.

The PoC demonstrates the most security-sensitive part of the design: **PII is tokenized/redacted before data becomes analyst-readable in Silver**. It uses deterministic HMAC tokens so the same email or phone number maps to the same stable token without exposing the original value.

What the PoC does:

1. Creates three synthetic LLM request events.
2. Seeds email and Vietnamese phone-number PII into prompt/response text.
3. Redacts analyst-readable text into placeholders such as `[EMAIL:email_tok_...]` and `[PHONE:phone_tok_...]`.
4. Writes a Silver-like CSV to `submission/bonus/poc/output/silver_redacted.csv`.
5. Writes a token audit CSV to `submission/bonus/poc/output/pii_token_audit.csv`.
6. Asserts that no raw email or phone number remains in Silver text.
7. Asserts deterministic tokenization for repeated email values.
8. Asserts that both phone-number formats are detected.

The successful run is captured in `submission/evidence/logs/15_bonus_poc.log` and ends with:

- `PII REDACTION POC PASS`
- `silver_rows=3`
- `audit_rows=4`

This is not a production redaction engine. It intentionally keeps the scope small. Its value is that it proves the architecture's governance boundary: raw prompt/response can enter Bronze, but Silver should only expose redacted text and token references.
