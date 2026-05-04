# Bonus Architecture Brief: Privacy-First LLM Observability Lakehouse at 1B Requests/Day

Topic: A. LLM observability at 1B requests/day.

## 0. Why I Picked This Problem

The instructor example uses a decision matrix before jumping into architecture. I follow that pattern because the topic choice itself is an engineering decision.

| Candidate | AI-course fit | Synthetic data feasible | Shows medallion | Shows ACID/time travel | Privacy/FinOps tension | Total |
|---|---:|---:|---:|---:|---:|---:|
| LLM observability at 1B req/day | 5 | 5 | 5 | 5 | 5 | 25 |
| Decree-13 CDC ride-hailing | 3 | 3 | 4 | 5 | 5 | 20 |
| Multimodal legal RAG | 5 | 2 | 3 | 4 | 3 | 17 |
| Clickstream FinOps tiering | 2 | 4 | 4 | 3 | 5 | 18 |
| Catalog migration | 2 | 2 | 2 | 4 | 3 | 13 |

LLM observability wins because it directly extends NB4, but at production scale it forces harder trade-offs: cost versus retention, privacy versus debugging, and low-latency dashboards versus open lake storage.

## 1. Problem Statement

We operate a foundation-model API that logs 1B requests/day at roughly 5 KB/request, or 5 TB/day raw JSON before compression. The platform needs tenant-level cost, p50/p95/p99 latency, error-rate, and token-usage dashboards refreshed every 5 minutes. Full prompt/response bodies must be available for incident review for 7 days, but no human may read PII unless an incident ticket grants scoped access. After 7 days, raw prompts/responses are deleted; only aggregate metrics and privacy-safe exemplars are retained for 1 year. The hard constraint is storage spend <= $5K/month.

The difficult part is not just volume. It is the conflict between observability and privacy: engineers need enough detail to debug model incidents, but the lakehouse must make the unsafe path hard by default. The design below treats Bronze as a short-lived black-box recorder and Silver/Gold as the normal operating surface.

## 2. Architecture Diagram

```text
API gateways
  | 1B req/day logs, JSON, gzip over mTLS
  v
Kafka topics: llm.calls.v1, llm.redaction_deadletter.v1
  | Flink redaction job: schema check, PII detection, HMAC tokenization, envelope encryption
  v
+------------------------+     +-------------------------+     +---------------------------+
| Bronze Delta tables    | --> | Silver Delta tables     | --> | Gold Delta tables         |
| 7-day retention        |     | 30-day serving history  |     | 1-year aggregates         |
|                        |     |                         |     |                           |
| bronze_llm_raw         |     | silver_llm_calls        |     | gold_tenant_5m_metrics    |
| - encrypted prompt     |     | - request_id            |     | - tenant_id               |
| - encrypted response   |     | - tenant_id             |     | - model                   |
| - tokenized user_id    |     | - model                 |     | - window_start            |
| - pii_token_map_id     |     | - latency_ms            |     | - p50/p95/p99 latency     |
| - schema_version       |     | - input/output tokens   |     | - cost_usd                |
| - ingest_file_id       |     | - status                |     | - error_rate              |
| partition: ingest_hour |     | - pii_class_counts      |     | partition: date           |
| cluster: tenant_hash   |     | partition: date/hour    |     | cluster: tenant_id, model |
+------------------------+     | cluster: tenant_id      |     +---------------------------+
           |                   +-------------------------+           |
           | Delta time travel, restore, VACUUM 8 days                 |
           v                                                           v
PII vault table, KMS, break-glass workflow                     Dashboard API, Trino, DuckDB
  | scoped rehydrate by ticket_id                               Tenant SLO alerts, FinOps alerts
  v
Audit Delta table: every raw/PII read, lineage event, approver, expiry
```

### Layer Schemas

The slide example makes schemas explicit instead of hiding them inside prose. These are the contracts I would defend in review.

Bronze keeps raw encrypted payloads and replay coordinates. It receives everything, but plaintext is already unavailable to normal readers.

```sql
CREATE TABLE bronze.llm_raw (
  request_id          STRING NOT NULL,
  tenant_id_hash      STRING NOT NULL,
  ingest_ts           TIMESTAMP NOT NULL,
  ingest_date         DATE NOT NULL,
  ingest_hour         INT NOT NULL,
  source_topic        STRING NOT NULL,
  kafka_partition     INT NOT NULL,
  kafka_offset        BIGINT NOT NULL,
  schema_version      STRING NOT NULL,
  redaction_version   STRING NOT NULL,
  encrypted_prompt    BINARY,
  encrypted_response  BINARY,
  tokenized_user_id   STRING,
  pii_token_map_id    STRING,
  raw_headers_json    STRING,
  raw_usage_json      STRING
)
USING DELTA
PARTITIONED BY (ingest_date, ingest_hour);
```

Silver is the first normal analytics surface. It is typed, deduplicated, tenant-queryable, and carries lineage back to Bronze.

```sql
CREATE TABLE silver.llm_calls (
  request_id           STRING NOT NULL,
  tenant_id            STRING NOT NULL,
  user_token           STRING,
  request_ts           TIMESTAMP NOT NULL,
  date                 DATE NOT NULL,
  hour                 INT NOT NULL,
  provider             STRING NOT NULL,
  model                STRING NOT NULL,
  route                STRING,
  prompt_tokens        INT,
  completion_tokens    INT,
  latency_ms           INT,
  status               STRING,
  error_code           STRING,
  pii_class_counts     MAP<STRING, INT>,
  cost_usd             DECIMAL(12, 6),
  bronze_request_id    STRING,
  silver_run_id        STRING
)
USING DELTA
PARTITIONED BY (date, hour);
-- OPTIMIZE silver.llm_calls ZORDER BY (tenant_id, model)
```

Gold tables are intentionally narrow because they serve dashboards and budget automation, not exploratory raw debugging.

```sql
CREATE TABLE gold.tenant_5m_metrics (
  window_start       TIMESTAMP NOT NULL,
  date               DATE NOT NULL,
  tenant_id          STRING NOT NULL,
  model              STRING NOT NULL,
  request_count      BIGINT,
  p50_latency_ms     DOUBLE,
  p95_latency_ms     DOUBLE,
  p99_latency_ms     DOUBLE,
  error_rate         DOUBLE,
  total_tokens       BIGINT,
  cost_usd           DECIMAL(14, 6),
  redaction_version  STRING,
  source_table_ver   BIGINT
)
USING DELTA
PARTITIONED BY (date);
-- OPTIMIZE gold.tenant_5m_metrics ZORDER BY (tenant_id, model)
```

## 3. Key Decisions and Rejected Alternatives

### Decision 1: Table Format

I chose Delta Lake for Bronze, Silver, and Gold. Delta gives ACID transactions, schema enforcement/evolution, time travel, MERGE, OPTIMIZE/Z-order, and broad Spark/Trino/DuckDB support. These are not convenience features here; they are incident controls. If a redaction bug writes unsafe Silver data at 03:00, RESTORE gives a concrete rollback plan.

I rejected raw Parquet folders because they make bad writes hard to isolate. A malformed partition can be listed, read, and cached before the team notices. I rejected log-store-only designs such as Kafka retention plus ClickHouse because they make 7-day prompt review fast, but they do not give cheap 1-year aggregate retention and auditable table history in the same storage layer.

### Decision 2: Medallion Boundary

I chose a strict Bronze -> Silver -> Gold contract. Bronze is the only layer that can contain encrypted full prompts/responses, and it expires after 7 days. Silver is typed, deduplicated, tokenized, and queryable by engineers. Gold is the only source for dashboards and budget alerts.

I rejected "dashboard reads Bronze directly" because retries and partial JSON would inflate cost and error metrics. I rejected "write only aggregates" because incident response needs a short-lived prompt/response recorder when a customer reports a bad generation.

### Decision 3: PII Handling

I chose tokenization at the Bronze landing edge: the Flink job detects obvious PII classes, writes HMAC tokens into Bronze/Silver, encrypts full prompt/response with a per-day KMS data key, and records `pii_token_map_id`. Normal readers never see plaintext. Break-glass rehydration requires `ticket_id`, `approver`, `tenant_id`, and expiry; every read appends to an audit Delta table.

I rejected "redact later in Silver" because it leaves a window where raw objects can be read or copied. I rejected irreversible redaction only because incident review sometimes needs exact text to reproduce prompt injection, jailbreak, or model regression reports.

### Decision 4: Partitioning and Clustering

I chose time partitions plus tenant clustering. Bronze is partitioned by `ingest_date` and `ingest_hour` because retention and replay are time-based. Silver is partitioned by `date/hour` and clustered/Z-ordered by `tenant_id, model`. Gold is partitioned by `date` and clustered by `tenant_id, model`.

I rejected partitioning by tenant because large tenants would create skewed partitions while small tenants would create tiny files. I rejected partitioning only by date because the hot query path is "show tenant X over the last N windows", so engines would scan too many files without tenant-aware clustering.

### Decision 5: File Size, Compression, and Maintenance

I chose Parquet with ZSTD compression, target file size 256 MB in Silver/Gold, and micro-batch compaction every 15 minutes for the last 2 hours. Bronze lands faster with smaller files, then compaction catches up. Gold is rewritten every 5 minutes per active window, then compacted hourly.

I rejected immediate large-file writes at ingestion because the redaction job should not wait to fill 256 MB when dashboard freshness is 5 minutes. I rejected daily-only OPTIMIZE because dashboard latency would degrade during peak traffic when small files accumulate fastest.

### Decision 6: Catalog and Governance

I chose Unity Catalog if the platform is Databricks-first, with an abstraction target of open REST catalog registration later. Tables are registered with owners, data classification, row filters by tenant, and column masking policies. OpenLineage events are emitted from Flink and Spark jobs into a lineage service.

I rejected "paths in code" because access control and lineage become tribal knowledge. I rejected a full Iceberg/Polaris migration on day one because the immediate risk is privacy and freshness, not vendor-neutrality; catalog migration can be a planned phase once contracts are stable.

### Decision 7: Lifecycle and Retention

I chose explicit lifecycle tiers. Bronze raw/encrypted payloads: S3 Standard equivalent for 7 days, then hard delete plus Delta VACUUM after legal hold checks. Silver typed calls: 30 days in Standard or Intelligent-Tiering. Gold aggregates: 1 year in Standard-IA or equivalent, with compaction to large files before transition.

I rejected keeping raw prompts for 30 or 90 days because it violates the privacy-minimization goal and pushes blast radius up. I rejected Glacier for Gold because dashboards and quarterly reviews need millisecond-to-second access, not restore workflows.

## 3.1 Production Ops Decisions

This is the operational checklist version of the design, modeled after the instructor's example slide.

| Area | Decision | Why it matters |
|---|---|---|
| Compaction | OPTIMIZE Silver every 15 minutes for the last 2 hours; full-table OPTIMIZE nightly; Z-order weekly if tenant filter pruning drops below 10x. | Keeps dashboard file scans bounded while avoiding constant rewrite cost. |
| Data contracts | Producer SDK must send `schema_version`; incompatible changes dual-write v1/v2 for 14 days. | Prevents silent corruption when a model provider changes usage JSON. |
| Quality gates | Bronze accepts; Silver validates required fields; Gold checks p95>=p50, 0<=error_rate<=1, and cost_usd>=0. | Mirrors NB4 but adds explicit production guards. |
| Lineage | Every Flink/Spark/dbt run emits OpenLineage; Silver rows carry `bronze_request_id` and `silver_run_id`. | Lets on-call trace a bad Gold number to table version and ingestion offset. |
| Alerting | Freshness >7 min, Silver/Bronze dedup drop >8%, Gold cost >1.5x baseline, p95 latency >5s for 3 windows. | Alerts are tied to business meaning, not only infrastructure metrics. |
| Privacy | Plaintext rehydrate requires incident ticket, approver, tenant scope, and expiry; every attempt writes audit Delta. | Makes privacy a data product behavior, not a wiki policy. |
| Lifecycle | Bronze VACUUM after 8 days; Silver 30 days; Gold 365 days; legal hold table can pause deletion for specific tenant/date. | Keeps storage under budget while still supporting incident review. |

## 3.2 Trade-Offs I Intentionally Accept

| I choose | I reject | Why |
|---|---|---|
| 5-minute micro-batch for Gold | Per-row true streaming dashboards | 5-minute SLA does not justify higher operational complexity for every aggregate. |
| Delta Lake | Iceberg hidden partitioning | Delta's RESTORE/MERGE path is already proven in the lab and is the core incident-control requirement. |
| Tokenize at landing | Encrypt-at-rest only | Encryption protects disks; tokenization protects analysts from accidental plaintext access. |
| Time + tenant clustering | Tenant-only partitioning | Tenant-only partitions skew badly for top tenants and create tiny files for long-tail tenants. |
| Full Gold window rebuild for last 30 minutes | Fine-grained incremental updates per event | Rebuild is idempotent and easier to reason about; cost is small because Gold windows are narrow. |
| One catalog first | Multi-catalog from day one | Catalog portability matters, but premature dual governance is a bigger delivery risk than future migration. |

## 4. Failure Modes

### 1. Redaction classifier misses a new PII pattern

Detection: Silver quality job tracks `pii_class_counts`, samples encrypted Bronze through a privileged canary, and alerts when unknown entity rate or manual audit hits exceed threshold. A schema contract also requires every raw row to carry `redaction_version`.

Rollback: Block Silver publication for affected `ingest_hour`, patch the detector, replay Bronze for that hour into a new Silver version, then RESTORE Gold windows computed from unsafe Silver. The old unsafe Silver version remains in table history but is access-blocked at the catalog policy layer until VACUUM.

Day 18 tie-in: time travel and RESTORE make rollback auditable instead of manual delete-and-rewrite.

### 2. Bad schema release from SDK vNext

Detection: Bronze schema enforcement accepts only allowed top-level fields; new schema versions are routed to a quarantine Delta table if required fields are missing or types change. A 5-minute dashboard checks quarantine rate by SDK version.

Rollback: Pin SDK at gateway config, replay quarantine after adding an explicit schema evolution rule, and MERGE corrected rows into Silver using `request_id` as idempotency key.

### 3. Small-file storm during traffic spike

Detection: Delta history and object-store inventory expose `numFiles` and average file size per table/hour. Alert if Silver has >10,000 files/hour or average file size <32 MB after compaction SLA.

Rollback: Temporarily widen micro-batch interval from 1 minute to 5 minutes, run targeted OPTIMIZE on last 2 hours, and route dashboard API to a stale-but-safe Gold version until compaction catches up.

### 4. Tenant isolation bug in dashboard API

Detection: Every dashboard query emits tenant_id, principal, query hash, and table version to the audit table. A policy check compares principal tenant scope against query predicate. Missing tenant predicate is a hard failure.

Rollback: Disable the dashboard service account, RESTORE any derived cache table from before the bad release, and use audit lineage to list all potentially exposed tenants.

### 5. Cost runaway from raw retention or rehydrate abuse

Detection: Daily FinOps job computes projected month-end storage and retrieval by layer. Break-glass rehydration has per-tenant and per-approver quotas.

Rollback: Enforce lifecycle immediately on Bronze versions older than 7 days, revoke rehydrate role, and move non-dashboard Gold history to IA only after compaction confirms object size >128 KB.

## 5. Back-of-Envelope Cost

Pricing assumptions: use public S3-like object storage list prices as planning numbers, checked against AWS S3 pricing and storage-class docs. Actual cloud contract can differ, but the design target is robust because the estimate is far below the $5K/month cap.

Ingest volume:

- Raw input: 1B req/day * 5 KB = 5 TB/day raw JSON.
- Bronze encrypted Parquet/ZSTD: assume 3.5x compression = 1.43 TB/day.
- 7-day Bronze steady state: 1.43 TB/day * 7 = 10.0 TB.
- Silver typed records without full prompt/response: assume 0.45 KB/request compressed = 0.45 TB/day.
- 30-day Silver steady state: 13.5 TB.
- Gold 5-minute tenant/model aggregates: assume 20K tenants * 20 models * 288 windows/day * 1 KB compressed = 115 GB/day before compaction; with sparse activity and Parquet compression assume 20 GB/day = 7.3 TB/year.
- PII vault tokens: assume 0.2 TB/day compressed, 7-day retention = 1.4 TB.

Storage:

| Layer | Steady TB | Tier assumption | Rate | Monthly |
|---|---:|---|---:|---:|
| Bronze raw encrypted | 10.0 | Standard | $23/TB-mo | $230 |
| PII vault | 1.4 | Standard | $23/TB-mo | $32 |
| Silver calls | 13.5 | Standard | $23/TB-mo | $311 |
| Gold aggregates | 7.3 | Standard-IA | $12.5/TB-mo | $91 |
| Delta logs/checkpoints/slack | 5.0 | Standard | $23/TB-mo | $115 |
| Safety factor 3x | - | requests, metadata, over-retention | - | $2,337 |
| Estimated storage total | - | - | - | $3,116/mo |

This leaves roughly $1.9K/month before the $5K cap for lifecycle transition requests, object metadata, and occasional retrieval. The main FinOps risk is not bytes; it is object count and accidental retention. Therefore the design requires compaction before IA transition and daily checks that Bronze has no data older than 8 days.

Compute:

- Flink redaction: 24 workers * $0.25/hour * 730 = $4,380/month.
- Spark/Delta maintenance: 8 workers * $0.35/hour * 2 hours/day * 30 = $168/month.
- Gold aggregation: 12 workers * $0.35/hour * 24 * 30 = $3,024/month.
- Query API/Trino pool: shared platform cost, budget guardrail $2,000/month.

Compute is outside the stated storage cap but still monitored. If compute must also fit $5K/month, we reduce Gold granularity to tenant/model/hour for inactive tenants and keep 5-minute windows only for top 1,000 tenants.

## 6. One-Week MVP Slice

The first shippable slice should prove the riskiest path: privacy-safe observability under realistic write shape.

Day 1: Implement a synthetic generator at 10M rows/day scale shape: request_id, tenant_id, model, token counts, latency, status, prompt containing seeded PII, and duplicate retries.

Day 2: Build the Bronze landing job with deterministic HMAC tokenization, encrypted raw prompt placeholder, schema_version, and a dead-letter Delta table.

Day 3: Build Silver parse/validate/dedup using `request_id`, with partitioning by date/hour and Z-order by tenant_id.

Day 4: Build Gold 5-minute aggregates for p50/p95/p99, cost_usd, error_rate, and top error codes.

Day 5: Add two failure drills: bad schema quarantine and redaction version rollback using time travel/RESTORE.

Day 6: Add audit table for every rehydrate attempt and a small break-glass CLI that requires ticket_id.

Day 7: Run a report: freshness p95, Silver < Bronze due to dedup, query pruning by tenant, storage estimate by layer, and a screenshot-ready dashboard table.

The MVP is not the full platform. It proves the three claims that matter: the privacy boundary works, the medallion pipeline produces correct metrics, and the lakehouse can rollback a bad batch without hiding the audit trail.

## 7. Demo Numbers I Would Show

The example deck uses concrete result numbers. These are the acceptance numbers I would put on the final slide after the MVP.

| Metric | Target | How to prove |
|---|---:|---|
| Bronze ingest | 10M synthetic rows/day shape | Generator writes partitioned Bronze with `_delta_log` visible. |
| Silver dedup | Silver rows < Bronze rows by 3-7% retry rate | Count distinct `request_id`, like NB4. |
| Tenant query pruning | >=10x file-pruned ratio | Delta file stats before/after Z-order, like NB2. |
| Gold freshness | p95 < 7 minutes from event timestamp | Compare `max(window_start)` to wall-clock. |
| Restore drill | <30 seconds for one bad Gold window | RESTORE target table version and verify bad rows gone, like NB3. |
| Privacy control | 0 plaintext reads without ticket | PoC audit table shows blocked and allowed rehydrate attempts. |

Example CEO query:

```sql
SELECT tenant_id, model, SUM(cost_usd) AS cost_24h,
       approx_percentile(p95_latency_ms, 0.95) AS p95_of_p95,
       AVG(error_rate) AS avg_error_rate
FROM gold.tenant_5m_metrics
WHERE window_start >= current_timestamp() - INTERVAL 24 HOURS
GROUP BY tenant_id, model
ORDER BY cost_24h DESC
LIMIT 10;
```

This is the kind of query the architecture is optimized for: narrow Gold table, tenant/model clustering, and no plaintext exposure.

## Optional PoC

See `submission/bonus/poc/privacy_tokenization_spike.py`. It demonstrates the non-trivial mechanism in this design: deterministic tokenization before Silver, scoped rehydration, and audit logging for every plaintext access.

## Source Notes

- AWS S3 pricing page: https://aws.amazon.com/s3/pricing/
- AWS S3 storage-class guide: https://docs.aws.amazon.com/AmazonS3/latest/userguide/storage-class-intro.html
