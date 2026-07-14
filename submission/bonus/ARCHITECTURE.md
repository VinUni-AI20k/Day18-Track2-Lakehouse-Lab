# Architecture Decision Record — Gaming Economy Fraud Lakehouse

**Author:** Tran Kien Truong
**Topic:** Custom — Idle/Clicker Game Economy Fraud Detection
**Date:** 2026-05-04
**Status:** Submitted

---

## 1. Problem Statement

An idle/clicker mobile game ingests **~50M events/day** — clicks, idle rewards, in-app purchases (IAP), and economy transactions — totaling **~25 GB/day raw**. A fraud ring exploits duping bugs in virtual currency accumulation, leaking an estimated **$200K/month** in virtual goods through receipt double-spend and idle reward amplification attacks.

The lakehouse must:
1. Land raw events with **player PII tokenized at Bronze** (GDPR + children's privacy compliance)
2. Detect duplication anomalies within **5 minutes** of occurrence
3. Retain raw events **90 days** for fraud investigation; aggregates thereafter for **2 years**
4. Enable **point-in-time replay** for lineage: "which checkpoint trained on which players"

**Scale:** 50M events/day × ~500 bytes/event ≈ 25 GB/day → 750 GB/month Bronze → ~70 GB/month Gold aggregates.

---

## 2. Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           INGESTION PATH                                     │
│                                                                              │
│  Game Server DB (Oracle/Postgres)                                           │
│         │                                                                    │
│         ▼                                                                    │
│  Debezium CDC ─────────────────────────────────────────────────────────     │
│         │                                      │                            │
│         ▼                                      ▼                            │
│  Kafka Topics                           Spark Structured Streaming          │
│  (player_events_raw)                     (tokenize + dedup)                │
│         │                                      │                            │
└─────────┼──────────────────────────────────────┼────────────────────────────┘
          │                                      │
          ▼                                      ▼
┌─────────────────────┐              ┌───────────────────────────────────────┐
│   BRONZE             │              │   SILVER                              │
│   _lakehouse/bronze/ │              │   _lakehouse/silver/                  │
│   player_events_raw  │              │   player_events_dedup                 │
│                      │   MERGE +    │   + player_dim_scd2                   │
│   Fields:            │   late-arr   │                                       │
│   - event_id (UUID)  │   handling   │   Fields:                             │
│   - player_id_enc    │◄─────────────│   - event_id                          │
│   - device_id_enc    │              │   - player_id_enc                     │
│   - session_id       │              │   - session_id                        │
│   - event_type       │              │   - event_type                        │
│   - currency_delta   │              │   - currency_delta                    │
│   - ts (event time)  │              │   - ts                                │
│   - receipt_hash     │              │   - is_duplicate (bool)                │
│   - game_version     │              │   - late_arrival_flag (bool)          │
│   - platform         │              │   - receipt_hash                      │
│   - salt_date        │              │   - game_version                      │
│                      │              │   - platform                          │
│   Partitioned by     │              │   - _hoodie_writes: event_id          │
│   date (event date)  │              │                                       │
│                      │              │   player_dim_scd2:                    │
│   Catalog: Polaris   │              │   - player_id_enc                     │
│   Format: Delta Lake │              │   - effective_from                     │
│   Retention: 90 days │              │   - effective_to                       │
│                      │              │   - country                           │
│                      │              │   - acquisition_channel                │
└─────────────────────┘              └───────────────────────────────────────┘
                                              │
                                              ▼
                              ┌───────────────────────────────┐
                              │   GOLD                        │
                              │   _lakehouse/gold/            │
                              │                               │
                              │   fraud_scores                │
                              │   - player_id_enc             │
                              │   - anomaly_score            │
                              │   - fraud_label              │
                              │   - first_seen_ts            │
                              │   - duping_alert_count       │
                              │                               │
                              │   economy_metrics_daily      │
                              │   - date                     │
                              │   - total_currency_earned    │
                              │   - total_currency_spent     │
                              │   - iap_revenue_usd          │
                              │   - duped_amount_detected    │
                              │   - by_platform              │
                              │   - by_game_version          │
                              │                               │
                              │   Catalog: Polaris           │
                              │   Format: Delta Lake         │
                              │   Partitioned: date          │
                              └───────────────────────────────┘
                                              │
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           QUERY PATH                                        │
│                                                                              │
│  Fraud Analyst ──► Unity / Polaris ──► Gold fraud_scores                   │
│  Ad-hoc query   Spark SQL        point-in-time via VERSION AS OF            │
│  p95 < 1s on Gold                                                          │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Catalog:** Apache Polaris (Unity Catalog API spec, vendor-neutral)
**Format:** Delta Lake (ACID commits, time travel, MERGE, deletion vectors)
**Ingestion:** Debezium CDC → Kafka → Spark Structured Streaming
**Storage:** S3 Standard (hot, 0-90d) → S3 IA (91-365d) → S3 Glacier (366d+)

---

## 3. Key Decisions with Rejected Alternatives

### Decision 1: Table Format — Delta Lake over Iceberg and Hudi

**Chosen: Delta Lake**

Delta provides the best combination of time travel, MERGE semantics, and ecosystem maturity for the fraud use case. The `RESTORE` capability is directly used in Failure Mode 1 rollback.

**Rejected — Apache Iceberg:** Iceberg has superior metadata pruning for very large tables (100M+ partitions) and better separate metadata from data files. However, MERGE support in Iceberg is less mature than Delta (Iceberg MERGE was added later and has lower performance in some benchmarks). For a team already on Databricks/Spark ecosystem, Delta's Hive Metastore integration is simpler.

**Rejected — Apache Hudi:** Hudi's upsert performance is excellent for write-heavy workloads, and Hudi's clustering is well-suited for late-arrival handling. However, Hudi's time travel semantics are less intuitive (require ` hoodie.datasource.query.type=incremental` vs Delta's `VERSION AS OF`), and the community ecosystem is smaller. For fraud replay use cases, Delta's `history()` and `describe history` are clearer for auditors.

---

### Decision 2: PII Tokenization — SHA-256 at Bronze Landing

**Chosen: SHA-256(player_id + daily_salt) encoded at Bronze before write**

Player PII (raw `player_id`, `device_id`) is hashed at ingestion so no unredacted PII ever touches the lake. The daily salt rotates to limit rainbow-table exposure. Salt is stored in AWS Secrets Manager, rotated quarterly.

**Rejected — Encrypted columns at Gold:** This approach would land raw PII in Bronze, violating GDPR Article 25 (data minimization by design). Any Bronze breach exposes unencrypted player identities.

**Rejected — No tokenization, rely on column-level security:** For a game with children in scope (COPPA compliance), having raw player IDs anywhere in the lake is unacceptable. Tokenization must happen at the earliest possible stage.

**Rejected — HMAC instead of SHA-256:** HMAC requires a secret key for every join operation, making cross-table identity resolution expensive and error-prone. SHA-256 with salt rotation achieves equivalent pseudonymization without key management overhead.

---

### Decision 3: Ingestion Path — Debezium CDC → Kafka → Spark Structured Streaming

**Chosen: CDC via Debezium on game server DB → Kafka → Spark Structured Streaming**

Debezium captures all changes (INSERT, UPDATE, DELETE) from the game server's primary DB, preserving event ordering and handling late-arriving writes naturally.

**Rejected — Direct DB polling (SELECT MAX(id) WHERE processed = false):** Polling requires a `processed` flag column, adding schema pollution and latency. Polling also misses DELETE events (player account deletion under GDPR).

**Rejected — Standalone Flink pipeline:** Flink is excellent for stateful stream processing but introduces a separate operational stack. A single Spark Structured Streaming job against Kafka fits the existing Spark/Scala codebase and shares cluster resources with Gold batch jobs.

---

### Decision 4: Deduplication Key — Server-generated event_id (UUID)

**Chosen: `event_id` UUID generated at game server, used as MERGE key**

Each event gets a UUID at server generation time, making it idempotent across retries and network re-transmits.

**Rejected — Composite natural key (ts, player_id, event_type):** Composite keys have collision risk in distributed systems. Two events with identical timestamps from different players could collide on player_id + ts in edge cases. Also brittle under schema evolution.

**Rejected — No deduplication:** The game client retries IAP receipt validation on network timeout. Without deduplication, retries create duplicate currency events. The 5-minute fraud detection SLA requires deterministic, not probabilistic, dedup.

---

### Decision 5: Late-Arrival Handling — `MERGE WHEN MATCHED AND src.ts > tgt.ts`

**Chosen: Upsert with event-time watermark; late arrivals flagged but not rejected**

For events arriving out of order (common in mobile with poor connectivity in remote provinces), the MERGE condition `WHEN MATCHED AND src.ts > tgt.ts` updates the record with the later timestamp, preserving the correct event ordering for fraud detection.

**Rejected — Reject late events:** Rejection creates false negatives in fraud detection (a legitimate player action arriving late would be silently dropped). For fraud forensics, accepting all events with correct ordering is more important than strict near-real-time.

**Rejected — Batch reprocessing with lookback window:** A 1-hour lookback window with batch reprocessing adds complexity and cost. The 5-minute SLA is achievable with streaming MERGE.

---

### Decision 6: Storage Tiering — S3 Standard → IA → Glacier

**Chosen: S3 Standard 0-90d → S3 IA 91-365d → S3 Glacier 366d+**

Bronze raw events must be queryable within 5 minutes for fraud investigation, requiring Standard tier. After 90 days, raw events are only needed for rare incident review; IA's 90-day minimum charged at $0.01/GB is acceptable. After 1 year, Glacier Deep Archive at $0.00099/GB handles the long tail.

**Rejected — All S3 Standard:** At 750 GB/month raw Bronze, Standard costs $17.25/mo. IA for the same 750 GB costs $7.50/mo after day 90. The savings compound significantly over 2-year retention.

**Rejected — All S3 IA:** IA incurs retrieval fees ($0.01-0.04/GB) on every fraud investigation query. Keeping hot data in Standard for 90 days avoids per-query costs for the active investigation window.

---

### Decision 7: Fraud Scoring — Batch (Spark MLlib) over Real-time Streaming

**Chosen: Gold batch job running every 5 minutes on Silver deduped data**

The 5-minute fraud detection SLA is achievable with batch scoring on the last 5 minutes of Silver data. A Spark job reading from Delta table state is deterministic and auditable.

**Rejected — Real-time streaming score (Flink/Structured Streaming with ML model):** Streaming ML scoring requires model serving infrastructure (TF Serving or equivalent), adds latency variance, and makes model versioning harder to audit. For a fraud team that needs to explain "how did we score this player at time T?", batch is more defensible.

**Rejected — Third-party SaaS (e.g., Fraud Detector):** SaaS solutions do not provide point-in-time replay capability. If a fraud analyst asks "what did our system think about this player on March 15?", a SaaS API cannot answer retroactively.

---

## 4. Failure Modes

### Failure Mode 1: False-Negative Dedup — Duplicate Events Reach Silver Without `is_duplicate=True`

**Scenario:** A bug in the Bronze deduplication logic (or a hash collision in `event_id`) causes two events with different `event_id` but identical `receipt_hash` to both be marked `is_duplicate=false`. The fraud ring exploits this by generating two events with different server-generated UUIDs but the same receipt_hash.

**Detection:** Daily reconciliation: `SELECT COUNT(*) FROM bronze WHERE receipt_hash IN (SELECT receipt_hash FROM bronze GROUP BY receipt_hash HAVING COUNT(*) > 1)` should match `SELECT COUNT(*) FROM silver WHERE is_duplicate=true`. A discrepancy triggers an alert.

**Detection (Day 18 tie-in):** Delta `history()` on the Silver table shows version history. If the bug was introduced in version 47, `RESTORE` to version 46 recovers correct dedup state while preserving the event_id UUIDs as the audit trail.

**Rollback:** `RESTORE TABLE silver.player_events_dedup VERSION AS OF 46`. After restore, a corrective MERGE re-runs dedup logic with the corrected algorithm.

---

### Failure Mode 2: Salt Rotation Breaks Player Identity Continuity

**Scenario:** Quarterly salt rotation means `SHA-256(player_id || old_salt)` != `SHA-256(player_id || new_salt)`. If a fraud investigation requires linking a player's historical events across the salt rotation boundary, the join fails.

**Detection:** Salt rotation date is stored as a column (`salt_date`) in Bronze. An alert fires if `DATEDIFF(CURRENT_DATE, MAX(salt_date)) > 90` (quarterly rotation missed).

**Detection (Day 18 tie-in):** Delta time travel via `VERSION AS OF` preserves table state at each salt epoch. The `player_dim_scd2` dimension table records which salt was active at `effective_from` time, enabling historical joins across rotations.

**Rollback:** The old salt is retained in Secrets Manager for 365 days post-rotation. For GDPR erasure requests, the old salt is used to re-identify and hard-delete the player's raw data before the new salt is applied.

---

### Failure Mode 3: Late-Arrival Events Reorder Fraud Signals After Gold Score Runs

**Scenario:** A player's currency duping event arrives 7 minutes after the Gold batch job completes. The fraud score for that 5-minute window does not include the event, creating a false negative — the player escapes a temporary account freeze.

**Detection:** A late-arrival watermark audit log tracks which 5-minute windows had late events arrive after the Gold job finished. If late events > threshold for a given window, the Gold job is re-triggered for that window only.

**Detection (Day 18 tie-in):** Delta `time travel` with `TIMESTAMP AS OF` allows the fraud analyst to query Silver at the exact timestamp the event arrived, not just the event timestamp. This is critical for post-incident review.

**Rollback:** No data rollback needed. The late-arriving event is incorporated in the next Gold run. If the fraud score for the player was already acted upon (e.g., account frozen), the fraud team is notified via PagerDuty to review the late-arrival case.

---

## 5. Cost Back-of-Envelope

### Storage

| Tier | Volume | Rate | Monthly Cost |
|------|--------|------|-------------|
| Bronze (S3 Standard, hot 0-90d) | 750 GB | $0.023/GB | $17.25 |
| Silver (S3 Standard) | 700 GB | $0.023/GB | $16.10 |
| Gold (S3 Standard) | 50 GB | $0.023/GB | $1.15 |
| Bronze → IA (91-365d, 700 GB aged out) | 700 GB | $0.01/GB | $7.00 |
| Bronze → Glacier (366d+, 600 GB aged out) | 600 GB | $0.00099/GB | $0.59 |
| **Storage Total** | | | **$42.09/mo** |

### Compute

| Job | Frequency | Duration | Instances | Cost |
|-----|-----------|----------|-----------|------|
| Spark Structured Streaming (ingest) | Continuous | 24h/day | 4× r5.xlarge | ~$280/mo (EC2 on-demand) |
| Gold fraud batch | Every 5 min | 2 min | 8× r5.xlarge | ~$40/mo |
| Silver dedup MERGE | Continuous (streaming) | N/A | Included above | $0 extra |
| **Compute Total** | | | | **~$320/mo** |

### Total

| Category | Monthly Cost |
|----------|-------------|
| Storage | $42.09 |
| Compute | $320.00 |
| **Total** | **~$362/mo** |

**ROI:** At $200K/month fraud leakage, a $362/mo lakehouse pays for itself if it catches one additional fraud ring per year. The expected value far exceeds the cost.

*Note: Compute costs assume EC2 on-demand. Reserved instances or EMR Serverless would reduce this by 40-60%.*

---

## 6. Week-1 MVP Slice

The smallest shippable cut that proves the architecture works:

### Scope

1. **Synthetic data generator** — 50K events/day with realistic event_type distribution and fraud pattern injection
2. **Bronze landing** — `tokenize_player()` function: SHA-256(player_id + rotating_salt) at write time
3. **Silver dedup** — `dedup_silver()` MERGE on event_id with late-arrival flag
4. **Gold fraud metric** — `duped_currency_alert` count per player, daily aggregate

### Not in MVP

- Full CDC pipeline (Kafka + Debezium) — replaced with batch synthetic writer for demo
- Player dimension SCD2 — replaced with flat player attributes
- ML-based fraud scoring — replaced with rule-based detection (receipt_hash collision)
- Storage tiering — all S3 Standard for MVP
- Polaris catalog — local filesystem Delta tables for demo

### Deliverable

`submission/bonus/poc/bronze_tokenize_dedup.py` — standalone ~120-line script that:
1. Generates 50K synthetic events with fraud pattern injection
2. Lands to Delta Bronze with tokenization
3. Runs Silver dedup MERGE with late-arrival handling
4. Computes and displays fraud alert counts

Run from clean checkout:
```bash
pip install deltalake duckdb polars pandas
python bronze_tokenize_dedup.py
```

Expected output:
```
Bronze events written: 50000
Silver after dedup:    49847  (153 duplicates removed)
Late-arrival events:   127
Fraud alerts triggered: 12
  - Receipt hash collisions: 8
  - Idle reward spam (< 1s interval): 4
```

### Day 18 Concepts Demonstrated

| Concept | Where Applied |
|---------|--------------|
| Medallion layout | Bronze → Silver → Gold分明 |
| ACID commits | Delta write atomicity |
| Time travel | `VERSION AS OF` in reconciliation query |
| MERGE semantics | `dedup_silver()` with `WHEN MATCHED AND src.ts > tgt.ts` |
| Schema enforcement | EventSchema dataclass prevents bad data |
| Deletion vectors | Delta log compaction (implicit in MERGE) |
| FinOps tiering | Documented in architecture; not in MVP |
| Lineage | `salt_date` and `event_id` audit trail |