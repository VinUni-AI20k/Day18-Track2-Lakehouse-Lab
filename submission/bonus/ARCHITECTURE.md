# Real-Time Data Lakehouse for Natural Language Robot Control and Telemetry at Scale

## 1. Problem Statement
We design a real-time data storage and analytics system for a fleet of **10,000 active robots** controlled using natural language. The system must ingest two distinct high-velocity streams:
1. **Robot Telemetry**: 10 Hz state updates (joint angles, battery, GPS, sensor anomalies). $10,000 \text{ robots} \times 10 \text{ Hz} = 100,000 \text{ messages/sec}$. At 200 bytes/message, this generates **1.72 TB/day** (raw JSON).
2. **LLM Interaction Logs**: Commands, prompt templates, LLM responses, and generated joint trajectory plans. ~1M commands/day. At 10 KB/log, this generates **10 GB/day**.

### Constraints & Challenges
* **Real-time SLA**: Analytics dashboards must monitor robot command execution latency and error rates with a refresh rate of **< 1 minute**.
* **PII Governance**: User voice transcripts and GPS coordinates must be tokenized or redacted before general analysts can query them.
* **FinOps Storage Budget**: Total storage + compute cost must remain below **$2,000/month**.

---

## 2. Architecture Diagram

```text
                                 [ 10,000 ROBOT FLEET ]
                                            │
                                            ▼
                        ┌───────────────────────────────────────┐
                        │ AWS IoT Core / Managed Kafka (MSK)    │
                        └───────────────────┬───────────────────┘
                                            │
                     ┌──────────────────────┴──────────────────────┐
                     ▼ (Robot Telemetry)                           ▼ (LLM Logs)
          ┌─────────────────────┐                       ┌─────────────────────┐
          │ Vector Agent (Rust) │                       │ Vector Agent (Rust) │
          └──────────┬──────────┘                       └──────────┬──────────┘
                     │ append-only                                 │ append-only
                     ▼                                             ▼
       ┌───────────────────────────┐                 ┌───────────────────────────┐
       │   bronze.robot_telemetry  │                 │   bronze.llm_interactions │
       │ (S3 Delta Table: hour-pt) │                 │ (S3 Delta Table: hour-pt) │
       └─────────────┬─────────────┘                 └─────────────┬─────────────┘
                     │                                             │
                     └──────────────────────┬──────────────────────┘
                                            │ Streaming Read (delta-rs)
                                            ▼
                               ┌──────────────────────────┐
                               │   Silver Pipeline Job    │ ◄── Tokenizer API
                               │   (Python/DuckDB Worker) │
                               └────────────┬─────────────┘
                                            │
                                            ▼
                               ┌──────────────────────────┐
                               │   silver.robot_activity  │
                               │  - PII Redacted          │
                               │  - Z-Ordered by robot_id │
                               │  - Partitioned by date   │
                               └────────────┬─────────────┘
                                            │
                                            ▼
                               ┌──────────────────────────┐
                               │    Gold Aggregate Job    │
                               │ (Hourly DuckDB Material) │
                               └────────────┬─────────────┘
                                            │
                                            ▼
                               ┌──────────────────────────┐
                               │    gold.daily_metrics    │
                               │  - Success rates & P95   │
                               │  - Z-Ordered by model    │
                               └────────────┬─────────────┘
                                            │
                                            ▼
                                [ BI / Analyst Queries ]
                        (DuckDB/Trino zero-copy Delta scan)
```

---

## 3. Key Architectural Decisions (with Rejected Alternatives)

### 3.1. Table Format: Delta Lake vs. Apache Iceberg vs. Apache Hudi
* **Decision**: **Delta Lake**.
* **Rejected Alternative 1 (Apache Iceberg)**: Iceberg is vendor-neutral, but lacks a mature, lightweight edge-writer ecosystem. Delta Lake's `delta-rs` bindings allow low-memory, zero-JVM Rust/Python clients to write directly from edge relays.
* **Rejected Alternative 2 (Apache Hudi)**: Hudi's Copy-on-Write and Merge-on-Read strategies are highly optimized for primary-key updates (CDC), but introduce unnecessary write-amplification and configuration overhead for our append-mostly telemetry streams.

### 3.2. Ingestion Path: Rust Vector Agents vs. Spark Streaming
* **Decision**: **Rust Vector Agents writing directly to S3 via delta-rs**.
* **Rejected Alternative (Spark Streaming)**: Running a 24/7 Spark cluster on AWS EMR to ingest telemetry consumes minimum $1,500/month in EC2 instances alone, eating up 75% of our budget. Vector uses < 100 MB of RAM, handles backpressure out-of-the-box, and writes directly to Delta Tables at zero compute idle cost.

### 3.3. Partitioning Strategy: Partition by Date + Z-Order by Robot ID
* **Decision**: **Partition Silver by `date`, Z-Order by `robot_id`**.
* **Rejected Alternative 1 (Partition by Robot ID)**: Partitioning by 10,000 `robot_id`s creates 10,000 subdirectories daily. With hourly telemetry flushes, we would generate $10,000 \times 24 = 240,000$ tiny files per day, hitting the **Small-file problem** and destroying read performance.
* **Rejected Alternative 2 (Partition by Date + Hour)**: Although files are larger, point queries for a specific robot still require scanning all files in the hourly folder. Z-Ordering by `robot_id` co-locates a single robot's telemetry into 1-2 files per day, enabling 90% file-skipping.

### 3.4. PII Redaction: Pre-ingestion Tokenization vs. Column-Level Catalog Policies
* **Decision**: **Tokenize PII at the Silver Layer using a stateless microservice**.
* **Rejected Alternative (Column-level catalog masking)**: Implementing policies in Databricks Unity Catalog or AWS Lake Formation locks us into proprietary enterprise licenses and requires running active metadata catalogs, exceeding the budget. Redacting at the Silver layer makes security portable across any engine (DuckDB, Trino, Polars).

### 3.5. Storage Tiering: S3 Intelligent-Tiering with Glacier Transition
* **Decision**: **S3 Intelligent-Tiering + S3 Lifecycle Rules**.
* **Rejected Alternative (S3 Standard Only)**: Storing 1.8 TB/day raw in S3 Standard costs $1.8 \times 30 \times \$0.023 = \$1,242/\text{month}$ for the first month, compounding to over $14,000/year. By transitioning Bronze raw data to Glacier Deep Archive after 14 days ($0.00099/GB), we reduce cold storage costs by 95%.

---

## 4. Failure Modes & Rollback Plans

### 4.1. 3 AM Failure Scenario: Schema Drift in LLM Command Parsing
* **Symptom**: The LLM team deploys a new model that changes the structure of `generated_trajectory` (e.g., nesting joint coordinates). The Silver pipeline fails parsing, dropping telemetry records or crashing.
* **Detection**: Alert triggers when the Silver write error rate exceeds 1% in CloudWatch, or Jaccard schema similarity between Bronze and Silver falls.
* **Rollback Plan**: 
  1. Halt the Silver streaming job.
  2. Use **Delta Time Travel** to query the exact schema of Silver prior to the incident: `DeltaTable(silver_path, version=current_version - 1)`.
  3. Deploy a fallback parser version to ignore the new nested fields.
  4. Restart the Silver job starting from the last valid Bronze transaction version using Delta log sequence numbers.

### 4.2. Telemetry Ingestion Burst (DDoS/Network Reconnection)
* **Symptom**: A cellular outage in a robot region resolves, causing 2,000 robots to reconnect and dump 6 hours of buffered telemetry at once. S3 API limits (3,500 PUTs/sec) rate-limit the writers, causing data loss.
* **Detection**: Vector metrics show `s3_put_throttled_total` increasing.
* **Rollback Plan**: 
  1. Ingestion nodes write telemetry to local disk buffer (Vector disk-buffer enabled).
  2. Scale the ingestion buffers' target file-write size from 5 MB to 50 MB dynamically to reduce PUT call frequency.

### 4.3. Corrupt Deletion Vectors on Compaction
* **Symptom**: During nightly compaction (`OPTIMIZE`), a node crashes, leaving dangling Deletion Vectors that cause queries to skip valid records or read duplicate files.
* **Detection**: Pytest-based data validation queries run at 4 AM and find row count mismatches between Silver and Bronze.
* **Rollback Plan**: Run a Delta **`RESTORE`** command to roll back the Silver table to the pre-compaction version (takes < 10 seconds).

---

## 5. Back-of-the-Envelope Cost Estimation

### 5.1. Storage Cost (S3)
* **Bronze Raw Data (1.8 TB/day)**:
  * 14 days in S3 Standard: $1.8 \text{ TB/day} \times 14 \times \$23/\text{TB-month} \approx \$580/\text{month}$.
  * Rest of the month (16 days) in Glacier Deep Archive: $1.8 \text{ TB/day} \times 16 \times \$0.99/\text{TB-month} \approx \$28/\text{month}$.
  * Bronze Storage: **~$608/month**.
* **Silver & Gold Cleaned Data (compresses 5x using Parquet, ~360 GB/day)**:
  * 90 days in S3 Standard: $0.36 \text{ TB/day} \times 90 \times \$23/\text{TB-month} \approx \$745/\text{month}$.
* **Total Storage Cost**: **~$1,353/month**.

### 5.2. Compute Cost (AWS Fargate Serverless Workers)
* 4 vCPU, 16 GB RAM ECS Fargate containers running the DuckDB Silver parsing logic continuously:
  * 2 instances $\times$ \$0.18/hour $\approx \$260/\text{month}$.
* Nightly compaction & optimization cron job:
  * 1 hour/day EMR Serverless $\approx \$50/\text{month}$.
* **Total Compute Cost**: **~$310/month**.

### 5.3. Total Cost
* **Total Monthly Budget Spend**: **$1,663/month** (under the $2,000 cap).

---

## 6. One-Week MVP Slice

To validate this architecture, we will build a 1-week MVP containing:
1. **Mock Telemetry & LLM Log Generator**: A python script simulating 10 robots sending JSON payloads.
2. **Local Bronze Store**: Delta tables on disk acting as the Bronze landing zone.
3. **Silver Pipeline PoC**: A lightweight python script utilizing `deltalake` and `DuckDB` to parse, tokenize (PII redaction), and append to the Silver table.
4. **Validation Test**: Measure Z-Order search speedup on `robot_id` query patterns.
