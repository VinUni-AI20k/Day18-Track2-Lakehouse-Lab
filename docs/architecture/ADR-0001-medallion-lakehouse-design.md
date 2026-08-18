# ADR-0001: Medallion Architecture and Table Format Selection

**Status:** Accepted  
**Deciders:** Pham Quoc Thanh (2A202601407), Architecture Committee  
**Date:** 2026-08-18  

---

## AI READING INSTRUCTION

Read `[SPEC]` blocks for architectural decisions and hard invariants.
Read `[NOTE]` blocks for background context and trade-offs.

---

## 1. Context and Problem Statement

**[NOTE]**
Modern AI systems require unified analytical and operational data infrastructure capable of handling:
1. High-throughput ingestion of unredacted LLM prompts, tool invocations, and agent traces (~1B req/day).
2. Strict ACID transactions to prevent partial write anomalies during concurrent reader executions.
3. Rapid file skipping for point-queries and analytics dashboards without paying prohibitive scan costs.
4. Robust schema evolution and compliance enforcement (e.g. EU AI Act Art. 10 data provenance).

Traditional Hive/HDFS table formats suffer from metadata bottlenecks, lack transactional safety, and require separate query-time partition filters.

---

## 2. Decision Drivers

**[SPEC]**
- **ACID Guarantees:** Multi-writer conflict resolution with serializable snapshot isolation.
- **File Pruning & Indexing:** Minimum 5×–10× scan reduction via file statistics and Z-ORDER clustering.
- **Catalog as Control Plane:** Centralized metadata management with schema evolution (Field-IDs).
- **ZeroJVM Execution Support:** Native Rust (`delta-rs`) and Python (`pyiceberg`) bindings to enable lightweight edge and local testing.
- **Cost Efficiency:** Parquet columnar compression with lifecycle-aware retention tiering.

---

## 3. Considered Options

**[SPEC]**

| Criterion | Delta Lake (delta-rs / Spark) | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| **ACID Metadata** | `_delta_log/*.json` single source of truth | 3-tier metadata tree (Catalog → Metadata → Manifest List → Manifest) | Timeline metadata in `.hoodie/` |
| **Schema Evolution** | Merge / Overwrite with type widening | Field-ID based (metadata-only rename/drop) | Complex key-based evolutions |
| **Partitioning** | Physical directory paths (`key=value`) | **Hidden Partitioning** (`day(ts)`, `bucket(N, col)`) | Directory-based / Virtual |
| **Local Python Support**| Excellent (`deltalake` Rust wheels) | Excellent (`pyiceberg` pure Python/Arrow) | Heavy JVM dependency |
| **Vector / Blobs** | Embedded arrays + decoupled storage | Embedded fixed-size lists | Parquet payload embedding |

---

## 4. Decision Outcome

**[SPEC]**
We adopt a **Hybrid Lakehouse Strategy**:
1. **Delta Lake** as the transactional engine for **Part A (Medallion Ingestion & Time Travel)**:
   - Utilized for high-speed streaming writes into `Bronze` and `Silver`.
   - Native `RESTORE`, `MERGE INTO`, and Change Data Feed (CDF) for SCD Type 2 tracking.
2. **Apache Iceberg** as the governance and catalog engine for **Part B (Control Plane & Provenance)**:
   - Utilized for schema evolution with immutable Field IDs.
   - Hidden partition pruning eliminates redundant query predicate boilerplate.
3. **DuckDB + Polars** as the zero-JVM in-process analytical query engine for vector searches and ad-hoc aggregations.

---

## 5. Consequences and Invariants

**[SPEC]**
- **Positive:**
  - 100% offline testability without JVM or Docker dependencies.
  - File pruning ratio exceeds 10× via Z-ORDER clustering on `(user_id, timestamp)`.
  - Zero schema drift risk: Column renames in Iceberg are strictly metadata operations without data rewrites.
- **Negative:**
  - Maintaining two table formats requires clear lifecycle boundaries (Delta for Bronze/Silver operational pipelines, Iceberg for governed enterprise catalogs).
