# Day 18 Lakehouse Architecture & Documentation
**Version 1.0.0** · Pham Quoc Thanh (2A202601407) · 2026-08-18 · AICB-P2T2

---

## AI READING INSTRUCTION

Read `[SPEC]` and `[BUG]` blocks for authoritative system facts, schema contracts, and invariants.
Read `[NOTE]` blocks for contextual narrative, trade-off rationale, and design motivation.
`[?]` blocks are unverified hypotheses.

---

## 1. System Overview

**[SPEC]**
- **Repository:** `Day18-Track2-Lakehouse-Lab` (AICB-P2T2 Day 18 Data Lakehouse Architecture).
- **Core Stacks:**
  - **Lightweight Path:** `deltalake` (Rust delta-rs 1.6+), `pyiceberg` (0.11+), `duckdb` (1.5+), `polars` (1.43+), `pyarrow`.
  - **Production Path:** Apache Spark 3.5.0, Delta Spark 3.2.0, MinIO S3 Object Storage via `docker-compose.yml`.
- **Medallion Layers:**
  - `_lakehouse/bronze/`: Append-only raw JSON logs (`llm_calls_raw`, `agent_traces`, `docs_multimodal`).
  - `_lakehouse/silver/`: Cleaned, validated, deduplicated Parquet (`llm_calls`, `training_corpus_governed`, `agent_trajectories`).
  - `_lakehouse/gold/`: 5-minute & daily aggregated metrics (`llm_daily_metrics`, `agent_performance`).

**[NOTE]**
This lab implements production Lakehouse patterns spanning Delta ACID transaction logs, Z-ORDER multidimensional clustering, Apache Iceberg REST catalogs with hidden partitioning, 4 mandatory storage maintenance jobs, multimodal tensor inlining vs. external storage, and EU AI Act Article 10 training provenance.

---

## 2. Documentation Sitemap

**[SPEC]**

| Document | Category | Scope |
|---|---|---|
| [ADR-0001: Lakehouse Architecture](architecture/ADR-0001-medallion-lakehouse-design.md) | Architecture | Core storage engine and format selection |
| [Storage Layout & Schema Contracts](design/storage-layout-and-schemas.md) | Design | Table schemas, partitioning, and indexing specifications |
| [Production Maintenance Runbook](guide/production-maintenance-runbook.md) | Guide | Compaction, Z-ORDER, orphan sweeping, and snapshot expiry |
| [Rubric Verification Matrix](planning/rubric-verification-matrix.md) | Planning | 100-point rubric breakdown and automated test gates |

---

## 3. Quick Verification Commands

**[SPEC]**
```bash
# 1. Environment Smoke Test (9/9 checks)
uv run python scripts/verify_lite.py

# 2. Automated Test Suite (24/24 invariant checks)
uv run pytest -q

# 3. Headless Notebook Execution Gate (8/8 passes)
uv run python scripts/run_all.py

# 4. Bonus Challenge Simulator (Topic A)
uv run python submission/bonus/simulate_bonus_a.py
```
