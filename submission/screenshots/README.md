# Lakehouse Storage Layout & Transaction Log Verification

This directory contains storage layout evidence and Delta transaction log inspection for the submission as required by the grading rubric (Lightweight Path).

---

## 1. Storage Hierarchy Overview (`_lakehouse/`)

The full tree structure of the Lakehouse storage is captured in [`lakehouse_tree.txt`](lakehouse_tree.txt).

### Key Layers & Tables:
- **`_lakehouse/bronze/`**:
  - `llm_calls_raw/`: 200,000 raw inference events with seeded duplicates.
  - `agent_traces/`: 1,578 trajectory steps across 300 sessions.
  - `docs_multimodal/`: 2,000 document embeddings and metadata.
- **`_lakehouse/silver/`**:
  - `llm_calls/`: Deduplicated and parsed inference calls partitioned by `date`.
  - `agent_trajectories/`: Conformed agent steps partitioned by `agent_version`.
  - `training_corpus_governed/`: Governed documents partitioned by EU AI Act Art. 10 `provenance_bucket`.
- **`_lakehouse/gold/`**:
  - `llm_daily_metrics/`: p50/p95 latency, error rates, and cost rollups across 7+ dates and 3 models.
  - `agent_performance/`: Success rate, step count, and cost rollups across policy versions.
- **`_lakehouse/iceberg/`**:
  - Isolated catalog databases (`nb5/`, `nb6/`, `nb8/`) with SQLite catalog and Iceberg v2/v3 metadata (`metadata.json`, manifest lists, manifest files).

---

## 2. Sample Delta Transaction Log Inspection

A sample transaction log commit from `_lakehouse/bronze/agent_traces/_delta_log/00000000000000000000.json` is formatted in [`delta_log_sample.json`](delta_log_sample.json).

### Commit Actions Present:
1. **`protocol`**: Minimum reader/writer protocol versions (`minReaderVersion: 1`, `minWriterVersion: 2`).
2. **`metaData`**: Schema definition (including types, nullability), table ID, partition columns, and configuration.
3. **`add`**: Physical Parquet data file additions, including:
   - File path (`part-00000-...parquet`)
   - File size in bytes
   - Modification timestamp
   - Per-file statistics (`minValues`, `maxValues`, `nullCount`, `numRecords`) used for Z-order and predicate file pruning.
4. **`commitInfo`**: Operation name (`WRITE` / `MERGE` / `RESTORE`), timestamp, and engine metadata.
