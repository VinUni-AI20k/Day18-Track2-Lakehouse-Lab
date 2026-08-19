# Lakehouse Anti-Pattern Reflection

**Identified Anti-Pattern:** Small-File Problem & Neglecting Compaction/Orphan Cleanup (The "Streaming Ingestion Trap")

Our team's data platform is at the highest risk of falling into the **Small-File Problem**, coupled with a lack of automated maintenance jobs. 

### Why is our team vulnerable?
1. **Streaming Ingestion Shape:** We ingest continuous high-frequency events (LLM traces, CDC, and micro-batches) directly into the Lakehouse. This creates thousands of tiny Parquet files every hour, significantly bloating transaction logs (`_delta_log/` and Iceberg metadata).
2. **Missing Maintenance Lifecycle:** Without automated, scheduled compaction (`OPTIMIZE` / `compact()`) and orphan file removal (`vacuum` / `remove_orphan_files()`), our query engine faces severe I/O bottlenecks. Scan planning time degrades exponentially, and cloud storage bills inflate due to stranded, uncommitted files from transient job failures.

### Action Plan & Mitigation
To prevent query degradation and cost spikes, we will:
* Implement scheduled background compaction to bin-pack small files into targeted sizes (e.g., 256MB–512MB).
* Apply `Z-ORDER` / clustering on key filter predicates (`user_id`, `model`, `date`) to maximize file skipping.
* Automate daily maintenance pipelines to sweep orphan files and expire old metadata snapshots safely.