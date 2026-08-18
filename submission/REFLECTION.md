# Reflection: Lakehouse Anti-Patterns in Production

**Anti-Pattern:** *The Small-File Problem & Unmanaged Maintenance (Anti-Pattern #1)*

In high-throughput LLM observability and streaming architectures, our workloads continuously append micro-batches of traces, tool calls, and user interactions. Without scheduled compaction jobs, this rapidly creates thousands of tiny Parquet files.

As demonstrated in NB2 and NB6, this causes two severe production bottlenecks:
1. **Query & Cost Degradation:** Query engines waste exponential CPU cycles on metadata listing and file opening rather than actual computation, degrading point-lookup and aggregation speeds by $3\times$ to $10\times$.
2. **Invisible Storage Bloat:** Streaming failures and uncommitted writes leave unreferenced orphan files that standard `VACUUM` ignores (since they were never committed to `_delta_log`), resulting in rising cloud storage costs.

**Mitigation:** We must implement an automated cron schedule executing the 4 mandatory maintenance jobs: (1) `OPTIMIZE` compaction, (2) `Z-ORDER` clustering on high-frequency predicate columns (`user_id`, `request_id`), (3) snapshot expiry, and (4) custom set-difference orphan scanning to prune uncommitted files.
