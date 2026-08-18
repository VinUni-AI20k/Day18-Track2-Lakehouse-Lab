# Lakehouse Anti-Pattern Reflection

**Anti-Pattern Most at Risk: High-Frequency Micro-Batch Ingestion without Coordinated Maintenance (The Small-File & Orphan Trap)**

In our production streaming pipelines (e.g., event logs, CDC from relational DBs, and LLM telemetry), ingestion triggers run every few seconds to satisfy sub-minute analytical latency SLAs. This pattern writes thousands of sub-megabyte Parquet files daily. 

Without automated compaction and explicit orphan-file sweeps, two severe issues emerge:
1. **Query & Cost Amplification:** File planning overhead skyrockets in object storage (S3/GCS), incurring millions of list/GET API calls that degrade p95 query latency from <1s to >30s.
2. **Invisible Orphan Accumulation:** Failed or retried streaming writers leave uncommitted files on disk that standard `VACUUM` (which only scans tombstoned entries in Delta logs) fails to delete, creating silently compounding storage bills.

**Remediation Strategy:** We enforce a two-tier maintenance cadence: (a) automated hourly bin-packing compaction targeting 128–256 MB file sizes alongside clustering (Z-Order/Liquid) on high-cardinality predicate keys, and (b) daily scheduled differential orphan-removal jobs (disk listing minus live transaction log references with a 24-hour safety guard) coupled with deliberate 7-day snapshot retention.
