# Reflection: Top Lakehouse Anti-Patterns in Production

**Anti-Pattern:** The Small-File Problem & Missing Table Maintenance Lifecycle

Our engineering team is most vulnerable to the **Small-File Problem** combined with **Unmanaged Storage Garbage**. With continuous streaming and micro-batch pipelines (e.g., Kafka ingesting LLM inference traces every 5–10 seconds into Bronze Delta tables), tens of thousands of sub-megabyte Parquet files accumulate rapidly.

Without automated table maintenance:
1. **Query & FinOps Degradation:** Analytics queries suffer severe latency and non-linear cost explosion caused by excessive S3 `GET` request fees and scan planning overhead rather than actual data transfer.
2. **Hidden Orphan Accumulation:** Teams mistakenly assume `VACUUM` cleans everything; however, uncommitted files from crashed jobs are never logged and remain invisible to basic vacuuming. Similarly, Iceberg snapshot expiry without manifest list sweeping leaves orphaned metadata on disk.

**Action Plan:** We must establish a mandatory 4-job maintenance cron schedule:
- **Job 1 (Compaction):** Target 128–512 MB file sizes.
- **Job 2 (Clustering):** Z-ORDER on high-cardinality filters (`user_id`, `model`).
- **Job 3 (Expiry):** Controlled retention windows.
- **Job 4 (Orphan Removal):** Directory listing diffs to sweep uncommitted files.
