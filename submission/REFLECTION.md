# Submission Reflection — Day 18 Lakehouse Lab

**Anti-Pattern at Highest Risk:** *Small-File Problem & Deferred Maintenance* (Anti-Pattern #1).

**Why our team is at risk:**
In our production ingestion pipeline, streaming CDC events and micro-batch ingestion write thousands of tiny Parquet files (< 1 MB) per hour into the storage layer. Without automated, background maintenance jobs, queries on Delta/Iceberg tables degrade dramatically due to metadata overhead and non-clustered row groups.

**Key Learnings from Day 18 Lab:**
1. **Compaction & Z-Ordering:** Running scheduled `OPTIMIZE` with Z-ordering on high-cardinality query keys (e.g., `user_id`) achieves over 10× file-pruned ratio, eliminating unnecessary row-group scans.
2. **Vacuum & Expiry Pairing:** Delta `VACUUM` only cleans tombstoned files present in log commits, ignoring uncommitted job crashes. Similarly, Iceberg `expire_snapshots` only updates metadata. Maintenance jobs must pair snapshot expiry with orphan file cleanup (`Job 4`) to prevent hidden storage billing bloat.
