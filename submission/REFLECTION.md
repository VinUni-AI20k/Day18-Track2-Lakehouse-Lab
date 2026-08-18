# Reflection

**Anti-pattern most at risk: Small-Files Problem from unbatched/streaming
ingestion — skipping `OPTIMIZE`.**

NB2 measures this directly on this lab's own lakehouse: writing 200K rows
without compaction produces **200 files**, and a point-lookup query runs at
a **257.8 ms** median. After `OPTIMIZE` + `Z-ORDER BY(user_id)`, the same
table drops to **55 files** and the same query runs at **27.6 ms** — a
**9.3× speedup** from file count alone, no query rewrite. Every extra file
means an extra Parquet footer to open and extra min/max stats to check
before a single row is read; below a certain file size, that per-file
overhead dominates actual I/O.

Our data is exposed here because writes arrive in many small batches rather
than one large write — exactly the shape streaming ingestion takes in
production: many small commits per minute, not one nightly load.

**Fix:** treat `OPTIMIZE` as a recurring compaction job (Job 1 in NB6,
hourly or triggered by file-count threshold), not a one-time cleanup —
paired with `Z-ORDER` on the columns hot-path queries filter by. Schedule
must match write cadence: too rare and small files pile back up, too
frequent and compaction competes with ingestion for the same cluster.
