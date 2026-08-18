# Lakehouse Anti-Pattern Reflection

The anti-pattern our team is most likely to encounter is **treating the lakehouse as cheap object storage without active maintenance**.

Data pipelines continuously create new files, and without compaction, clustering, retention policies, and orphan-file cleanup, the storage layer gradually becomes fragmented. This can increase query latency, metadata overhead, and storage cost even when the logical amount of useful data remains almost unchanged.

The Day 18 lab made this risk clearer to me because maintenance operations solve different problems. `OPTIMIZE` reduces small-file fragmentation, while clustering improves data skipping. Snapshot expiration controls historical metadata, but it does not necessarily remove every physical file. Similarly, Delta `VACUUM` cannot remove files that were never recorded in the transaction log, so orphan detection must be handled separately.

For our team, the safest approach is to treat maintenance as part of the data architecture rather than an occasional cleanup task. Compaction, clustering, snapshot retention, orphan detection, and storage-cost monitoring should therefore run as scheduled production jobs with measurable before/after metrics.
