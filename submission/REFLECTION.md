# Reflection — Day 18 Lakehouse Lab
**Nguyễn Xuân Quân · Track 2 · 2A202601976**

## Top 5 Lakehouse Anti-Patterns: Which One Is Our Team Most At Risk Of?

**Anti-pattern: Orphan File Accumulation (Storage Leak)**

Our team's data workloads involve frequent batch ingestion jobs and iterative ML experiments that often fail mid-write — exactly the scenario where orphan files accumulate silently. NB6 demonstrated a critical production trap: `VACUUM` only reclaims files that were *tombstoned* in the transaction log. Files left by a crashed job that never committed are invisible to vacuum at any retention window.

In our environment, running `expire_snapshots` on Iceberg without a follow-up orphan sweep compounds this — the snapshot count dropped from 20 to 3, yet **zero Avro files were deleted**, causing metadata to actually expand. Job 3 (expiry) without Job 4 (orphan sweep) is the reason "we expired snapshots but the S3 bill didn't go down" — a failure mode we've observed in analogous pipelines.

The fix requires a set-difference approach: compare manifest-referenced files against the physical storage listing, then delete the delta. We are implementing this as a scheduled maintenance job paired with alerts on orphan-file count thresholds.

*(196 words)*
