# Anti-Pattern Reflection (≤200 words)

Our team hit the **Small-Files Problem** during streaming ingestion: each micro-batch wrote a 500KB Parquet file, producing 200 files for 100K rows. The pre-OPTIMIZE DuckDB scan took 303 ms — 5× over the target.

We applied `OPTIMIZE` + `Z-ORDER` — file count dropped from 200 → 55, scan time fell to 28.8 ms, **~10× faster**. Z-ORDER clustering on `status, score` also kept small-file counts down across subsequent appends.

The lesson: the table format and the engine's storage layout are the same thing when you write your own Parquet. Open formats give you the lever; you have to pull it.

Other things we considered:
- Z-ORDER columns matter — wrong choice gives no prune.
- Per-commit versioning enables time-travel, but unbounded snapshots bloat the log.
- Hidden partitioning (Iceberg) eliminates the "did the user remember the partition column?" failure mode Delta users still hit.