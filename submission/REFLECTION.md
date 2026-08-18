# Reflection

**Anti-pattern most at risk: assuming garbage collection (`VACUUM` /
`expire_snapshots`) fully reclaims storage.**

NB6 measures the gap directly on this lab's own lakehouse. After a compact +
delete cycle, `VACUUM` dry-run found 211 tombstoned files it *could* reclaim
— but 5 files written by a simulated crashed job, never committed to the
transaction log, stayed on disk after vacuum ran (15 Parquet files on disk
vs. 10 in the log). `VACUUM` only diffs against the log; it has no way to
see what the log never recorded. Iceberg shows the mirror bug:
`expire_snapshots` dropped 20 snapshots to 3 but deleted **0** data files —
metadata shrank, the byte count didn't move.

This data is exposed here because every notebook writes many small commits
(NB2 alone produces 200 files before OPTIMIZE), and nothing runs a scheduled
orphan sweep outside the lab notebooks themselves. In production that
becomes "we ran VACUUM, the S3 bill didn't drop" — the trap Job 3 and Job 4
exist to close, but only if paired, not run independently. The fix: monitor
`files_on_disk − files_in_log` as its own metric, and never schedule expiry
without the orphan sweep that follows it.
