# Reflection

**Anti-pattern our data is most at risk of: "no maintenance job" / treating storage as self-healing.**

Our pipeline is exactly what NB6 modeled: LLM call logs and agent traces
written continuously by many short-lived workers, some of which crash
mid-write. That's precisely the failure mode NB6 measured, not simulated —
100,000 rows reported by the table, 15 Parquet files on disk, only 10 in the
log: 5 files we pay for and cannot see. Compaction alone doesn't help either;
`VACUUM` only reclaims files the transaction log has *tombstoned*, so a file
that crashed before ever committing stays invisible at every retention
setting. The same trap shows up on the Iceberg side: `expire_snapshots` cut
20 snapshots to 3 but deleted **zero** manifest files — metadata kept growing
until we ran the orphan sweep separately (17 stranded `.avro` files, 37 KB).

Without a scheduled Job 4 (orphan sweep) paired with Job 3 (expiry), our
storage bill would silently climb even as "cleanup" jobs report success —
the exact "we expired snapshots but the S3 bill never dropped" story. Given
how many services write to this lakehouse, unmonitored crash-orphans are a
matter of when, not if.
