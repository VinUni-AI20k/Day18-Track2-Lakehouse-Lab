# Reflection

The anti-pattern our team's data is most at risk of: **treating "run maintenance"
as one job instead of a pair** — expiring/vacuuming metadata without a
matching orphan sweep, and assuming it reclaims storage.

NB6 measured this directly. Three files a crashed writer left mid-commit
(`part-9999x-crashed-writer-*.parquet`, 21.2 KB) never appeared in `deltalake`'s
`VACUUM` dry-run at *any* retention — because they were never tombstoned in
`_delta_log/`, VACUUM only reclaims what the log already knows is dead, not
what's simply sitting in the directory. Iceberg showed the mirror problem:
`expire_snapshots` dropped 20 → 3 snapshots, but 0 of 40 avro manifest files
were deleted, and metadata *grew* (330.3 KB → 337.7 KB) until we ran the
orphan/stranded-manifest sweep as a second, explicit step.

For an LLM-observability pipeline logging at real volume, crashed ingestion
jobs and routine snapshot expiry are weekly events, not edge cases. A team
that schedules "expire snapshots" as its only cleanup cron watches its S3
bill keep climbing with no reason to suspect why, since every job reported
success. The fix isn't a smarter VACUUM; it's treating Job 3 (expiry) and
Job 4 (orphan sweep) as one atomic maintenance unit, never run alone.
