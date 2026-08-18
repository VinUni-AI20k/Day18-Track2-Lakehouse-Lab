# Reflection

Running NB2 and NB6, the anti-pattern our data is most at risk of is the
**small-files problem going unnoticed until a maintenance job is forced to
run**. NB2 reproduces it directly: an unbatched streaming-style write
pattern leaves 100+ tiny Parquet files per table before `OPTIMIZE`, and scan
latency tracks file count, not row count, until Z-ORDER compaction is run.

The riskier half is NB6's finding that this doesn't self-heal: `VACUUM`
only reclaims files that were *tombstoned* in the Delta log — a job that
crashes mid-write leaves orphan files that are invisible to vacuum at any
retention setting, and `expire_snapshots` on the Iceberg side deletes
*metadata* pointers, not the underlying Avro/Parquet bytes. Both jobs report
success while the S3 bill doesn't drop. For a team without a scheduled
compaction + orphan-sweep pipeline, file count creeps up silently until a
query that used to prune to 1 file starts scanning hundreds, and nobody
notices because "the job ran green."

The fix isn't a one-time OPTIMIZE — it's treating compaction, clustering,
snapshot expiry, and orphan sweep as one scheduled pipeline (NB6's four
jobs), not four independent buttons to press when things get slow.
