# Reflection — Top 5 Lakehouse Anti-Patterns

*Nguyễn Thanh Bình — 2A202601274 — Track 2, Day 18*

**Most at risk: small files, plus the belief that one maintenance job covers four.**

Our data is streaming-shaped — short-interval batches off application logs. NB6
reproduced that exactly: 200 individually correct micro-commits left 200 files
against a 128–512 MB production target. No commit was wrong; the accumulation
was. Compaction fixed it 18×, and clustering then made 90% of files skippable
for a point query.

The part that changed my mind was second-order. I assumed `VACUUM` was the
cleanup job. It is not — delta-rs reclaims only *tombstoned* files, so the three
orphans I planted as crashed writers survived vacuum at every retention and
stayed invisible to `history()` and `file_uris()`. Iceberg failed in mirror
image: `expire_snapshots` cut 20 snapshots to 3 and deleted **zero** avro files
while metadata grew. Expiry makes files unreferenced; only an orphan sweep
deletes them.

That gap is what produces "we expire snapshots but the S3 bill never drops."
And it compounds: in NB6's managed-compaction model the per-object component
was 24% of the bill, driven by file count, not data volume. Fixing the writer's
trigger interval is cheaper than paying to clean up after it.
