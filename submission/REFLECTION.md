# Reflection — which anti-pattern is our data most at risk of?

**Small files from unmaintained streaming ingestion** — and the false safety of
a `VACUUM` we never verified.

Our data is agent/LLM telemetry: one append per session, thousands hourly. That
is NB6's setup exactly — 200 correct commits producing 200 files averaging
51.5 KB. No commit is wrong; the accumulation is the bug. Compaction gave
200 → 11 files (18×), and NB6's request model put that at $4.00/day versus
$0.08/day in GETs alone, before compute.

What makes it *our* risk is the second half. We would have scheduled `VACUUM`,
watched it reclaim 16.1 MB, and called the table clean — while NB6 measured 5
parquet files on disk the log never tombstoned, invisible to vacuum at any
retention. Iceberg was worse: expiry cut 20 snapshots to 3, deleted **zero**
avro files, and metadata *grew*. Only chaining expiry → orphan sweep reclaimed
37.3 KB.

So the anti-pattern isn't "we forgot to compact." It is trusting a green
maintenance job never checked by set-difference against actual storage.

**Mitigation:** compact on a trigger-interval budget, and reconcile
files-on-disk against files-in-log monthly, alerting on the gap.
