# REFLECTION — Lakehouse Anti-Patterns

**Our data is most at risk of: small-file accumulation from streaming ingestion with no scheduled compaction.**

NB6 makes this concrete: a Kafka→lakehouse job with a 5-second trigger writes hundreds of tiny, perfectly-correct commits overnight. Each commit is correct; the accumulation is the bug. The cost is non-linear — object storage bills per request, and every extra file costs a `GET` plus metadata to plan over. In our pipeline, telemetry lands in micro-batches by design, so without a cron-based maintenance layer (compaction, Z-ORDER, snapshot expiry, orphan sweep) the table degrades silently until query latency blows up and the storage bill doubles.

What the lab showed beyond the slide: `VACUUM` does not remove uncommitted orphan files, and Iceberg's `expire_snapshots` is metadata-only — Job 3 and Job 4 must run as a pair. The fix is not a bigger writer but a scheduled maintenance job that treats these four jobs as non-optional.
