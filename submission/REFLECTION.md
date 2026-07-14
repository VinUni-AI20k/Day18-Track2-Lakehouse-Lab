The anti-pattern my team's data would most easily fall into is the **small-file problem**
from unbatched streaming ingestion. We currently ingest clickstream events row-by-row
into cloud storage for real-time dashboards. Without an explicit buffering or
OPTIMIZE schedule, this produces thousands of tiny Parquet files per hour.

Downstream Spark jobs then spend more time on metadata overhead and S3 LIST calls
than on actual computation, and Z-order cannot skip files effectively when the
table is already over-partitioned. The Day 18 lab showed that even a modest
`OPTIMIZE` + `ZORDER BY` can yield a 27× speed-up, so the fix is both known and
low-effort: batch writes at the source (e.g., Kafka → micro-batches of 5 min)
and run a nightly `OPTIMIZE ... ZORDER BY (user_id)` maintenance job. The only
blocker is cultural — engineers treat the lake like a database rather than a
file system that needs compaction.
