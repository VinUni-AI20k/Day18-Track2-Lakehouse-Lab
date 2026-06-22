The lakehouse anti-pattern I am most likely to hit is the small-file problem.

In an LLM observability pipeline, it is tempting to write every micro-batch or request burst directly to object storage and call it “streaming.” That works in a demo, but at high volume it creates too many tiny Parquet/Delta files. The result is slow metadata listing, poor file pruning, expensive queries, and dashboards that miss their latency SLA even when the data itself is correct.

This lab made that risk concrete. In NB2, the table had 200 small files before OPTIMIZE. After compaction and Z-ordering, the file count dropped and the query became much faster. That showed me that storage layout is not just an implementation detail; it is part of the architecture.

My fix is to design ingestion with reasonable batch sizes, scheduled compaction, clustering by hot query keys, and table-health monitoring from day one.
