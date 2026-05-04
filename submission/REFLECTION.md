# Reflection — Anti-Pattern Most Likely to Affect Our Team

## The Small-File Problem

The anti-pattern our team is most at risk of is the **small-file problem** — accumulating hundreds or thousands of tiny Parquet files in the lakehouse instead of fewer, well-sized files.

This risk is high for us because our primary data sources are **streaming-style ingestion pipelines** (e.g., LLM API call logs, user interaction events). Each micro-batch or API callback appends a small file to the Bronze layer. Without a scheduled `OPTIMIZE` + `COMPACT` process, the number of files grows linearly with ingestion frequency. Within days, a table can balloon to thousands of files, each only a few KB.

The consequences are severe: query performance degrades dramatically because the engine must open and read metadata for every file. In NB2, we observed that 200 small files caused queries to run **3–10× slower** than after compaction and Z-ordering. File-listing overhead on object stores (S3, GCS) compounds this further in production.

Our mitigation plan is to schedule **daily `OPTIMIZE` jobs** on high-ingestion tables and apply **Z-ORDER** on frequently filtered columns (e.g., `user_id`, `date`). We will also set `target_size` thresholds to prevent over-compaction into single files, preserving file-skipping benefits.
