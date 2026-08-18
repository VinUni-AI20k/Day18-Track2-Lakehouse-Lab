# Reflection: Top 5 Lakehouse Anti-Patterns

Our team's data pipeline is most at risk of **Anti-Pattern #1: Treating the Data Lakehouse like a traditional Data Lake without automated compaction and maintenance jobs**.

In our high-throughput streaming environment, frequent small appends rapidly produce thousands of micro-Parquet files. As demonstrated in Notebook 2 and 6, failing to schedule periodic compaction and Z-Ordering degrades point-query latency by over 10× and inflates metadata reading overhead.

Furthermore, Notebook 6 revealed a critical production trap: standard `VACUUM` operations only clean tombstones from committed transactions, completely missing uncommitted orphan files left behind by crashed ingestion jobs. Similarly, Iceberg snapshot expiry only cleans metadata while leaving underlying data files orphaned unless explicit orphan file sweeps are coupled with expiry.

Without automated 4-job maintenance (compaction, Z-order clustering, snapshot expiry, and orphan file cleanup), our cloud storage bills will swell silently while query performance continuously degrades.
