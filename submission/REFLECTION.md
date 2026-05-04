# Lab Reflection

In our LLM observability pipeline, the anti-pattern we are most at risk of is the **Small File Problem**. 

Since LLM calls are often ingested in real-time or via frequent mini-batches, streaming them directly into the Lakehouse without optimization would result in thousands of tiny Parquet files. As demonstrated in Lab 2, this "streaming-ingestion shape" significantly degrades read performance due to the metadata overhead of scanning numerous files. 

If left unmanaged, our Gold-layer dashboards would slow down substantially. By implementing regular `OPTIMIZE` (compaction) and `ZORDER` (clustering by `model` or `user_id`), we can reduce file counts and enable efficient file skipping, ensuring our latency and cost metrics remain performant even as the dataset grows to millions of rows.
