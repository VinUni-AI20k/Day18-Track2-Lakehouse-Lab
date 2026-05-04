# Reflection

**Question:** Which anti-pattern from slide §5 would your team's data be most at risk of, and why? (≤ 200 words)

**Answer:**

My data is most at risk of the **Small Files Problem** and devolving into a **Data Swamp**. 

Since I'm dealing with continuous LLM observability telemetry (capturing API latencies, token usage, and status codes), this data is generated constantly at high volume. If this stream is ingested directly into the Bronze layer without regular compaction (like `OPTIMIZE`), it will result in millions of tiny files. This severely degrades I/O and metadata performance for any downstream queries.

Furthermore, because my telemetry arrives as unstructured `raw_json`, failing to enforce a strict Medallion architecture would turn my storage into a Data Swamp. Without a robust Silver layer to parse the JSON, enforce schema, and deduplicate retries (which make up ~5% of my requests), the data remains untrustworthy. This would make it impossible to reliably calculate the critical Gold-layer aggregations my business needs, such as p50/p95 latencies and daily costs.
