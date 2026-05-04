# Reflection

Anti-pattern from slide section 5 that our team would be most at risk of: letting append-heavy ingestion create too many small files, then querying raw data directly as if storage layout did not matter.

Why: our likely real workload is event-style LLM observability data, where requests arrive continuously, retries create duplicates, and teams want dashboards quickly. In that situation, it is easy to optimize for "data lands fast" and ignore compaction, clustering, and table maintenance. The result is that queries stay logically correct but become slower, more expensive, and less predictable as file counts grow. That risk is realistic for us because it does not come from a dramatic design mistake; it comes from many seemingly harmless append decisions over time.

Mitigation we would apply in this lab / in production: keep Bronze append-only, enforce cleanup and dedup in Silver, and publish only curated Gold aggregates for routine analytics. Operationally, we would schedule compaction, use clustering or Z-order on high-value filter columns, and track table health with simple metrics such as file count, average file size, and query latency regression. The main lesson is that open-table reliability is not only about ACID correctness; physical layout is part of correctness for performance.
