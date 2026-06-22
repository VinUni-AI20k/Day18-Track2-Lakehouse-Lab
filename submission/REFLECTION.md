# Reflection

Our team would be most at risk of the **mixing Bronze/Silver/Gold responsibilities** anti-pattern. In a fast project, it is tempting to clean raw records directly inside the first table and let dashboards read from whatever table is available. That makes the pipeline look simple at first, but it hides data quality decisions and makes debugging difficult when duplicate requests, malformed JSON, or late-arriving records appear.

This lab shows why the layers should stay separate. Bronze preserves the raw LLM call logs, Silver applies validation and deduplication by `request_id`, and Gold contains only aggregated business metrics such as p50/p95 latency, cost, and error rate. If a Gold number looks wrong, we can trace it back through Silver to the original Bronze records instead of guessing. For our future data products, this separation is important because it protects auditability, rollback, and reproducibility.
