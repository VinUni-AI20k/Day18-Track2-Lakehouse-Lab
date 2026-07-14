Reflection (≤200 words)

Our team is most at risk of the "schema drift + silent writes" anti-pattern. When producers evolve event payloads (new optional fields, changed types) and writers default to permissive modes, bad or malformed rows slip into Bronze. If not caught early, subsequent joins and aggregations produce subtle downstream biases and expensive manual fixes.

Mitigations we’d apply: enforce strict writes at Bronze with explicit schema checks and automated smoke tests; use schema evolution only after a manual review and controlled `mergeSchema` with a documented migration plan; add monitoring alerts for sudden schema changes or spikes in null rates. These lightweight checks balance developer velocity and data quality for our medallion pipeline.
