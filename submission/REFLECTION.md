# Reflection

The anti-pattern my team is most at risk of is turning the lake into a write-only data swamp: capturing every raw request/response, but postponing ownership, retention, and access control until later.

That is especially dangerous for LLM observability because the volume is large, the fields are sensitive, and the value decays quickly. If we keep Bronze data forever or let schema changes arrive without validation, the platform becomes expensive to run, hard to query, and risky to expose. The safer pattern is to enforce schema at ingest, tokenize PII before human access, and move older data into purpose-built Silver/Gold layers with explicit retention windows.
