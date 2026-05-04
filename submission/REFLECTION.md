# Reflection (Day 18)

The slide §5 anti-pattern our team would be most exposed to is the **small-file storm**: many tiny commits to the same Delta table (micro-batches, per-request appends, or “just append every Kafka poll”) **without** a disciplined **OPTIMIZE / compaction** cadence and **without** aligning file layout to real query predicates (e.g. **Z-order** on `tenant_id`).

We ship features quickly and lean toward streaming-style writes for observability and model logs. That pattern is deceptively healthy at low volume—queries still feel fast—then collapses under production traffic: metadata explodes, list operations and planning slow down, and dashboards that filter on a hot dimension scan far more Parquet files than the data size justifies. The table is “there,” but **FinOps and latency quietly rot** until someone runs an expensive full scan during an incident.

Guarding against this means treating compaction, clustering, and **file-count SLOs** as part of the product contract, not a weekend cleanup—exactly the lesson NB2 makes measurable.
