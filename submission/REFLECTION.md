# Reflection

Anti-pattern our team's data is most at risk of: **the Small-Files
Problem from unbatched streaming ingestion.**

Our LLM-observability pipeline (NB4's Bronze `llm_calls_raw`) ingests one
row per API call. A production version of this would land via a
short-trigger streaming job (e.g. Kafka → lakehouse every few seconds),
which is exactly the shape NB2 and NB6 reproduced: 200 micro-batches →
200 tiny files, each perfectly correct on its own. The accumulation is
the bug, not any single commit.

Measured evidence from our own run: before compaction, a point-query
scanned all files and object-storage GET costs projected to **$4.00/day**
at 200 files vs **$0.08/day** at 4 files (NB6). After `OPTIMIZE` +
`Z-ORDER`, file count dropped 200→55 (NB2) and query latency improved
~11×, with 90% of files skippable for a point query (NB6 Job 2).

**Fix:** schedule Job 1 (compaction) and Job 2 (Z-ORDER clustering) as a
recurring maintenance job, cadenced to the ingestion trigger interval —
not run ad hoc after someone notices slow dashboards. Pair every
`VACUUM`/`expire_snapshots` (Job 3) with an orphan sweep (Job 4), since
NB6 showed expiry alone reclaims zero bytes. Track file count as a
first-class pipeline metric, not just row count.
