# Reflection

The anti-pattern our data is most at risk of: **the small-files problem from
high-frequency, low-volume writes** — a streaming job that commits every
event instead of batching, silently fragmenting the table.

NB2 and NB6 made this concrete. NB6's 200 micro-batches produced 200 files
averaging 51.5 KB each — orders of magnitude below the 128–512 MB production
target — and it's billed, not just slow: 200 files means 10,000,000 GETs/day
(~$4.00/day) full-scanning versus $0.08/day for the same data in 4 compacted
files. NB2 showed the latency side: 197.6 ms median before OPTIMIZE +
Z-ORDER, 14.2 ms after (13.9× speedup, 55× files-pruned).

Our LLM-observability pipeline (NB4/NB8) is exactly this shape — every
inference call is its own event, arriving continuously rather than in
scheduled batches. Nothing in the write path enforces a minimum batch size
or compaction cadence, so the table would degrade the same way NB6's did,
unnoticed, until a dashboard query or a storage bill made it visible. The
fix isn't a one-time OPTIMIZE; it's a scheduled Job 1 (compaction) plus
Job 5 (checkpointing) running continuously, which is why `06_maintenance.py`
treats them as production jobs, not lab exercises.
