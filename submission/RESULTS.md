# Measured results — every rubric criterion, with the number and the reading

Environment: Python 3.12.3, `deltalake` 1.6.2, `pyiceberg` (sql-sqlite),
DuckDB 1.5.5, Polars 1.43.2 — lightweight path, fully offline.
Gate transcript: [`screenshots/00_terminal_make_gate.png`](screenshots/00_terminal_make_gate.png) (screenshot) and [`screenshots/00_make_gate.txt`](screenshots/00_make_gate.txt) (text)
(`make smoke` 9/9 · `make test` 24/24 · `make run-all` **8/8** — 68.6s in the shell run, 78.7s in the Jupyter-terminal run).

## Part A — Foundations (44 pts)

| # | Criterion | Measured | Reading |
|---|---|---|---|
| 1 | `_delta_log/` JSON commits | v0 commit with `protocol` + `metaData` + `add(stats)` — see [`02_delta_log_v0_commit.json.txt`](screenshots/02_delta_log_v0_commit.json.txt) | The log *is* the table. Parquet files are inert until an `add` action names them. |
| 1 | Schema enforcement | `Cast error: Cannot cast string 'thirty' to value of Int64 type` | The write is rejected at commit time, not discovered by a downstream analyst three weeks later. |
| 1 | `schema_mode="merge"` | `tier` added; old rows read back `NULL` (3 null, 1 `premium`) | Evolution is **opt-in**: the same write that fails by default succeeds when the author declares intent. No backfill job ran. |
| 2 | Small-file problem | 200 files before OPTIMIZE | 200 commits of ~500 rows — the shape every naive streaming writer produces. |
| 2 | Speedup **or** pruning | speedup **12.4×** (281.0 ms → 22.7 ms) **and** files-pruned **55.0×** (1 of 55 files covers `user_id=4242`) | Both criteria clear. Wall-clock is the noisy one; the file-pruning ratio is the number that survives a loaded laptop, which is why the rubric accepts either. |
| 2 | `numFiles` drops | 200 → 55 | Only 4× fewer files, but 55× fewer *read*: compaction alone buys the file count, Z-ORDER buys the pruning by making min/max ranges disjoint. |
| 3 | `history()` ≥ 5 incl. RESTORE | 5 versions: v0 WRITE, v1 WRITE, v2 MERGE, v3 WRITE, **v4 RESTORE** | RESTORE is an ordinary commit ([`03_delta_log_v4_restore.json.txt`](screenshots/03_delta_log_v4_restore.json.txt)) — it *adds* a version rather than erasing three, so the rollback is itself auditable. |
| 3 | MERGE upsert 100K | 0.17s; `num_source_rows=100000`, 50,000 updated + 50,000 inserted, 50,000 copied | One MERGE, two outcomes per key — the operation a Hive table cannot express without rewriting the partition. |
| 3 | RESTORE removes bad data | `score < 0` count = **0** after restore (0.03s) | A metadata-only rollback: 1 file removed, 0 files restored. Nothing was rewritten, which is why it is a 30-millisecond operation instead of a 30-minute one. |
| 4 | Bronze/Silver/Gold on disk | all three under `_lakehouse/{bronze,silver,gold}/` — [`01_tree_lakehouse.txt`](screenshots/01_tree_lakehouse.txt), [shot](screenshots/07_terminal_lakehouse_layout.png) | Silver and Gold are physically partitioned (`date=…`), Bronze is not: raw landing optimises for write throughput, curated layers for read pruning. |
| 4 | Silver dedup | 200,000 → **190,052** (9,948 duplicates dropped) | ~5% duplication is what at-least-once delivery costs. Deduping at Silver, not at query time, is what keeps the Gold cost figure honest. |
| 4 | Gold correctness | **8 dates × 3 models = 24 rows**, p50/p95/`cost_usd`/`error_rate` populated | e.g. 2026-04-07 `claude-opus-4-7`: p50 3,069 ms, p95 5,967 ms, $285.16, error rate 6.18%. Opus costs 6× Haiku's daily spend on ⅓ the tokens — the Gold table exists so that sentence is one query, not a data-engineering project. |

## Part B — Lakehouse 2026 (50 pts)

| # | Criterion | Measured | Reading |
|---|---|---|---|
| 5 | Created through the catalog; `day(ts)` spec | `SqlCatalog` → `('lake','llm_events')`, spec `1000: ts_day: day(2)` | `ts_day` is never inserted — it is *derived* from `ts` by the transform recorded in the spec. The catalog, not the writer, owns the layout. |
| 5 | Hidden-partition pruning ([shot](screenshots/04_jupyter_nb5_hidden_partition_pruning.png)) | **10×** (10 files → 1) filtering on `ts`, not `ts_day` | The filter names a column the user actually knows. A Hive user who forgot `WHERE dt=…` reads all 10 files: at 512 MB/file and $5/TB scanned that is 4.5 GB wasted per query = $0.022, **$220/day at 10K queries**. Hidden partitioning removes the opportunity to forget. |
| 5 | Three-tier metadata + ratio | metadata.json → 10 manifest lists → 10 manifests → 10 data files; data 47.3 KB vs metadata 131.5 KB = **278%** | Absurd at 10 rows/file, ~0.1% at 512 MB/file. Small files punish you twice: more data files *and* more metadata to plan over. |
| 5 | Rename keeps `field_id`; ≥ 2 specs | `latency_ms → latency_millis` both `field_id=4`; specs `[1, 2]` coexist, 5,500 rows readable | The rename rewrote no data because Parquet is addressed by field ID, not by name. Two layouts live in one table with zero rewrites — that is the migration story Hive never had. |
| 6 | Job 1 — Compaction | 200 → **11 files** (18×); avg file 51.5 KB → compacted | Data bytes went *up* (10.1 → 16.1 MB) before vacuum: compaction writes new files before old ones are reclaimed. You pay for both, briefly — budget for it. |
| 6 | Job 2 — Clustering | point query opens **1 of 10** files → **90% skipped** | Unclustered data has overlapping min/max ranges, so the stats prove nothing and the engine reads everything. Clustering is what makes stats *useful*. |
| 6 | Job 3 — Expiry | Delta vacuum reclaimed **16.1 MB** (211 tombstoned files); Iceberg 20 → **3 snapshots** | Vacuum bought bytes *and* destroyed time travel to v0 — that is the trade, made explicitly. Iceberg's expiry reclaimed nothing (below). |
| 6 | Job 4 — Orphans | **3 planted Delta orphans found + removed** (21.2 KB); **17 stranded Iceberg manifest lists swept** (36.6 KB) | Two findings that contradict the common belief — see below, and [`05`](screenshots/05_jupyter_nb6_vacuum_blindspot.png) / [`06`](screenshots/06_jupyter_nb6_orphan_removal.png). |
| 6 | Job 5 — Checkpoint | `00000000000000000099.checkpoint.parquet` + `_last_checkpoint` present | A cold reader replays 1 checkpoint + a few JSONs instead of 204 JSONs — the difference between a 200 ms and a 20 s cold start. |
| 7 | Random-access amplification | inline row group 12.5 MB vs one 64 KB object = **200×** | The inline file has **1 row group of 200 rows**: the smallest thing Parquet will hand you is a row group, so fetching one frame reads all 200. At 1,000 fetches/sec that amplification *is* the GPU-starvation problem. Formats like Lance restructure the file so a random read costs ~one row. |
| 7 | int8 quantization | 2.6 MB → 451.9 KB = **5.8× smaller** (83% saved); **recall@10 = 0.904**, **topic fidelity = 1.000** | int8 loses ~10% of exact IDs but 100% of returned neighbours are still on-topic — the "misses" are swaps between near-equivalent docs, so exact-ID recall *understates* quantization quality for RAG. Measure both on your own corpus. |
| 7 | Semantic search as SQL | top-5 for `storage-note-00007` all `topic=storage` (sim 1.000 → 0.768), brute force 32.3 ms over 2,000 vectors | Extrapolated: 1M vectors ≈ 16 s — not a serving path. The lakehouse is the system of record; the vector DB is a *rebuildable derived index*. |
| 7 | Lifecycle bug ([shot](screenshots/09_jupyter_nb7_lifecycle_bug.png)) | erasure for `user_042`: lakehouse 2,000 → 1,992 rows, **0 hits in-table**; external index untouched at 2,000 rows, **8 hits still retrievable** | The index will feed erased content into a RAG prompt until the next sync — and forever if the sync is one-way upsert, because *deletes are the operation sync pipelines forget*. The fix shown: subscribe to CDF (8 delete events carrying the doc IDs) instead of guessing. |
| 8 | Medallion + partitions + both policies | Silver 1,578 steps partitioned `agent_version=policy-v2 / policy-v3`; Gold covers both (v2: 150 traj, 76.0% success, $10.37; v3: 150 traj, 75.3%, $10.39) | v3 is not better than v2 — 0.7 pp on n=150 is noise. The Gold table's job is to make that legible *before* someone ships the new policy. |
| 8 | Version pin + replay | run pinned `table_version=0` / 1,578 steps; table advanced to v1 / 1,978 steps; replay at v0 = 1,578, **exact match = True** | That one integer is the difference between a reproducible run and a story. |
| 8 | MCP surface | 5 turns → **1 catalog round-trip** (`ttlMs: 60000`, `cacheScope: session`); `delete_rows` → `resultType: input_required` before executing; `tasks/get` polls working → working → completed (`{'rows': 300}`) | The agent cannot self-approve — the gate belongs to the protocol, not to the model. Iceberg 1.11 server-side planning returns a plan ID polled the same way; two protocols, one shape. |
| 8 | Four Art. 10 buckets; UNCLASSIFIED excluded | partitions `licensed / public_domain / scraped_optout_checked / synthetic / UNCLASSIFIED`; **1,666 of 2,000** rows trainable, **334 excluded** | Provenance as a *partition* means "show me only defensible data" is a partition prune, not a filter over everything. Mixing scraped and licensed rows in one unlabelled bucket is the 2026 audit failure. |

## Part C — Reproducibility (6 pts)

* `make test` — **24 passed** (suite has grown past the 22 the rubric names).
* `make run-all` — **8/8**, repeatedly, from the venv built by `make setup` (68.6s / 78.7s across the two captured runs).
* Notebooks in `notebooks/*.ipynb` are executed **with output cells preserved**
  (`jupyter nbconvert --execute --inplace`); the Jupytext `.py` twins are what
  `make run-all` executes, and both are in sync.

## The two findings that contradict a common belief

**1. `VACUUM` does not remove orphans it never saw.** After vacuum the table
reported 100,000 rows and 10 files in the log — but **15 parquet files sat on
disk**. A second vacuum dry-run listed 211 files and still left the 5 orphans
behind. `deltalake` reclaims what the log **tombstoned**; a file written by a
crashed job was never committed, so it was never tombstoned, so the log has no
idea it exists. It is invisible at *every* retention setting. The only way to
find it is the set difference `on-disk − in-log`, which NB6 makes you write
yourself — and the age guard on that diff is not optional, because without it
you will delete a concurrent writer's uncommitted file and corrupt the table.
Spark's VACUUM does add a directory listing pass, which is why the slide files
VACUUM under orphan removal — but never *assume* your engine does it. Verify.

**2. Iceberg's `expire_snapshots` reclaimed zero bytes.** Snapshots 20 → 3,
avro files **40 → 40**, and metadata *grew* 326.0 KB → 333.2 KB (expiry itself
writes a new metadata.json). Only after chaining an orphan sweep — 17 stranded
manifest lists, 36.6 KB — did the footprint actually drop to 296.5 KB / 23 avro.
Job 3 and Job 4 are a **pair**. Running expiry without a sweep is the precise
reason teams report "we expire snapshots every night but the S3 bill never moves."

Both behaviours are pinned by canary tests in `tests/` — if a library changes
its mind, the suite goes red instead of the lesson going quietly stale.
