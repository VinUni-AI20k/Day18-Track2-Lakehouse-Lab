# Evidence map — rubric criterion → measured value

Every number below comes from the executed notebooks in [`notebooks/`](../notebooks/)
on this machine (Python 3.12.13, macOS 26.5.2 arm64, lightweight path, 2026-08-18).
Raw transcripts: [`screenshots/`](screenshots/).

Gate: `make smoke` ✓ · `pytest` 24 passed ✓ · `make run-all` 8/8 ✓.

## Part A — Foundations (44 pts)

| # | Criterion | Measured | Reading |
|---|---|---|---|
| 1 | `_delta_log/` JSON commits visible | 2 commits, `delta-rs:py-1.6.2` | Commit 0 carries `protocol` + `metaData` + `add`; commit 1 carries a *new* `metaData` — evolution is a log action, not a table rewrite. Full text: [`02_delta_log_commit.txt`](screenshots/02_delta_log_commit.txt) |
| 1 | Schema enforcement blocks `age=str` | `Cast error: Cannot cast string 'thirty' to value of Int64 type` | Rejected at write, not at read — the table never enters a mixed-type state |
| 1 | `schema_mode="merge"` adds `tier` | 4 rows, 3 with `tier=null` | Existing rows are not backfilled; the null *is* the history |
| 2 | Small-file problem reproduced | 200 files before OPTIMIZE | 200 micro-batches, each individually correct |
| 2 | Speedup ≥ 3× **or** pruning ≥ 10× | **speedup 8.9×**, **pruning 55×** | Both targets met. Pruning is the honest metric — 1 of 55 files covers `user_id=4242`; wall-clock on a laptop is noise-prone |
| 2 | `numFiles` drops after OPTIMIZE | 200 → 55 | Bin-packing to the target file size, not to one file |
| 3 | `history()` ≥ 5 versions incl. RESTORE | 5 versions, v4 = `RESTORE` | RESTORE is itself a commit — rollback moves *forward* in the log, so the bad state stays auditable |
| 3 | MERGE upsert 100K rows | 100K source → 50K updated + 50K inserted, 45 ms | `num_target_rows_copied: 50000` shows copy-on-write rewriting untouched rows |
| 3 | RESTORE rolls back; `score < 0` = 0 | 0 rows | |
| 4 | Bronze/Silver/Gold on storage | all three `_delta_log/` present | [`01_tree_lakehouse.txt`](screenshots/01_tree_lakehouse.txt) |
| 4 | Silver < Bronze | 200,000 → 190,052 (−9,948) | Dedup by `request_id`; at-least-once delivery upstream is the source of the 9,948 |
| 4 | Gold ≥ 7 dates × 3 models | 8 dates × 3 models = 24 rows | p50/p95, `cost_usd`, `error_rate` all populated; `date`-partitioned + Z-ordered on `model` |

## Part B — Lakehouse 2026 (50 pts)

| # | Criterion | Measured | Reading |
|---|---|---|---|
| 5 | Table through the catalog; `day(ts)` spec | `SqlCatalog`, spec `1000: ts_day: day(2)` | `ts_day` is derived, never inserted — the writer cannot forget it |
| 5 | Hidden-partition pruning ≥ 5× | **10×** (10 files → 1) filtering on `ts` | The filter is on `ts`; Iceberg maps it through the stored transform. A Hive user who forgot `WHERE dt=…` reads all 10 → at 512 MB/file and $5/TB, 4.5 GB wasted/query = **$220/day at 10K queries** |
| 5 | Three-tier metadata; metadata:data ratio | metadata 136.9 KB vs data 47.3 KB = **289.5%** | Absurd at 10 rows/file, ~0.1% at 512 MB/file. Small files punish twice: more data files *and* more metadata to plan over |
| 5 | Rename keeps `field_id`; ≥ 2 specs coexist | `latency_ms→latency_millis` both `field_id=4`; specs `[1, 2]`, 5,500 rows readable | Rename rewrote no data; two layouts live in one table with zero migration job |
| 6 | **Job 1** Compaction ≥ 10× fewer files | **200 → 11 (18×)** | Avg file 51.5 KB vs the 128–512 MB production target. Note data bytes rose 10.1 → 16.1 MB first: you pay for both copies until reclamation |
| 6 | **Job 2** Clustering ≥ 50% skippable | **90%** (1 of 10 files opened) | Unclustered min/max ranges overlap, so stats prove nothing. Clustering is what makes stats *usable* |
| 6 | **Job 3** Expiry | Delta reclaimed 16.1 MB; Iceberg 20 → 3 snapshots | Delta vacuum cost us time travel to v0 — that is the trade, not a side effect |
| 6 | **Job 4** Orphans | 3 planted Delta orphans removed; 17 stranded Iceberg manifest lists swept (37.3 KB) | See the two findings below |
| 6 | **Job 5** Checkpoint | `00000000000000000099.checkpoint.parquet` + `_last_checkpoint` | Cold reader replays 1 checkpoint + a few JSONs instead of 204 |
| 7 | Random-access amplification ≥ 5× | **200×** (12.5 MB row group vs 64 KB object) | Parquet's unit of read is the row group, not the row. At 1,000 fetches/s this *is* GPU starvation — the problem Lance restructures the file to solve |
| 7 | int8 ≥ 3× smaller; recall + fidelity | **5.8× on disk**; recall@10 **0.904**, topic fidelity **1.000** | Exact-ID recall *understates* quantization quality for RAG: the 10% "misses" are swaps between near-equivalent neighbours, and 100% of results stayed on-topic |
| 7 | Semantic search as SQL, on-topic | top-5 all `topic=storage`, sim 0.768–1.000, 7.2 ms/2K vectors | Extrapolates to 3.6 s at 1M vectors — a brute-force scan is not a serving path |
| 7 | **Lifecycle bug reproduced** | in-table 0 hits, external index **8 hits** after erasure | The index keeps serving `user_042` to RAG prompts until the next sync — and forever if the sync is upsert-only, because deletes are what sync pipelines forget. CDF emitted all 8 deletes: the index should *subscribe*, not guess |
| 8 | Trajectories through medallion | Silver 1,578 steps, partitions `agent_version={policy-v2,policy-v3}`; Gold both policies | v2 success 0.760 vs v3 0.753 at equal cost — the comparison the partition exists to make |
| 8 | Training run pins the version | pinned v0 = 1,578 steps; table moved to v1 = 1,978; replay matched | That one integer is the difference between a reproducible run and a story |
| 8 | MCP surface | 5 turns → **1** catalog read (`ttlMs: 60000`); `delete_rows` → `input_required`; task poll → `completed` | The agent cannot self-approve — the gate is the protocol's, not the model's |
| 8 | Four Art. 10 buckets as partitions | licensed 675 · public_domain 333 · synthetic 331 · scraped_optout_checked 327 · **UNCLASSIFIED 334** | 1,666/2,000 rows defensible; 334 excluded for `license=unknown`. Erasure for `user_007` removed 8 rows (v0 → v1) — but v0 still holds them, so "we support time travel" and "we honour erasure" only coexist if the retention window is a written decision |

## Part C — Reproducibility (6 pts)

| Criterion | Result |
|---|---|
| `make test` green | **24 passed** in 0.70 s ([transcript](screenshots/03_make_test_run_all.txt)) |
| `make run-all` green from clean setup | **8/8 passed** in 10.7 s |

> The rubric says 22 tests; this checkout collects 24 (`pytest --collect-only -q` →
> `tests/test_lab18.py: 24`). Note `make test` passes `-q` on top of the `-q` in
> `pytest.ini`, which suppresses the `24 passed` summary line — the transcript
> therefore also shows a plain `pytest` run.

## The two findings that contradict a common belief

The rubric singles these out; both are reproduced here.

**1. `VACUUM` does not remove orphans it never tombstoned.** After vacuum,
15 parquet files sat on disk while the log listed 10 — **5 files we pay for and
cannot see**. `deltalake` (Rust/Python) reclaims only files the log has
*tombstoned*; a file left by a crashed writer was never committed, so it was
never tombstoned. A dry-run reported 211 files and still found none of the 5.
Spark's VACUUM adds a directory listing pass — but "our engine probably does it"
is not an operational control. The fix in NB6 is a set difference between
`file_uris()` and the parquet files actually on disk, plus an age guard, without
which you delete a concurrent writer's uncommitted output and corrupt the table.

**2. `expire_snapshots` is metadata-only.** Snapshots 20 → 3, avro files
**40 → 40 (zero deleted)**, and metadata on disk *grew* 343.0 → 351.0 KB because
expiry writes a new `metadata.json`. Expiry's job is to make files
*unreferenced*; deleting them is Job 4. Chaining expiry → orphan sweep dropped
avro 40 → 23 and reclaimed 37.3 KB. This is precisely why teams report "we
expire snapshots but the S3 bill never drops."

Both are pinned by canary tests in [`tests/test_lab18.py`](../tests/test_lab18.py):
if a library starts behaving differently, the tests go red and these notes must
be revised rather than silently becoming wrong.
