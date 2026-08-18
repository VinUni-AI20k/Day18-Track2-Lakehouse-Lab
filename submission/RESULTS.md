# Measured results — Day 18 Lakehouse Lab

Every number below was produced on this machine by the executed notebooks in
[`notebooks/`](notebooks/). Reproduce with:

```bash
make setup && make smoke && make data && make data-ai && make test && make run-all
```

Environment: Python 3.10.14 · macOS (Darwin 25.1.0, arm64) · deltalake 1.6.2 · pyiceberg 0.11.1 · duckdb 1.5.5
Gate status: **`make test` 24/24 passed · `make run-all` 8/8 passed in 18.7s · `make smoke` 9/9 checks**

## Part A — Foundations

| NB | Criterion | Target | Measured |
|---|---|---|---|
| 1 | `_delta_log/` JSON commits visible | present | ✅ (full log in [`screenshots/EVIDENCE.md`](screenshots/EVIDENCE.md)) |
| 1 | Schema enforcement blocks `age=str` | blocked | ✅ write rejected |
| 1 | `schema_mode="merge"` adds `tier` | added | ✅ 2 tier groups queryable |
| 2 | Small-file problem reproduced | ≥ 100 files | **200 files** |
| 2 | Speedup **or** files-pruned | ≥ 3× / ≥ 10× | **10.6× speedup and 55.0× pruning** (1 of 55 files covers `user_id=4242`) |
| 2 | `numFiles` drops after OPTIMIZE | meaningful | 200 → 55 |
| 3 | `history()` incl. RESTORE | ≥ 5 versions | **5**, RESTORE present |
| 3 | MERGE upsert 100K rows | succeeds | ✅ 0.11s, 150,000 output rows |
| 3 | RESTORE removes bad data | `score < 0` = 0 | **0** |
| 4 | Bronze/Silver/Gold on disk | all three | ✅ |
| 4 | Silver dedup drops rows | Silver < Bronze | 200,000 → **190,052** (9,948 dupes removed) |
| 4 | Gold ≥ 7 dates × 3 models | ≥ 21 rows | **24 rows** (8 dates × 3 models) |

## Part B — Lakehouse 2026

| NB | Criterion | Target | Measured |
|---|---|---|---|
| 5 | Table created through the catalog, `day(ts)` spec | yes | ✅ path never chosen by hand |
| 5 | Hidden-partition pruning, filtering on `ts` | ≥ 5× | **10×** (10 → 1 files) |
| 5 | Three-tier metadata walked | reported | ✅ 10 manifest lists, ratio reported |
| 5 | Rename keeps `field_id`; ≥ 2 specs coexist | yes | ✅ `latency_millis` keeps `field_id=4`; 5,500 rows readable across both specs |
| 6 | **Job 1** Compaction | ≥ 10× fewer files | **200 → 11 (18×)** |
| 6 | **Job 2** Clustering, from min/max stats | ≥ 50% skippable | **90% skipped** (11/11 files before → 1/10 after) |
| 6 | **Job 3** Expiry | bytes reclaimed / 3 snapshots | Delta 16.1 → 6.2 MB; Iceberg **20 → 3 snapshots** |
| 6 | **Job 4** Orphans | 3 found + removed | **3 planted orphans found (21.2 KB) and removed**; Iceberg sweep 40 → 23 avro |
| 6 | **Job 5** Checkpoint | written | ✅ `*.checkpoint.parquet` + `_last_checkpoint` |
| 7 | Random-access amplification | ≥ 5× | **200×** (one row group = whole 12.5 MB file) |
| 7 | int8 smaller on disk | ≥ 3× | **5.8×** (83% saved) |
| 7 | int8 recall@10 **and** topic fidelity | ≥ 0.80 / ≥ 0.95 | **0.904** and **1.000** |
| 7 | Semantic search as SQL, on-topic | yes | ✅ top-5 all `storage`, 30.6 ms brute force |
| 7 | **Lifecycle bug reproduced** | 0 in-table, > 0 stale | **0 vs 8** ← violation; CDF emits 8 deletes |
| 8 | Silver partitioned by `agent_version` | yes | ✅ `policy-v2`, `policy-v3` on disk |
| 8 | Version pin replays exactly | match | ✅ pinned v0 = 1,578 steps after table grew to 1,978 |
| 8 | MCP: cacheable list, `input_required`, task poll | all three | ✅ 5 turns → 1 catalog read |
| 8 | 4 Art. 10 buckets as partitions; UNCLASSIFIED excluded | yes | ✅ 4 buckets + UNCLASSIFIED; **1,666 / 2,000 defensible**, 334 excluded |

## The three findings that contradict the common belief

1. **`VACUUM` does not remove uncommitted orphans.** After vacuum, the table
   reported 100,000 rows and 10 files in the log, while **15 parquet files sat
   on disk — 5 you pay for and cannot see**. A vacuum dry-run at 30-day
   retention still listed only the 211 tombstoned files and left all 5 alone:
   a file that was never committed was never tombstoned, so the log has no idea
   it exists, and no retention setting reaches it. The set difference *files on
   disk − files referenced by live metadata* found the **3 planted orphans
   (21.2 KB)** and removed them; the age guard correctly spared the 2 newest,
   which is exactly what stops you corrupting a table under a concurrent writer.

2. **Iceberg's `expire_snapshots` deletes no files.** Snapshots went 20 → 3, but
   the avro count stayed at **40** and metadata on disk *grew* 330.7 → 338.2 KB
   (expiry writes a new `metadata.json`). Expiry only makes files unreferenced;
   deleting them is Job 4. Chaining expiry → orphan sweep took avro 40 → 23 and
   reclaimed 36.9 KB. This is why teams report "we expire snapshots but the S3
   bill never drops."

3. **Delta has no fixed-width vector type.** `fixed_size_list<float>[256]` is
   written but reads back as a variable-length `list<float>`, so it must be cast
   (`emb::FLOAT[256]`) before DuckDB's fixed-size array functions bind — the
   reason Hudi 1.2 added a first-class `VECTOR(dim, type)` column.

Reading of the headline number, per the rubric's top band: NB5's **10× pruning
is 10× because the filter is on `ts` and Iceberg derived `ts_day` from the
stored transform.** A Hive user who forgot the partition predicate would have
read all 10 files — the notebook prices that mistake at **$220/day at 10K
queries/day** ($5/TB scanned).
