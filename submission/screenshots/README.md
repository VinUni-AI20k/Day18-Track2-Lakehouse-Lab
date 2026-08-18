# Screenshots to capture

Rubric item 2 accepts **either** path. This folder now has both.

## Spark / MinIO path (done)

Live console: http://localhost:9001 (user/pass `minioadmin`).
Jupyter: http://localhost:8888 (token `lakehouse`).

Already captured:

- `minio_lakehouse_bucket.png` — buckets `bronze` / `silver` / `gold` / `lakehouse`
- `minio_users_delta.png` — `lakehouse/users_delta/` with `_delta_log/` + parquet
- `minio_delta_log.png` — `00000000000000000000.json` and `…0001.json`
- `spark_00000000000000000000.json` — Spark commit (`engineInfo: Apache-Spark/3.5.0 Delta-Lake/3.2.0`)
- `minio_listing.txt` — `mc ls` of the four buckets
- `nb2_spark_metrics.txt` — OPTIMIZE speedup **13.7×**, `numFiles` → 1

| NB | Spark notebook | Measured |
|---|---|---|
| 1 | `notebooks-spark/01_delta_basics.py` | schema enforcement blocked `age="thirty"`; `mergeSchema` added `tier` |
| 2 | `notebooks-spark/02_optimize_zorder.py` | BEFORE 1.84s → AFTER 0.13s = **13.7×**; `numFiles=1` |
| 3 | `notebooks-spark/03_time_travel.py` | MERGE 100K in 2.30s; RESTORE 3.54s; 5 versions incl. RESTORE; `score<0` = 0 |
| 4 | `notebooks-spark/04_medallion.py` | Bronze 1,000,000 → Silver 949,981; Gold 7 days × 3 models |

NB5–NB8 stay on the lightweight path (`pyiceberg` is Python-only).

## Lightweight path (also done)

Also photograph the **last code cell** of each notebook (`NBx complete.`) —
that is the same gate as `make run-all`. Open the `.ipynb` (not the `.py`).
Search (`Cmd-F`) for the printed string in the last column.

| NB | File | Cell(s) to photograph | What the grader should see |
|---|---|---|---|
| 1 | `01_delta_basics.ipynb` | 5 (history), 7 (`BLOCKED by schema enforcement`), 9 (`tier` column), **13 (`NB1 complete.`)** | `_delta_log` JSON commits; `age="thirty"` rejected; 2 DuckDB tier groups |
| 2 | `02_optimize_zorder.ipynb` | 3 (`Files before OPTIMIZE: 200`), 11 (Z-order metrics), **13 (`NB2 complete.`)** | Speedup **9.9×** and files-pruned **55.0×** (1 of 55 files cover `user_id=4242`) |
| 3 | `03_time_travel.ipynb` | 3 (`MERGE 100K rows`), 9 (`score<0 … 0`), 11 (history v0–v4 RESTORE), **13 (`NB3 complete.`)** | 5 versions including `RESTORE`; bad rows gone |
| 4 | `04_medallion.ipynb` | 5 (Silver 190,052 < Bronze 200,000), 9 (Gold 8 dates × 3 models), **11 (`NB4 complete.`)** | Bronze/Silver/Gold on disk; p50/p95/`cost_usd` |
| 5 | `05_iceberg_catalog.ipynb` | 9 (pruning **10×** on `ts`), 13–15 (3-tier metadata + byte ratio), 17 (`field_id=4`), 22 (`spec_id` 1 and 2), **24 (`NB5 complete.`)** | Catalog-created table; rename is metadata-only |
| 6 | `06_maintenance.ipynb` | 7 (200 → 11 files, **18×**), 9 (skip **90%**), 11 (VACUUM reclaimed **16.1 MB**), 15 (`VACUUM` misses orphans), 17 (3 orphans removed), 19 (checkpoint), 25 (`20 → 3` snapshots, 0 avro deleted), **30 (`NB6 complete.`)** | All 5 jobs + the two measured findings |
| 7 | `07_vectors_multimodal.ipynb` | 7 (amplification **200×**), 9 (int8 **5.8×** smaller), 17 (recall@10 **0.904**, fidelity **1.000**), 23 (0 vs 8 hits), **27 (`NB7 complete.`)** | Lifecycle bug: lakehouse 0, external index 8 |
| 8 | `08_agents_provenance.ipynb` | 3 (`agent_version=policy-v2/v3`), 6 (pin + replay True), 11 (5 turns → 1 catalog read), 13 (`input_required`), 21 (4 Art. 10 partitions), 25 (`user_007` erased), **27 (`NB8 complete.`)** | 4 Art. 10 buckets; UNCLASSIFIED excluded |

## Storage-layout evidence (this folder)

- `tree_lakehouse.txt` — lightweight medallion + `_delta_log/`
- `00000000000000000000.json` — lightweight NB1 first commit
- `spark_00000000000000000000.json` — Spark/MinIO NB1 first commit
