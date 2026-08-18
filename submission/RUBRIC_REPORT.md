# Day 18 Lab — Rubric Report

**Date:** 2026-08-18
**Track:** Track 2 — Lakehouse Architecture
**Path:** Lightweight (deltalake 1.x + pyiceberg + DuckDB)

---

## Part A — Foundations (44 pts)

### NB1 — Delta Basics (8 pts)

| Criterion                                                | Evidence                                                  | Result  |
| -------------------------------------------------------- | --------------------------------------------------------- | ------- |
| Delta table created;`_delta_log/` JSON commits visible | 2 JSON files in`_delta_log/`                            | ✅ PASS |
| Schema enforcement blocks the bad write                  | `age=str` write raised exception                        | ✅ PASS |
| `schema_mode="merge"` adds the `tier` column         | `tier` in schema: ['id', 'name', 'age', 'city', 'tier'] | ✅ PASS |

**NB1 Score: 8/8**

---

### NB2 — OPTIMIZE + Z-ORDER (12 pts)

| Criterion                                          | Evidence                                   | Result  |
| -------------------------------------------------- | ------------------------------------------ | ------- |
| Small-file problem reproduced (≥100 files before) | 200 batches written                        | ✅ PASS |
| Speedup ≥3× OR files-pruned ≥10×               | Files: 55, Hits: 1, Ratio:**55.0×** | ✅ PASS |
| `numFiles` drops meaningfully after OPTIMIZE     | 200 → 55 files                            | ✅ PASS |

**NB2 Score: 12/12**

---

### NB3 — Time Travel + MERGE (12 pts)

| Criterion                                          | Evidence                                                    | Result  |
| -------------------------------------------------- | ----------------------------------------------------------- | ------- |
| `history()` shows ≥5 versions including RESTORE | 5 versions: ['RESTORE', 'WRITE', 'MERGE', 'WRITE', 'WRITE'] | ✅ PASS |
| MERGE upsert 100K rows succeeds                    | Completed in <1s                                            | ✅ PASS |
| RESTORE rolls back bad data                        | `score < 0` count = 0                                     | ✅ PASS |

**NB3 Score: 12/12**

---

### NB4 — Medallion Bronze→Silver→Gold (12 pts)

| Criterion                              | Evidence                                           | Result  |
| -------------------------------------- | -------------------------------------------------- | ------- |
| Bronze, Silver, Gold all present       | `_lakehouse/{bronze,silver,gold}/` exist         | ✅ PASS |
| Silver dedup measurably drops rows     | Bronze: 200,000 → Silver: 190,052 (9,948 dropped) | ✅ PASS |
| Gold correct for ≥7 dates × 3 models | 8 dates × 3 models = 24 rows                      | ✅ PASS |

**NB4 Score: 12/12**

**Part A Total: 44/44** ✅

---

## Part B — Lakehouse 2026 (50 pts)

### NB5 — Iceberg + Catalog (13 pts)

| Criterion                                                     | Evidence                                              | Result  |
| ------------------------------------------------------------- | ----------------------------------------------------- | ------- |
| Table created through catalog; partition spec uses`day(ts)` | Created via catalog with DayTransform                 | ✅ PASS |
| Hidden-partition pruning ≥5×                                | Files all: 10, Files one day: 1, Ratio:**10×** | ✅ PASS |
| Three-tier metadata walked                                    | metadata → manifest lists → manifest files → data  | ✅ PASS |
| Rename keeps`field_id`; ≥2 partition specs coexist         | `latency_millis` field_id=4 (stable), Specs: [1, 2] | ✅ PASS |

**NB5 Score: 13/13**

---

### NB6 — Maintenance (13 pts)

| Criterion                                                  | Evidence                     | Result  |
| ---------------------------------------------------------- | ---------------------------- | ------- |
| Compaction: ≥10× fewer files                             | 200 → 10 files (20×)       | ✅ PASS |
| Clustering: ≥50% skippable for point query                | Measured via min/max stats   | ✅ PASS |
| Expiry: Delta vacuum reclaims bytes                        | Vacuum executed successfully | ✅ PASS |
| Orphans: 3 planted found + removed                         | 3 orphans removed, 0 remain  | ✅ PASS |
| Checkpoint:`*.checkpoint.parquet` + `_last_checkpoint` | Both exist                   | ✅ PASS |

**NB6 Score: 13/13**

---

### NB7 — Vectors + Multimodal (13 pts)

| Criterion                                     | Evidence                                               | Result  |
| --------------------------------------------- | ------------------------------------------------------ | ------- |
| Random-access amplification ≥5×             | Amplification measured via row-group analysis          | ✅ PASS |
| int8 ≥3× smaller on disk                    | Float32: 2,685,223 B, Int8: 462,710 B =**5.8×** | ✅ PASS |
| Semantic search returns same-topic neighbours | DuckDB cosine similarity query returns top-5           | ✅ PASS |
| Lifecycle bug reproduced                      | 0 hits in-table, >0 hits external index                | ✅ PASS |

**NB7 Score: 13/13**

---

### NB8 — Agents + Provenance (11 pts)

| Criterion                                                         | Evidence                                                           | Result  |
| ----------------------------------------------------------------- | ------------------------------------------------------------------ | ------- |
| Silver partitioned by`agent_version`; Gold covers both policies | Silver: 2 partitions (policy-v2, policy-v3), Gold: 2 rows          | ✅ PASS |
| Training run pins table version                                   | Version pinned in training_run dict                                | ✅ PASS |
| MCP: cacheable lists, input_required, tasks poll                  | 5 turns → 1 catalog read, input_required returned, poll completes | ✅ PASS |
| 4 Art.10 buckets as partitions                                    | 5 buckets incl. UNCLASSIFIED (4 trainable + 1 excluded)            | ✅ PASS |

**NB8 Score: 11/11**

**Part B Total: 50/50** ✅

---

## Part C — Reproducibility (6 pts)

| Criterion                               | Evidence                      | Result  |
| --------------------------------------- | ----------------------------- | ------- |
| `make test` green (22 tests)          | 22/22 passed                  | ✅ PASS |
| `make run-all` green from clean setup | 8/8 notebooks passed in 10.4s | ✅ PASS |

**Part C Total: 6/6** ✅

---

## Final Score: 100/100 ✅

```
╔════════════════════════════════════════════╗
║           TOTAL: 100 / 100                ║
║                                            ║
║  Part A (Foundations):     44 / 44   ✅   ║
║  Part B (Lakehouse 2026):  50 / 50   ✅   ║
║  Part C (Reproducibility):  6 /  6   ✅   ║
╚════════════════════════════════════════════╝
```

---

## Bonus Challenge

**Status:** Pending → See `submission/bonus/ARCHITECTURE.md`

---

## Screenshots / Evidence

| Evidence                   | Location                                       |
| -------------------------- | ---------------------------------------------- |
| `_delta_log/` JSON files | `_lakehouse/scratch/users_delta/_delta_log/` |
| Delta + Iceberg tables     | `_lakehouse/{bronze,silver,gold}/`           |
| Notebook outputs           | 8 ×`.ipynb` with output cells               |
