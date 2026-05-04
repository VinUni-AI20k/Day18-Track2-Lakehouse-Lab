# Day 18 Lab Evidence Summary

This file summarizes the executed notebook outputs so the grader can map each
artifact to `rubric.md` quickly.

| Notebook | Rubric evidence captured in executed `.ipynb` |
|---|---|
| NB1 | Delta `_delta_log` JSON listed; bad `age=str` append blocked; `schema_mode="merge"` adds `tier`; DuckDB tier groups = `[('(null)', 3), ('premium', 1)]`. |
| NB2 | Small-file problem reproduced with 200 files; after OPTIMIZE+ZORDER = 55 files; wall-clock speedup = 8.9x; files-pruned ratio = 55.0x. |
| NB3 | MERGE 100K rows = 0.14s; RESTORE = 0.02s; `score < 0` count after restore = 0; final history has 5 versions including RESTORE. |
| NB4 | Bronze = 200,000 rows; Silver = 190,052 rows; dedup/quality drop = 9,948 rows; Gold = 8 dates x 3 models = 24 rows with p50/p95/cost/error_rate checks passed. |

Recommended screenshots:

1. `notebooks/01_delta_basics.ipynb`: Delta log listing + blocked bad write.
2. `notebooks/02_optimize_zorder.ipynb`: `Z-order deliverable metrics` block.
3. `notebooks/03_time_travel.ipynb`: final history block showing 5 versions.
4. `notebooks/04_medallion.ipynb`: `Gold deliverable metrics` block.

Bonus:

- `submission/bonus/ARCHITECTURE.md`: 1B req/day privacy-first LLM observability lakehouse design.
- `submission/bonus/ARCHITECTURE-SLIDES.html`: concise presentation deck adapted from the instructor example structure.
- `submission/bonus/poc/privacy_tokenization_spike.py`: runnable PoC for deterministic PII tokenization, scoped rehydration, and audit logging.
