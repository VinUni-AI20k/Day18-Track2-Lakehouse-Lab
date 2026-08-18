# Day 18 Lightweight Lab Results

Environment: Python 3.13.9, `deltalake` 1.6.2, `pyiceberg` 0.11.1, DuckDB 1.5.5. Spark and Docker were not used.

## Verification

- `make smoke`: 9/9 checks passed.
- `make test`: 24/24 tests passed.
- `make run-all`: 8/8 notebooks passed in 61.9 seconds.
- All eight submitted `.ipynb` files were executed in place and retain cell output.

## Measured evidence

| Notebook | Result |
|---|---|
| NB1 | Delta JSON commits present; bad schema blocked; `tier` added via opt-in evolution |
| NB2 | 8.5× wall-clock speedup; 55× file-pruning ratio |
| NB3 | At least five history versions; MERGE and RESTORE recorded; zero negative scores after restore |
| NB4 | Bronze 200,000 → Silver 190,052; Gold covers 8 dates × 3 models |
| NB5 | Hidden-partition pruning 10×; 10 snapshots; partition specs `[1, 2]` coexist |
| NB6 | Compaction 200 → 11 files; 90% skip rate; 3 planted Delta orphans removed; Iceberg 20 → 3 snapshots |
| NB7 | int8 storage 5.8× smaller; recall@10 0.904; topic fidelity 1.000; stale index retained 8 erased docs |
| NB8 | 5 turns caused 1 catalog read; all four Art. 10 buckets present; subject rows 8 → 0 |

The filesystem and Delta transaction-log evidence is captured in `screenshots/lakehouse_evidence.png`.
