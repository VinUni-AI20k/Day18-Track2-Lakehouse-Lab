# Lab Verification — automated run

I ran the lightweight path notebooks end-to-end using `.venv/bin/python notebooks/*.py`.

Summary of rubric checks (all passed):

- NB1 `01_delta_basics`:
  - Delta table `_lakehouse/bronze/llm_calls_raw` created; `_delta_log/00000000000000000000.json` present.
  - Schema enforcement blocked bad write as expected.
  - `schema_mode="merge"` added `tier` column.

- NB2 `02_optimize_zorder`:
  - Pre-OPTIMIZE files: 200
  - Post-OPTIMIZE files: 55
  - Speedup: 10.7× (target ≥ 3×)
  - Files-pruned ratio: 55.0× (target ≥ 10×)

- NB3 `03_time_travel`:
  - `history()` shows 5 versions (including RESTORE).
  - MERGE 100K rows: 0.12s (target < 60s)
  - RESTORE: 0.00s (target < 30s). Rows with `score < 0` after restore: 0.

- NB4 `04_medallion`:
  - Bronze rows: 200,000
  - Silver rows: 190,052 (dedup reduced rows)
  - Gold aggregations computed for 8 distinct dates and 3 models (target ≥ 7 dates × 3 models).

Repro steps (lightweight):

```bash
make setup
make smoke
.venv/bin/python notebooks/01_delta_basics.py
.venv/bin/python notebooks/02_optimize_zorder.py
.venv/bin/python notebooks/03_time_travel.py
.venv/bin/python notebooks/04_medallion.py
```

Key artifacts saved in this repo under `submission/`:

- `VERIFICATION.md` (this file)
- `_lakehouse/` — the on-disk Delta tables and `_delta_log/` files
