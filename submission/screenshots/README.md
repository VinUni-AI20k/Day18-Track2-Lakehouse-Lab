# Screenshot checklist — Lightweight path

Use the executed notebooks in `notebooks/*.ipynb` as the primary graded evidence.
If the grader asks for screenshots, capture these cells/terminal views:

1. `01_delta_basics.ipynb`: output showing schema enforcement was blocked and the table includes the `tier` column.
2. `02_optimize_zorder.ipynb`: final "Z-order deliverable metrics" block showing speedup/files-pruned ratio.
3. `03_time_travel.ipynb`: final history after RESTORE plus `Rows with score<0 after restore: 0`.
4. `04_medallion.ipynb`: Bronze/Silver row counts and Gold deliverable metrics.
5. Local filesystem evidence: `_lakehouse/.../_delta_log/*.json` files. See `../EVIDENCE.md` for exact paths.
