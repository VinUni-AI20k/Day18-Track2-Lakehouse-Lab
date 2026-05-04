# Submission Checklist

1. Run the lightweight workflow on Windows:
   `powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 setup`
   `powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 smoke`
   `powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 data`
   `powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 lab`

2. Execute notebooks in order:
   `01_delta_basics.ipynb`
   `02_optimize_zorder.ipynb`
   `03_time_travel.ipynb`
   `04_medallion.ipynb`

3. Capture evidence for grading:
   NB1: `_delta_log` exists, schema-enforcement error, `tier` column added.
   NB2: `Speedup >= 3x` or `Files-pruned ratio >= 10x`, plus file count before/after.
   NB3: `MERGE 100K` runtime, `RESTORE` runtime, `Rows with score<0 after restore: 0`, `Total versions >= 5`.
   NB4: Bronze/Silver/Gold tables exist, `Silver rows < Bronze rows`, `Distinct dates >= 7`, 3 models in Gold.

4. Save screenshots under `submission/screenshots/`.
   Recommended evidence set:
   `nb1-01-history-transaction-log.png`
   `nb1-02-schema-enforcement-blocked.png`
   `nb1-03-schema-evolution-tier-column.png`
   `nb1-04-delta-log-on-disk.png`
   `nb2-01-optimize-file-reduction-speedup.png`
   `nb2-02-zorder-deliverable-metrics.png`
   `nb3-01-history-after-restore.png`
   `nb3-02-merge-100k-runtime.png`
   `nb3-03-restore-bad-rows-removed.png`
   `nb4-01-silver-dedup-row-drop.png`
   `nb4-02-gold-deliverable-metrics.png`
   `nb4-03-bronze-silver-gold-on-disk.png`

5. Complete `submission/REFLECTION.md` in 200 words or fewer.
