# Submission — Day 18 Lakehouse Lab (Track 2)

**Student:** Lê Nguyễn Minh Quang · **ID:** 2A202601248
**Path:** lightweight (`deltalake` + `pyiceberg` + DuckDB + Polars) — no Docker, no JVM, fully offline
**Environment:** Python 3.12.13 · macOS 26.5.2 (arm64) · executed 2026-08-18

## What's here

| Item | Where | Rubric |
|---|---|---|
| Eight executed notebooks, output cells preserved | [`../notebooks/*.ipynb`](../notebooks/) | Submission item 1 |
| Storage-layer evidence (`tree` + `_delta_log/*.json`) | [`screenshots/`](screenshots/) | Submission item 2 |
| Reflection (≤ 200 words) | [`REFLECTION.md`](REFLECTION.md) | Submission item 3 |
| Bonus architecture brief + PoC | [`bonus/`](bonus/) | Submission item 4 (ungraded) |
| Criterion-by-criterion measured values | [`EVIDENCE.md`](EVIDENCE.md) | — |

`EVIDENCE.md` is the one to read first: it maps every rubric row to the number
this run actually produced *and* the reading of that number, which is what the
rubric's top band asks for.

## Verification

```
make smoke      →  9/9 checks
pytest          →  24 passed in 0.70s
make run-all    →  8/8 notebooks passed in 10.7s
```

Full transcript: [`screenshots/03_make_test_run_all.txt`](screenshots/03_make_test_run_all.txt).

To reproduce from a clean checkout:

```bash
make setup && make smoke && make data && make data-ai && make test && make run-all
```

## Note on a change to lab source

`notebooks/04_medallion.py` gained a pass-criteria block at the end. The other
seven notebooks each end in a printed checklist plus an `assert`; NB4 asserted
only its date count, leaving two of its three rubric criteria (all three layers
on disk, Silver < Bronze) unverified by `make run-all`. The added cell is purely
additive — it checks the layers exist, that Silver < Bronze, the 7×3 Gold shape,
and that `cost_usd`/`error_rate`/`p50 ≤ p95` are sane. No existing cell was
modified.
