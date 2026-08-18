# Day 18 — Track 2 Lakehouse Lab · Submission

Path taken: **lightweight** (`deltalake` 1.6.2 + `pyiceberg` + DuckDB 1.5.5 +
Polars 1.43.2, Python 3.12.3). Fully offline — no Docker, no JVM, no API key.

## Contents

| Path | What it is |
|---|---|
| [`notebooks/`](notebooks/) | The eight notebooks, **executed with output cells preserved**. Identical to `notebooks/*.ipynb` at the repo root, copied here because the root path is `.gitignore`d (Jupytext regenerates it from the `.py` twins). |
| [`RESULTS.md`](RESULTS.md) | Every rubric criterion → the number measured on this machine → what the number means. |
| [`REFLECTION.md`](REFLECTION.md) | 185 words on the anti-pattern our data is most at risk of. |
| [`screenshots/`](screenshots/) | Evidence captures (see below). |
| [`bonus/ARCHITECTURE.md`](bonus/ARCHITECTURE.md) | Bonus challenge, topic C — ride-hailing CDC → lakehouse under Decree 13/2023/NĐ-CP. |
| [`bonus/poc/late_arriving_merge.py`](bonus/poc/late_arriving_merge.py) | 156-line spike proving the hard part of that design: tokenize-on-landing plus a late-arriving MERGE that cannot reopen a settled trip. |

## Evidence

Screenshots were taken in JupyterLab (`make lab`) against the executed
notebooks and a real shell in Jupyter's built-in terminal.

| File | Shows |
|---|---|
| `00_terminal_make_gate.png` | **The grading gate in one frame** — `make smoke` 9/9 · `make test` [100%] · `make run-all` **8/8 in 78.7s** |
| `04_jupyter_nb5_hidden_partition_pruning.png` | NB5 — pruning **10×** filtering on `ts`, and the **$220/day** cost of one forgotten Hive predicate |
| `05_jupyter_nb6_vacuum_blindspot.png` | NB6 — the measured finding: `VACUUM dry-run now finds: 211 files` / `Orphans still on disk: 5` |
| `06_jupyter_nb6_orphan_removal.png` | NB6 — the set-difference orphan algorithm and `Orphans found: 3 (21.2 KB)` |
| `07_terminal_lakehouse_layout.png` | `_lakehouse/` layout — Bronze/Silver/Gold, `_delta_log/`, `agent_version=`, `provenance_bucket=` (all 5 Art. 10 buckets), 8 × `date=` |
| `08_terminal_delta_log_restore_commit.png` | `cat` of a `_delta_log/*.json` — the **RESTORE** commit: `numRemovedFile: 1`, `numRestoredFile: 0` |
| `09_jupyter_nb7_lifecycle_bug.png` | NB7 — erased docs retrievable from lakehouse: **0**; from the stale external index: **8 ← VIOLATION** |

Companion text captures, for grepping and diffing:

| File | Shows |
|---|---|
| `00_make_gate.txt` | Same gate, run from a plain shell (`8/8 in 68.6s`) |
| `01_tree_lakehouse.txt` | Full `tree _lakehouse/` with byte sizes |
| `02_delta_log_v0_commit.json.txt` | A first commit: `protocol` + `metaData` + `add` with column stats |
| `03_delta_log_v4_restore.json.txt` | The RESTORE commit, pretty-printed |
| `10_delta_log_partitioned.json.txt` | `partitionValues` per `add` action on the `agent_version`-partitioned Silver table |

Parquet filenames carry a fresh UUID on every run, so the names in a screenshot
and in a text capture taken at different times will differ — the structure,
the counts and the metrics are what is being evidenced.

## Reproducing

```bash
make setup && make smoke && make data && make data-ai && make test && make run-all
.venv/bin/python submission/bonus/poc/late_arriving_merge.py   # bonus PoC
```

Notebook `.ipynb` files here were produced with
`jupyter nbconvert --to notebook --execute --inplace notebooks/0*.ipynb`,
so their outputs are the same run the tables in `RESULTS.md` cite.
