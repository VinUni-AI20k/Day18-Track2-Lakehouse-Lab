# Screenshots / storage-layer evidence

The rubric asks for **one of**:

* MinIO console showing `_delta_log/` + bucket layout (Spark path), **or**
* `tree _lakehouse/` plus the contents of one `_delta_log/*.json` (lightweight path)

This submission ran the **lightweight path**, so the second form applies. The
captures are plain text rather than PNGs on purpose: text is greppable,
diffable in review, and does not lose the byte counts to JPEG artefacts.
`tree(1)` is not installed on this machine, so the listing is rendered by an
equivalent walker (long file lists collapsed to keep it readable).

| File | What it shows |
|---|---|
| [`01_tree_lakehouse.txt`](01_tree_lakehouse.txt) | Full `_lakehouse/` layout — `bronze/`, `silver/`, `gold/` (with `date=…` partition dirs), `iceberg/` warehouses, `blobs/`, `scratch/` (incl. the bonus PoC tables). 107 directories, 1,162 files |
| [`02_delta_log_commit.txt`](02_delta_log_commit.txt) | Both `_delta_log/*.json` commits of the NB1 table, pretty-printed with `schemaString` unpacked: `commitInfo` / `protocol` / `metaData` / `add` with per-column min/max stats, and the second `metaData` that adds `tier` |
| [`03_make_test_run_all.txt`](03_make_test_run_all.txt) | `make smoke` (9 checks), `pytest` (24 passed), `make run-all` (8/8) with timings and platform banner |
| [`04_notebook_pass_blocks.txt`](04_notebook_pass_blocks.txt) | Every notebook's terminal assert block with its measured values, lifted from the executed `.ipynb` output cells |

Regenerate any of these from a clean checkout with `make setup && make data &&
make data-ai && make run-all`.
