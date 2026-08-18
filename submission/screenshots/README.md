# Storage-layer evidence — lightweight path

`rubric.md` accepts either of two proofs that the tables are real table
formats. This submission takes the second one:

> * MinIO console showing `_delta_log/` + bucket layout (Spark path), **or**
> * `tree _lakehouse/` plus the contents of one `_delta_log/*.json` (lightweight path)

The lab ran on the lightweight path (`deltalake` + `pyiceberg`, no JVM, no
Docker, no MinIO), so there is no object-store console to screenshot — the
lakehouse is a local directory tree. These two text captures are that tree and
that commit log, taken from the same run that produced the executed notebooks.

| File | What it shows |
|---|---|
| `01-tree-lakehouse.txt` | Full `_lakehouse/` layout: bronze / silver / gold medallion, Hive-style `date=` partition dirs under Gold, `_delta_log/` beside every Delta table, and the Iceberg catalog's `metadata/` tree. Bulk parquet is collapsed to counts; metadata is listed in full. |
| `02-delta-log-commit.txt` | The five commits of `scratch/customers_tt` (NB3) with the action list of each, then two commits printed in full: the `MERGE` (v2) with its `operationMetrics`, and the `RESTORE` (v4). |

The `RESTORE` commit is the one worth reading: it contains `remove` and
`metaData` actions and **no `add` of new data**. Rolling back 100K rows cost
one JSON file, because the old parquet files were never mutated in the first
place — that is the whole argument for an open table format over "just
parquet in a bucket".
