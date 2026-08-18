# Screenshots

Rubric §Submission item 2 (lightweight path): `tree _lakehouse/` plus the
contents of one `_delta_log/*.json`.

| File | What it shows |
|---|---|
| `01-lakehouse-tree-and-delta-log.png` | `tree -L 2 _lakehouse/` — Bronze / Silver / Gold / Iceberg catalogs on disk — then `ls` of a `_delta_log/` and the head of commit `00000000000000000000.json` |
| `02-grading-gates-green.png` | `make smoke` (9 checks), `make test` (24 passed), `make run-all` (8/8 in 18.9s) |
| `03-vacuum-and-expiry-findings.png` | NB6's two counter-intuitive measurements, verbatim from the executed notebook |

Each PNG is the captured stdout of the command shown in its prompt line,
typeset in a terminal style rather than grabbed from the window server — the
text is byte-for-byte what the command printed. `03` is copied verbatim from
the output cells of [`../notebooks/06_maintenance.ipynb`](../notebooks/06_maintenance.ipynb),
so every line can be checked against the notebook itself.

The untruncated transaction log and the full directory tree are in
[`EVIDENCE.md`](EVIDENCE.md).
