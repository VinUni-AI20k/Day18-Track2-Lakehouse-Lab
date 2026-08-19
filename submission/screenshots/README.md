# Evidence notes

Lab was run from WSL2 with the repo on a Windows drive (`/mnt/c`).
`delta-rs` requires atomic rename, which drvfs does not support — the first
`make data` failed with `DeltaError: Generic LocalFileSystem error: Upload aborted`.

Fix: `export LAKEHOUSE_ROOT="$HOME/day18-lakehouse"` (supported by
`scripts/lakehouse.py`), which puts the lakehouse on ext4 inside WSL.
That is why the tree screenshot shows `~/day18-lakehouse/` rather than
`./_lakehouse/` — same layout, same Delta format.

- `tree.png`      — lakehouse layout incl. `_delta_log/`
- `delta_log.png` — contents of `scratch/users_delta/_delta_log/000...0.json`
- `make_test.png` — 24 tests green
- `run_all.png`   — 8/8 notebooks pass
