"""Regenerate the storage-layer evidence for rubric Submission #2.

Run from the repo root, after `make run-all` has populated `_lakehouse/`:

    python submission/screenshots/make_evidence.py

Writes two files next to this script:
  01_tree_lakehouse.txt      — `tree _lakehouse/`
  02_delta_log_contents.txt  — every action in every commit of one Delta table
"""
import io
import json
from pathlib import Path

ROOT = Path("_lakehouse")
OUT = Path(__file__).parent
TABLE = ROOT / "scratch" / "users_delta"   # the table NB1 builds


def write_tree() -> None:
    buf = io.StringIO()
    buf.write("$ tree _lakehouse/\n_lakehouse/\n")

    def walk(d: Path, prefix: str = "", depth: int = 0):
        entries = sorted(d.iterdir(), key=lambda p: (p.is_file(), p.name))
        dirs = [e for e in entries if e.is_dir()]
        files = [e for e in entries if e.is_file()]
        # Leaf dirs hold hundreds of parquet/avro parts — summarise instead of
        # dumping them, otherwise the tree is unreadable as evidence.
        if depth >= 3 and len(files) > 6:
            size = sum(f.stat().st_size for f in files) / 1024
            buf.write(f"{prefix}└── ... {len(files)} files ({size:.1f} KB)\n")
            return
        truncated = len(files) > 12
        shown = dirs + (files[:6] if truncated else files)
        for i, e in enumerate(shown):
            last = i == len(shown) - 1 and not truncated
            conn = "└── " if last else "├── "
            if e.is_dir():
                buf.write(f"{prefix}{conn}{e.name}/\n")
                walk(e, prefix + ("    " if last else "│   "), depth + 1)
            else:
                buf.write(f"{prefix}{conn}{e.name}  ({e.stat().st_size:,} B)\n")
        if truncated:
            rest = files[6:]
            size = sum(f.stat().st_size for f in rest) / 1024
            buf.write(f"{prefix}└── ... +{len(rest)} more files ({size:.1f} KB)\n")

    walk(ROOT)

    n_dirs = sum(1 for p in ROOT.rglob("*") if p.is_dir())
    n_files = sum(1 for p in ROOT.rglob("*") if p.is_file())
    total = sum(f.stat().st_size for f in ROOT.rglob("*") if f.is_file())
    buf.write(f"\n{n_dirs} directories, {n_files} files, "
              f"{total / 1024 / 1024:.1f} MB total\n")

    (OUT / "01_tree_lakehouse.txt").write_text(buf.getvalue(), encoding="utf-8")
    print(f"01_tree_lakehouse.txt      {n_dirs} dirs, {n_files} files, "
          f"{total / 1024 / 1024:.1f} MB")


def write_delta_log() -> None:
    logs = sorted((TABLE / "_delta_log").glob("*.json"))
    buf = io.StringIO()
    buf.write("Evidence 2 — Delta transaction log contents (lightweight path)\n")
    buf.write(f"Table: {TABLE.as_posix()}\n")
    buf.write("Produced by NB1 (01_delta_basics): "
              "v0 = initial write, v1 = schema evolution append.\n")
    buf.write("=" * 78 + "\n")
    for p in logs:
        buf.write(f"\n$ cat {p.as_posix()}\n")
        buf.write("-" * 78 + "\n")
        # One JSON object per line, each keyed by its action name.
        for line in p.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            rec = json.loads(line)
            action = next(iter(rec))
            buf.write(f"\n[{action}]\n")
            buf.write(json.dumps(rec[action], indent=2, ensure_ascii=False) + "\n")

    (OUT / "02_delta_log_contents.txt").write_text(buf.getvalue(), encoding="utf-8")
    print(f"02_delta_log_contents.txt  {len(logs)} commit files")


if __name__ == "__main__":
    if not ROOT.exists():
        raise SystemExit("_lakehouse/ not found — run `make run-all` from the repo root first.")
    write_tree()
    write_delta_log()
