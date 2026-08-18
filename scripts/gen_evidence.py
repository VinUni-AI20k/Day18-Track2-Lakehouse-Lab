"""Generate submission evidence: a _lakehouse tree + one _delta_log JSON sample.

Run with the lab venv python. Outputs go to submission/screenshots/.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from lakehouse import ROOT  # noqa: E402

OUT = Path(__file__).resolve().parents[1] / "submission" / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)

DEPTH = 4


def tree_lines(base: Path, prefix: str = "", depth: int = 0) -> list[str]:
    lines = []
    entries = sorted(base.iterdir(), key=lambda p: (p.is_file(), p.name.lower()))
    dirs = [p for p in entries if p.is_dir()]
    files = [p for p in entries if p.is_file()]
    shown = dirs[:12] + files[:8]
    hidden = len(entries) - len(shown)
    for i, p in enumerate(shown):
        last = i == len(shown) - 1 and hidden == 0
        branch = "`-- " if last else "|-- "
        if p.is_dir():
            lines.append(f"{prefix}{branch}{p.name}/")
            if depth < DEPTH:
                lines.extend(tree_lines(p, prefix + ("    " if last else "|   "), depth + 1))
        else:
            size = p.stat().st_size
            lines.append(f"{prefix}{branch}{p.name}  ({size:,} B)")
    if hidden:
        lines.append(f"{prefix}`-- ... {hidden} more entries")
    return lines


def main() -> None:
    tree = ["_lakehouse/"] + tree_lines(ROOT)
    (OUT / "lakehouse_tree.txt").write_text("\n".join(tree), encoding="utf-8")
    print("Wrote", OUT / "lakehouse_tree.txt")

    log_candidates = sorted((ROOT / "bronze" / "llm_calls_raw" / "_delta_log").glob("*.json"))
    if log_candidates:
        sample = log_candidates[-1]
        lines = sample.read_text(encoding="utf-8").strip().splitlines()
        pretty = "\n\n".join(json.dumps(json.loads(ln), indent=2) for ln in lines[:3])
        header = (
            f"# Delta transaction log sample\n"
            f"# File: {sample.relative_to(ROOT.parent)}\n"
            f"# First {min(3, len(lines))} commit action(s) shown (each line = one atomic commit).\n\n"
        )
        (OUT / "delta_log_sample.txt").write_text(header + pretty, encoding="utf-8")
        print("Wrote", OUT / "delta_log_sample.txt")
    else:
        print("WARNING: no _delta_log/*.json found under bronze/llm_calls_raw")

    print("\nLayout summary:")
    for layer in ("bronze", "silver", "gold", "scratch", "iceberg"):
        d = ROOT / layer
        if d.exists():
            print(f"  {layer}: {len(list(d.iterdir()))} table(s)")


if __name__ == "__main__":
    main()
