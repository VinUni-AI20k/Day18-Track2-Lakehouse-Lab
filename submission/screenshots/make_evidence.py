"""Generate evidence files for submission/screenshots/.

Per rubric §Submission: lightweight path needs
  1. tree of _lakehouse/
  2. contents of one _delta_log/*.json

Also capture key notebook output numbers (the things the grader actually reads).
"""
from __future__ import annotations

import datetime as dtm
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "scripts"))

from lakehouse import ROOT as LH  # noqa: E402
from deltalake import DeltaTable  # noqa: E402

OUT = ROOT / "submission" / "screenshots"
OUT.mkdir(parents=True, exist_ok=True)


def tree(target: Path, prefix: str = "") -> list[str]:
    """Compact text-art tree, mirroring `tree -L N --noreport` output."""
    lines: list[str] = []
    entries = sorted(target.iterdir(), key=lambda p: (not p.is_dir(), p.name.lower()))
    for i, entry in enumerate(entries):
        last = i == len(entries) - 1
        connector = "└── " if last else "├── "
        if entry.is_dir():
            sub_count = sum(1 for _ in entry.rglob("*") if _.is_file())
            lines.append(f"{prefix}{connector}{entry.name}/  ({sub_count} files)")
            extension = "    " if last else "│   "
            lines.extend(tree(entry, prefix + extension))
        else:
            size = entry.stat().st_size
            lines.append(f"{prefix}{connector}{entry.name}  ({size:,} B)")
    return lines


def write_lakehouse_tree() -> None:
    body = ["$ tree _lakehouse/", ""]
    if LH.exists():
        body.append(f"_lakehouse/  ({sum(1 for _ in LH.rglob('*') if _.is_file())} files)")
        body.extend(tree(LH))
    else:
        body.append("(empty)")
    (OUT / "01_lakehouse_tree.txt").write_text("\n".join(body) + "\n", encoding="utf-8")


def write_delta_log_excerpt() -> None:
    """First commit JSON of NB1's table — proves the ACID transaction log."""
    target = LH / "scratch" / "users_delta" / "_delta_log"
    json_files = sorted(target.glob("*.json")) if target.exists() else []
    body = ["# _delta_log/00000000000000000000.json  (NB1 first commit)", ""]
    if not json_files:
        body.append("(no log yet — run NB1 first)")
    else:
        first = json_files[0]
        body.append(f"Path: _lakehouse/scratch/users_delta/_delta_log/{first.name}")
        body.append(f"Bytes: {first.stat().st_size:,}")
        body.append("")
        body.append("```json")
        body.append(first.read_text(encoding="utf-8"))
        body.append("```")
    (OUT / "02_delta_log_first_commit.json.txt").write_text("\n".join(body) + "\n", encoding="utf-8")


def write_table_evidence() -> None:
    """Per-table facts the grader reads."""
    rows: list[tuple[str, str, str, str]] = []
    targets = [
        ("scratch.users_delta",            LH / "scratch" / "users_delta",            "NB1"),
        ("scratch.events_smallfiles",      LH / "scratch" / "events_smallfiles",      "NB2"),
        ("scratch.customers_tt",           LH / "scratch" / "customers_tt",           "NB3"),
        ("bronze.llm_calls_raw",           LH / "bronze" / "llm_calls_raw",           "NB4-Bronze"),
        ("silver.llm_calls",               LH / "silver" / "llm_calls",               "NB4-Silver"),
        ("gold.llm_daily_metrics",         LH / "gold"   / "llm_daily_metrics",       "NB4-Gold"),
        ("scratch.maint_events",           LH / "scratch" / "maint_events",           "NB6-Delta"),
        ("bronze.docs_multimodal",         LH / "bronze" / "docs_multimodal",         "NB7/NB8"),
        ("bronze.agent_traces",            LH / "bronze" / "agent_traces",            "NB8"),
        ("silver.agent_trajectories",      LH / "silver" / "agent_trajectories",      "NB8-Silver"),
        ("silver.training_corpus_governed",LH / "silver" / "training_corpus_governed","NB8-Governed"),
    ]
    for name, p, nb in targets:
        if not p.exists():
            rows.append((name, "(missing)", "(missing)", nb))
            continue
        dt = DeltaTable(str(p))
        versions = len(dt.history())
        size = sum(f.stat().st_size for f in p.rglob("*") if f.is_file())
        rows.append((name, str(dt.count()), f"{size:,} B / v{versions}", nb))
    body = ["# Table inventory (post run-all)", "",
            f"Generated: {dtm.datetime.now().isoformat(timespec='seconds')}", "",
            f"{'table':<38} {'rows':>10}   {'disk / versions':<28} nb"]
    for name, n, info, nb in rows:
        body.append(f"{name:<38} {n:>10}   {info:<28} {nb}")
    (OUT / "03_table_inventory.txt").write_text("\n".join(body) + "\n", encoding="utf-8")


def write_iceberg_inventory() -> None:
    iceberg_root = LH / "iceberg"
    body = ["# Iceberg catalogs created during NB5 / NB6 / NB8", ""]
    if not iceberg_root.exists():
        body.append("(none)")
    else:
        for cat_dir in sorted(iceberg_root.iterdir()):
            if not cat_dir.is_dir():
                continue
            wh = cat_dir / "warehouse"
            tables = list(wh.rglob("metadata/version-hint.text")) if wh.exists() else []
            body.append(f"catalog={cat_dir.name}/  tables={len(tables)}")
            for t in tables:
                rel = t.relative_to(wh.parent.parent)
                body.append(f"  • {rel}")
    (OUT / "04_iceberg_inventory.txt").write_text("\n".join(body) + "\n", encoding="utf-8")


def write_notebook_asserts() -> None:
    """Concatenate the final `assert` block of each notebook .py source.

    The grader's mechanical gate is these lines.
    """
    body = ["# Final `assert` block of each notebook — the rubric's mechanical gate", ""]
    for nb_path in sorted((ROOT / "notebooks").glob("[0-9]*.py")):
        text = nb_path.read_text(encoding="utf-8")
        marker = "checks = {"
        idx = text.find(marker)
        if idx < 0:
            continue
        tail = text[idx: idx + 1200]
        # truncate to closing brace
        end = tail.find("}\n")
        if end > 0:
            tail = tail[: end + 1]
        body.append(f"=== {nb_path.name} ===")
        body.append(tail.rstrip())
        body.append("")
    (OUT / "05_notebook_pass_criteria.txt").write_text("\n".join(body) + "\n", encoding="utf-8")


if __name__ == "__main__":
    write_lakehouse_tree()
    write_delta_log_excerpt()
    write_table_evidence()
    write_iceberg_inventory()
    write_notebook_asserts()
    for f in sorted(OUT.iterdir()):
        print(f"  wrote  {f.relative_to(ROOT)}  ({f.stat().st_size:,} B)")