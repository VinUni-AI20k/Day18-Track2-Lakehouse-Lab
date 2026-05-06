"""Tiny helper module for the lightweight (delta-rs + DuckDB) path.

Used by all notebooks/*.py — keeps imports + paths consistent.

The Delta tables on disk are the *same format* Spark/Databricks would write,
so a student can later point Spark at `_lakehouse/silver/llm_calls` and get
the same data. This is the value of an open table format.
"""
from __future__ import annotations

import os
from pathlib import Path

# Repo-local lakehouse — easy to inspect, easy to wipe.
import sys
_root_path = Path(os.environ.get("LAKEHOUSE_ROOT", Path(__file__).resolve().parents[1] / "_lakehouse"))
if sys.platform == "win32" and " " in str(_root_path):
    # Workaround for delta-rs bug on Windows with spaces in path
    ROOT = Path(r"C:\Users\Public\Day18_Lakehouse")
    try:
        ROOT.mkdir(parents=True, exist_ok=True)
        if not _root_path.exists() and not _root_path.is_symlink():
            import subprocess
            subprocess.run(["cmd", "/c", f"mklink /J \"{_root_path}\" \"{ROOT}\""], capture_output=True)
    except Exception:
        pass
else:
    ROOT = _root_path


def path(layer: str, table: str) -> str:
    """Return absolute path to a table inside a medallion layer.

    layer ∈ {"bronze", "silver", "gold", "scratch"}.
    """
    p = ROOT / layer / table
    p.parent.mkdir(parents=True, exist_ok=True)
    return str(p)


def reset(*paths: str) -> None:
    """Delete tables (idempotent rerun support). No-op if missing."""
    import shutil
    for p in paths:
        shutil.rmtree(p, ignore_errors=True)


# ── Convenience: swap to S3 / MinIO with one env var ──
# To target s3://bucket/key instead of local disk, set:
#   LAKEHOUSE_ROOT=s3://my-bucket/lakehouse
#   AWS_* env vars per usual
# delta-rs handles the s3:// scheme natively (no Hadoop, no JVM).
