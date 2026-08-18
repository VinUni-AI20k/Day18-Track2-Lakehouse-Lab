"""Execute all notebooks in-place and preserve outputs in notebooks/*.ipynb."""
from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"


def main():
    # 1. Generate data
    print("Generating Bronze and AI data...")
    subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_data_lite.py")], check=True)
    subprocess.run([sys.executable, str(ROOT / "scripts" / "generate_ai_data.py")], check=True)

    # 2. Convert .py to .ipynb using jupytext
    print("\nSyncing .py to .ipynb with jupytext...")
    py_files = sorted(NB_DIR.glob("[0-9]*.py"))
    for py in py_files:
        subprocess.run([sys.executable, "-m", "jupytext", "--to", "notebook", "--update", str(py)], check=True)

    # 3. Execute each notebook in order
    print("\nExecuting notebooks with ExecutePreprocessor...")
    ipynb_files = sorted(NB_DIR.glob("[0-9]*.ipynb"))
    for ipynb_path in ipynb_files:
        print(f"--> Executing {ipynb_path.name}...")
        t0 = time.perf_counter()
        nb = nbformat.read(str(ipynb_path), as_version=4)
        ep = ExecutePreprocessor(timeout=900, kernel_name="python3")
        ep.preprocess(nb, {"metadata": {"path": str(NB_DIR)}})
        nbformat.write(nb, str(ipynb_path))
        dt = time.perf_counter() - t0
        print(f"    ✓ {ipynb_path.name} done in {dt:.1f}s")

    print("\nAll notebooks executed successfully and saved with output cells!")


if __name__ == "__main__":
    main()
