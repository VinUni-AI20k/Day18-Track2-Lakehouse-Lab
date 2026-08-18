"""Execute all notebooks and save outputs in-place into .ipynb files for submission."""
from __future__ import annotations

import sys
import time
from pathlib import Path
import nbformat
from nbconvert.preprocessors import ExecutePreprocessor

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"

def main() -> int:
    notebooks = sorted(p for p in NB_DIR.glob("*.ipynb") if not p.name.startswith("_"))
    if not notebooks:
        print("No .ipynb notebooks found.")
        return 1

    print(f"Executing {len(notebooks)} notebooks and saving outputs...\n")
    ep = ExecutePreprocessor(timeout=600, kernel_name="python3")
    failures = []
    
    for nb_path in notebooks:
        print(f"Running {nb_path.name}...")
        t0 = time.perf_counter()
        try:
            with open(nb_path, "r", encoding="utf-8") as f:
                nb = nbformat.read(f, as_version=4)
            
            ep.preprocess(nb, {"metadata": {"path": str(NB_DIR)}})
            
            with open(nb_path, "w", encoding="utf-8") as f:
                nbformat.write(nb, f)
            
            dt = time.perf_counter() - t0
            print(f"  PASS  {nb_path.name:<32} {dt:6.1f}s")
        except Exception as e:
            dt = time.perf_counter() - t0
            print(f"  FAIL  {nb_path.name:<32} {dt:6.1f}s")
            print(f"  Error: {e}")
            failures.append((nb_path.name, str(e)))

    if failures:
        print(f"\n{len(failures)} notebooks failed execution.")
        return 1
    
    print("\nAll 8 notebooks executed and outputs preserved successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(main())
