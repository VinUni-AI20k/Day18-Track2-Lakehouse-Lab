"""Execute all 8 Jupyter notebooks in-place, preserving full cell outputs."""
import os
import sys
import time
from pathlib import Path
import nbformat
from nbclient import NotebookClient

# Ensure UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOKS_DIR = ROOT / "notebooks"

notebook_files = [
    "01_delta_basics.ipynb",
    "02_optimize_zorder.ipynb",
    "03_time_travel.ipynb",
    "04_medallion.ipynb",
    "05_iceberg_catalog.ipynb",
    "06_maintenance.ipynb",
    "07_vectors_multimodal.ipynb",
    "08_agents_provenance.ipynb",
]

def main():
    print("=" * 60)
    print("Executing 8 Notebooks with output preservation")
    print("=" * 60)
    
    start_total = time.time()
    for nb_name in notebook_files:
        nb_path = NOTEBOOKS_DIR / nb_name
        print(f"\n▶ Executing {nb_name} ...", flush=True)
        t0 = time.time()
        
        # Read notebook
        nb = nbformat.read(nb_path, as_version=4)
        
        # Execute notebook in cwd = NOTEBOOKS_DIR (same as Jupyter Lab)
        client = NotebookClient(nb, timeout=600, kernel_name="python3", resources={"metadata": {"path": str(NOTEBOOKS_DIR)}})
        client.execute()
        
        # Write back executed notebook with outputs
        nbformat.write(nb, nb_path)
        dur = time.time() - t0
        print(f"  ✓ {nb_name} executed & saved ({dur:.1f}s)", flush=True)
        
    total_dur = time.time() - start_total
    print("\n" + "=" * 60)
    print(f"✓ All 8 notebooks executed successfully with outputs saved! Total time: {total_dur:.1f}s")
    print("=" * 60)

if __name__ == "__main__":
    main()
