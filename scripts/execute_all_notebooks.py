"""Execute all .ipynb notebooks and save executed outputs in-place."""
import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"

def main():
    notebooks = sorted(p for p in NB_DIR.glob("*.ipynb") if not p.name.startswith("_"))
    print(f"Executing {len(notebooks)} .ipynb notebooks using dotvenv kernel...\n", flush=True)
    
    env = {**os.environ, "PYTHONIOENCODING": "utf-8", "PYTHONUTF8": "1"}
    
    for nb in notebooks:
        t0 = time.perf_counter()
        cmd = [
            sys.executable,
            "-m",
            "jupyter",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            "--inplace",
            "--ExecutePreprocessor.kernel_name=dotvenv",
            "--ExecutePreprocessor.timeout=180",
            str(nb),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)
        dt = time.perf_counter() - t0
        if proc.returncode == 0:
            print(f"  PASS  {nb.name:<32} {dt:6.1f}s", flush=True)
        else:
            print(f"  FAIL  {nb.name:<32} {dt:6.1f}s", flush=True)
            print("STDOUT:", proc.stdout[-1000:], flush=True)
            print("STDERR:", proc.stderr[-1000:], flush=True)
            return 1
    print("\nAll .ipynb notebooks executed and outputs preserved successfully!", flush=True)
    return 0

if __name__ == "__main__":
    sys.exit(main())
