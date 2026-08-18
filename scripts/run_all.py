"""Execute every notebook headlessly, in order. `make run-all`.

Each notebook ends in its own `assert` block over its pass criteria, so a
non-zero exit here means a criterion actually failed — this is the same gate
the instructor runs before grading.
"""
from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
NB_DIR = ROOT / "notebooks"


def main() -> int:
    notebooks = sorted(p for p in NB_DIR.glob("*.ipynb") if not p.name.startswith("_") and not p.name.endswith(".nbconvert.ipynb"))
    if not notebooks:
        print(f"No notebooks found in {NB_DIR}")
        return 1

    print(f"Running {len(notebooks)} notebooks with {sys.executable}\n")
    failures, total = [], 0.0

    for nb in notebooks:
        t0 = time.perf_counter()
        # Chạy notebook không ghi đè và tính toán từ thư mục ROOT hoặc thư mục chứa notebook
        cmd = [
            sys.executable,
            "-m",
            "jupyter",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            "--inplace", # hoặc bỏ --inplace nếu không muốn lưu output vào file gốc
            "--ExecutePreprocessor.timeout=600",
            str(nb),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True, cwd=ROOT)
        dt = time.perf_counter() - t0
        total += dt

        if proc.returncode == 0:
            print(f"  PASS  {nb.name:<32} {dt:6.1f}s")
        else:
            print(f"  FAIL  {nb.name:<32} {dt:6.1f}s")
            failures.append((nb.name, proc.stdout[-1500:], proc.stderr[-1500:]))

    print(f"\n{len(notebooks) - len(failures)}/{len(notebooks)} passed in {total:.1f}s")
    for name, out, err in failures:
        print(f"\n{'=' * 70}\n{name}\n{'=' * 70}")
        if out.strip():
            print(f"[STDOUT]\n{out}")
        if err.strip():
            print(f"[STDERR]\n{err}")

    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())