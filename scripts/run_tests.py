"""Run the grading tests with a disposable, workspace-local temp directory.

Pytest normally derives its temp root from the account that installed Python.
In managed Windows labs that directory can be readable but not writable by the
current process.  A unique local base temp avoids that account-level coupling
and is removed before this process exits.
"""
from __future__ import annotations

import shutil
import sys
import uuid
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    base_temp = ROOT / ".tmp" / f"pytest-{uuid.uuid4().hex}"
    base_temp.parent.mkdir(parents=True, exist_ok=True)
    try:
        return pytest.main(
            ["-q", f"--basetemp={base_temp}", "-p", "no:cacheprovider"]
        )
    finally:
        shutil.rmtree(base_temp, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
