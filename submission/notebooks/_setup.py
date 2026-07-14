"""Path bootstrap for the lightweight notebooks.

Resolves `scripts/lakehouse.py` from the repo root regardless of where
Jupyter / Python was launched from. Used by all NB*/lite notebooks:

    import _setup  # noqa: F401  -- adds scripts/ to sys.path
    from lakehouse import path, reset

Why: the prior pattern `sys.path.insert(0, "../scripts")` is *cwd-relative*
and silently breaks if the notebook is run from the repo root or a CI
runner. In Jupyter, `__file__` may be undefined, so we fall back to a
cwd-based search for the repo root.
"""
from __future__ import annotations

import sys
from pathlib import Path

def _find_repo_root() -> Path:
    here = Path(__file__).resolve().parent if "__file__" in globals() else Path.cwd()
    candidates = [here, *here.parents]
    for candidate in candidates:
        if (candidate / "scripts" / "lakehouse.py").exists():
            return candidate
    raise RuntimeError("Could not locate scripts/lakehouse.py from notebook context")


_HERE = _find_repo_root() / "notebooks"
_DOCKER = Path("/workspace/scripts")
_LOCAL = _HERE.parent / "scripts"

_TARGET = _DOCKER if _DOCKER.exists() else _LOCAL
sys.path.insert(0, str(_TARGET))
