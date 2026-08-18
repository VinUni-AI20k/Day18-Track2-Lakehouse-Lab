#!/usr/bin/env bash
# One-shot setup for Day 18 Lakehouse Lab (lightweight path — no Docker, no JVM).
# Equivalent to `make setup && make smoke` for users without `make`.
set -euo pipefail

cd "$(dirname "$0")"

VENV=.venv

echo "[1/3] Creating venv + installing deps…"
if command -v uv >/dev/null 2>&1; then
  uv venv "$VENV" --python '>=3.10,<3.15'
else
  python3 -m venv "$VENV"
fi

if [ -f "$VENV/bin/python" ]; then
  PY="$VENV/bin/python"
else
  PY="$VENV/Scripts/python.exe"
fi

"$PY" -c 'import sys; raise SystemExit(0 if (3,10)<=sys.version_info[:2]<(3,15) else 1)' \
  || { echo "ERROR: need Python 3.10-3.14. Install 'uv' (auto-fetches one) or run: python3.12 -m venv .venv" >&2; exit 1; }

if command -v uv >/dev/null 2>&1; then
  uv pip install --python "$PY" -r requirements.txt
else
  "$PY" -m pip install -q -r requirements.txt
fi

echo "[2/3] Converting notebooks (jupytext)…"
if [ -f "$VENV/bin/jupytext" ]; then
  JUPYTEXT="$VENV/bin/jupytext"
else
  JUPYTEXT="$VENV/Scripts/jupytext.exe"
fi
"$JUPYTEXT" --to notebook --update notebooks/*.py 2>/dev/null || "$JUPYTEXT" --to notebook notebooks/*.py

echo "[3/3] Running smoke test…"
PYTHONIOENCODING=utf-8 "$PY" scripts/verify_lite.py

cat <<EOF

  Lab is ready.
  Next:
    make data && make data-ai && make lab
  (or without make: run "$PY" scripts/generate_data_lite.py, then scripts/generate_ai_data.py,
   then start Jupyter Lab from the venv pointing at notebooks/)

EOF
