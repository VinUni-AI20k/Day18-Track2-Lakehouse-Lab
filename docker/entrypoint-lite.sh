#!/usr/bin/env bash
# Bring the lab up: build .venv on first run, then serve JupyterLab.
#
# This lives in the image, not in `command:` of the compose file, on purpose.
# Compose interpolates $ inside command blocks — including inside comments —
# which is exactly what broke `make spark-up` for the Spark path. A script
# file is passed through untouched.
set -euo pipefail

cd /workspace

if [ ! -x .venv/bin/python ]; then
  echo ">> First run: building .venv from requirements.txt (~1-2 min)…"
  make setup
else
  echo ">> .venv present — skipping setup."
  # Pick up any .py notebook edits made on the host since last boot.
  .venv/bin/jupytext --to notebook --update notebooks/*.py >/dev/null 2>&1 || true
fi

echo ""
echo "  JupyterLab  → http://localhost:8888   (token: ${JUPYTER_TOKEN})"
echo "  Notebooks   → notebooks/01_delta_basics.ipynb … 08_agents_provenance.ipynb"
echo ""

# --allow-root: the container runs as root so that files written into the
# bind mount stay writable from Windows.
exec .venv/bin/jupyter lab \
  --ip=0.0.0.0 --port=8888 --no-browser --allow-root \
  --ServerApp.root_dir=/workspace \
  --IdentityProvider.token="${JUPYTER_TOKEN}"
