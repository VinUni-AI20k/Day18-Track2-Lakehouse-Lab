#!/usr/bin/env bash
export PYTHONIOENCODING=utf-8
echo "Updating notebooks from scripts..."
.venv/Scripts/jupytext --to notebook notebooks/01_delta_basics.py notebooks/02_optimize_zorder.py notebooks/03_time_travel.py notebooks/04_medallion.py 2>/dev/null
echo "Starting Jupyter Lab..."
.venv/Scripts/jupyter-lab --notebook-dir=notebooks --ServerApp.token=""
