# Setup script for Windows PowerShell (equivalent to make setup)
Write-Host "[1/4] Creating virtual environment (.venv)..." -ForegroundColor Cyan
python -m venv .venv

Write-Host "[2/4] Upgrading pip..." -ForegroundColor Cyan
.\.venv\Scripts\python.exe -m pip install --upgrade pip

Write-Host "[3/4] Installing dependencies from requirements.txt..." -ForegroundColor Cyan
.\.venv\Scripts\pip.exe install -r requirements.txt

Write-Host "[4/4] Generating notebooks (.ipynb) from .py files using jupytext..." -ForegroundColor Cyan
.\.venv\Scripts\jupytext.exe --to notebook notebooks/01_delta_basics.py notebooks/02_optimize_zorder.py notebooks/03_time_travel.py notebooks/04_medallion.py notebooks/05_iceberg_catalog.py notebooks/06_maintenance.py notebooks/07_vectors_multimodal.py notebooks/08_agents_provenance.py

Write-Host "`nSetup complete! You can now run:" -ForegroundColor Green
Write-Host "  - Smoke test:    .\.venv\Scripts\python scripts/verify_lite.py" -ForegroundColor Yellow
Write-Host "  - Run pytest:    .\.venv\Scripts\pytest -q" -ForegroundColor Yellow
Write-Host "  - Launch Jupyter: .\.venv\Scripts\jupyter-lab --notebook-dir=notebooks" -ForegroundColor Yellow
