param(
    [ValidateSet("setup", "smoke", "data", "lab", "clean", "all")]
    [string]$Action = "setup"
)

$ErrorActionPreference = "Stop"

$RepoRoot = Split-Path -Parent $PSScriptRoot
$VenvDir = Join-Path $RepoRoot ".venv"
$PythonExe = Join-Path $VenvDir "Scripts\python.exe"
$PipExe = Join-Path $VenvDir "Scripts\pip.exe"
$NotebookDir = Join-Path $RepoRoot "notebooks"

function Assert-Venv {
    if (-not (Test-Path $PythonExe)) {
        throw "Virtual environment not found. Run: powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 setup"
    }
}

function Convert-Notebooks {
    Assert-Venv
    Get-ChildItem -Path $NotebookDir -Filter *.py | ForEach-Object {
        & $PythonExe -m jupytext --to notebook --update $_.FullName
    }
}

function Invoke-Setup {
    if (-not (Test-Path $VenvDir)) {
        if (Get-Command py -ErrorAction SilentlyContinue) {
            & py -m venv $VenvDir
        } else {
            & python -m venv $VenvDir
        }
    }

    & $PythonExe -m pip install --upgrade pip
    & $PipExe install -r (Join-Path $RepoRoot "requirements.txt")
    Convert-Notebooks
    Write-Host ""
    Write-Host "Setup complete. Next run:" -ForegroundColor Green
    Write-Host "  powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 smoke"
    Write-Host "  powershell -ExecutionPolicy Bypass -File scripts/lite.ps1 lab"
}

function Invoke-Smoke {
    Assert-Venv
    & $PythonExe (Join-Path $RepoRoot "scripts\verify_lite.py")
}

function Invoke-Data {
    Assert-Venv
    & $PythonExe (Join-Path $RepoRoot "scripts\generate_data_lite.py")
}

function Invoke-Lab {
    Assert-Venv
    Convert-Notebooks
    & $PythonExe -m jupyter lab --notebook-dir $NotebookDir --ServerApp.token= --no-browser
}

function Invoke-Clean {
    Remove-Item -LiteralPath $VenvDir -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $RepoRoot "_lakehouse") -Recurse -Force -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath (Join-Path $NotebookDir ".ipynb_checkpoints") -Recurse -Force -ErrorAction SilentlyContinue
}

switch ($Action) {
    "setup" { Invoke-Setup }
    "smoke" { Invoke-Smoke }
    "data"  { Invoke-Data }
    "lab"   { Invoke-Lab }
    "clean" { Invoke-Clean }
    "all" {
        Invoke-Setup
        Invoke-Smoke
        Invoke-Data
    }
}
