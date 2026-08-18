$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
$venvDir = Join-Path $repoRoot ".venv"
$python = Join-Path $venvDir "Scripts\python.exe"
$pip = Join-Path $venvDir "Scripts\pip.exe"
$jupytext = Join-Path $venvDir "Scripts\jupytext.exe"

Push-Location $repoRoot
try {
    $uv = Get-Command uv -ErrorAction SilentlyContinue
    $venvHealthy = $false
    $venvConfig = Join-Path $venvDir "pyvenv.cfg"
    $basePythonExists = $false
    if (Test-Path -LiteralPath $venvConfig) {
        $homeLine = Get-Content -LiteralPath $venvConfig | Where-Object { $_ -match '^home\s*=' } | Select-Object -First 1
        if ($homeLine) {
            $baseHome = ($homeLine -split '=', 2)[1].Trim()
            $basePythonExists = Test-Path -LiteralPath (Join-Path $baseHome "python.exe")
        }
    }
    if ($basePythonExists -and (Test-Path -LiteralPath $python)) {
        $previousPreference = $ErrorActionPreference
        try {
            $ErrorActionPreference = "SilentlyContinue"
            $global:LASTEXITCODE = 999
            & $python -c "import sys; raise SystemExit(0 if (3,10) <= sys.version_info[:2] < (3,15) else 1)" 2>$null
            $venvHealthy = $LASTEXITCODE -eq 0
        }
        catch {
            $venvHealthy = $false
        }
        finally {
            $ErrorActionPreference = $previousPreference
        }
    }

    if (-not $venvHealthy) {
        if ($uv) {
            & $uv.Source venv $venvDir --clear --python ">=3.10,<3.15"
        }
        else {
            $launcher = Get-Command py -ErrorAction SilentlyContinue
            if (-not $launcher) {
                throw "Python 3.10-3.14 or uv is required."
            }
            if (Test-Path -LiteralPath $venvDir) {
                Remove-Item -LiteralPath $venvDir -Recurse -Force
            }
            & $launcher.Source -3 -m venv $venvDir
        }
        if ($LASTEXITCODE -ne 0) { throw "Failed to create $venvDir" }
    }

    if ($uv) {
        & $uv.Source pip install --python $python -r requirements.txt
    }
    else {
        & $pip install -q -r requirements.txt
    }
    if ($LASTEXITCODE -ne 0) { throw "Dependency installation failed." }

    foreach ($source in Get-ChildItem -LiteralPath notebooks -Filter "*.py") {
        & $jupytext --to notebook --update $source.FullName 2>$null
        if ($LASTEXITCODE -ne 0) {
            & $jupytext --to notebook $source.FullName
        }
        if ($LASTEXITCODE -ne 0) { throw "Jupytext conversion failed for $($source.Name)" }
    }

    Write-Output ""
    Write-Output "  Setup complete. Run 'make smoke' then 'make lab'."
}
finally {
    Pop-Location
}
