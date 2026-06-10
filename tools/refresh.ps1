# tools/refresh.ps1 — one-command Streamlit refresh on Windows.
#
# Runs the pipeline then the gated publish (commit + push -> Streamlit Cloud
# redeploys). Use it by hand or wire it into Task Scheduler:
#   Program/script:  powershell.exe
#   Arguments:       -ExecutionPolicy Bypass -File "C:\Users\pglyn\PycharmProjects\dail_extractor\tools\refresh.ps1"
#
# Any extra args pass straight through to tools/refresh.py, e.g.:
#   .\tools\refresh.ps1 --select iris,votes
#   .\tools\refresh.ps1 --dry-run

$ErrorActionPreference = "Stop"
$repo = Split-Path -Parent $PSScriptRoot           # tools/ -> repo root
Set-Location $repo

# Prefer the project venv (it has the pipeline deps: polars, PyMuPDF, requests);
# fall back to PATH python if the venv is missing.
$py = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $py)) { $py = "python" }

& $py "tools\refresh.py" @args
exit $LASTEXITCODE
