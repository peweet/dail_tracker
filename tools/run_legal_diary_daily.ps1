<#
run_legal_diary_daily.ps1 — daily capture of the Courts Service Legal Diary.

Runs the poller (archives today's diary .docx if new — idempotent) and, when the
poller succeeds, the extractor (rebuilds the judiciary gold parquets from the full
archive so days accumulate). Intended to be driven by Windows Task Scheduler; safe
to run by hand. The poller returns 0 for both "archived a new day" and "already
current", 1 for a transient network error, 2 for source drift — we only run the
extractor on 0.

Logs: the Python steps log to logs/standalone/{legal_diary_poller,legal_diary_extract}.log
(rotated); this wrapper appends a one-line run summary to logs/standalone/legal_diary_daily.log.

Register/refresh the scheduled task with:  tools/register_legal_diary_task.ps1
#>

$ErrorActionPreference = 'Stop'
$root = Split-Path -Parent $PSScriptRoot           # repo root (tools/ -> ..)
$py   = Join-Path $root '.venv\Scripts\python.exe'
$log  = Join-Path $root 'logs\standalone\legal_diary_daily.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

Set-Location $root
& $py -m pdf_infra.legal_diary_poller
$pollExit = $LASTEXITCODE

if ($pollExit -eq 0) {
    & $py extractors\legal_diary_extract.py
    $extractExit = $LASTEXITCODE
    Add-Content $log "$stamp  poll=0  extract=$extractExit"
    exit $extractExit
}
else {
    Add-Content $log "$stamp  poll=$pollExit  extract=skipped"
    exit $pollExit
}
