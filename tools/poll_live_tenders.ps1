<#
.SYNOPSIS
  Poll the live national tender pipeline (etenders.gov.ie) -- refreshes the silver snapshot
  data/silver/parquet/etenders_live_tenders.parquet via extractors/etenders_live_tenders_extract.py.

.DESCRIPTION
  Wrapper for a scheduled daily refresh (the snapshot is a point-in-time capture; deadlines pass
  and new tenders appear daily). SANDBOX-only -- it does NOT touch the awards/payments gold or the
  pipeline. Grid capture and CPV detail enrichment run as separate processes so a slow detail crawl
  cannot prevent a complete grid snapshot from being published.

  Logs to logs/standalone/poll_live_tenders.log. Single-instance guarded via a lock file. Self-limits
  via the extractor's own page timeouts + the --max-pages cap, with a hard 20-min watchdog per stage.

.NOTES
  Register the scheduled task with: tools/poll_live_tenders.ps1 -Register
  Remove it with:                   tools/poll_live_tenders.ps1 -Unregister
  ToU: public procurement record (aggregators scrape the same); polite pacing is built into the extractor.
#>
param(
    [int]$MaxPages = 400,
    [int]$DelayMs = 1000,
    [int]$MaxDetails = 300,
    [switch]$Register,
    [switch]$Unregister
)

$ErrorActionPreference = 'Stop'
$Root = Split-Path -Parent $PSScriptRoot
$TaskName = 'DailTracker-LiveTenders-Poll'

if ($Register) {
    $action = "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$PSCommandPath`" -MaxPages $MaxPages -DelayMs $DelayMs -MaxDetails $MaxDetails"
    schtasks /Create /TN $TaskName /SC DAILY /ST 06:30 /F `
        /TR "powershell.exe $action" | Out-Null
    Write-Host "Registered scheduled task '$TaskName' (daily 06:30). Disable: tools/poll_live_tenders.ps1 -Unregister"
    return
}
if ($Unregister) {
    schtasks /Delete /TN $TaskName /F | Out-Null
    Write-Host "Removed scheduled task '$TaskName'."
    return
}

$py = Join-Path $Root '.venv/Scripts/python.exe'
if (-not (Test-Path $py)) { $py = 'python' }
$logDir = Join-Path $Root 'logs/standalone'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null
$log = Join-Path $logDir 'poll_live_tenders.log'
$lock = Join-Path $env:TEMP 'dailtracker_poll_live_tenders.lock'

if (Test-Path $lock) {
    $age = (Get-Date) - (Get-Item $lock).LastWriteTime
    if ($age.TotalMinutes -lt 60) {
        <#
        "$(Get-Date -Format s)  SKIP -- a poll is already running (lock age $([int]($age.TotalMinutes))m)" | Add-Content $log
#>
        "$(Get-Date -Format s)  SKIP - a poll is already running (lock age $([int]$age.TotalMinutes)m)" | Add-Content $log
        return
    }
}
New-Item -ItemType File -Force -Path $lock | Out-Null

function Invoke-TenderStage {
    param(
        [string]$Name,
        [string[]]$StageArgs
    )
    $stageOut = "$log.$Name.run"
    $stageErr = "$log.$Name.err"
    "$(Get-Date -Format s)  STAGE $Name" | Add-Content $log
    $process = Start-Process -FilePath $py `
        -ArgumentList $StageArgs `
        -WorkingDirectory $Root -NoNewWindow -PassThru `
        -RedirectStandardOutput $stageOut -RedirectStandardError $stageErr
    if (-not $process.WaitForExit(20 * 60 * 1000)) {
        $process.Kill()
        <#
        "$(Get-Date -Format s)  KILLED $Name -- exceeded 20-min watchdog" | Add-Content $log
#>
        "$(Get-Date -Format s)  KILLED $Name - exceeded 20-min watchdog" | Add-Content $log
        return $false
    }
    Get-Content $stageOut -Tail 8 -ErrorAction SilentlyContinue | Add-Content $log
    Get-Content $stageErr -Tail 8 -ErrorAction SilentlyContinue | Add-Content $log
    "$(Get-Date -Format s)  DONE $Name exit=$($process.ExitCode)" | Add-Content $log
    return $process.ExitCode -eq 0
}

try {
    "$(Get-Date -Format s)  START poll (max-pages=$MaxPages)" | Add-Content $log
    $script = Join-Path $Root 'extractors/etenders_live_tenders_extract.py'
    $gridArgs = @($script, '--max-pages', $MaxPages, '--delay-ms', $DelayMs, '--no-cpv-details')
    $gridOk = Invoke-TenderStage -Name 'grid' -StageArgs $gridArgs
    if ($gridOk) {
        $detailArgs = @($script, '--details-only', '--max-details', $MaxDetails, '--delay-ms', $DelayMs)
        $detailsOk = Invoke-TenderStage -Name 'details' -StageArgs $detailArgs
        if (-not $detailsOk) {
            "$(Get-Date -Format s)  WARN detail enrichment failed; complete grid snapshot remains usable" | Add-Content $log
        }
    } else {
        "$(Get-Date -Format s)  ERROR grid refresh failed; previous canonical snapshot was retained" | Add-Content $log
    }
} catch {
    "$(Get-Date -Format s)  ERROR $($_.Exception.Message)" | Add-Content $log
} finally {
    Remove-Item $lock -Force -ErrorAction SilentlyContinue
}
