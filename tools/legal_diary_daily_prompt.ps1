<#
.SYNOPSIS
    Daily Legal Diary refresh with a command-line permission prompt.

    Fired by the "DailTracker-LegalDiary" scheduled task (8pm, Mon-Fri). The
    Courts Service Legal Diary is forward-accumulating - only the current court
    day's .docx is published, so a missed day is lost forever. This wrapper asks
    before pulling, but errs on the side of capturing the data:

      * prompts "run now? (y/n)" with a 15-minute window for a key press;
      * y  -> run the legal-diary chain now;
      * n  -> skip today (respect the explicit decline) and exit;
      * no answer -> re-prompt, up to 3 attempts (so ~15 min apart);
      * still no answer after the 3rd attempt -> RUN ANYWAY (day-or-lost wins).

    The chain run is: pipeline.py --select legal_diary_poller,
    legal_diary_extract,judiciary_diary_link
#>

$ErrorActionPreference = 'Stop'

$repo = Split-Path $PSScriptRoot -Parent
$py = Join-Path $repo '.venv\Scripts\python.exe'
$chains = 'legal_diary_poller,legal_diary_extract,judiciary_diary_link'
$timeoutSec = 900   # 15 minutes per prompt
$maxAttempts = 3

$logDir = Join-Path $repo 'logs\scheduled'
if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
$logFile = Join-Path $logDir 'legal_diary_daily.log'

function Write-Log([string]$msg) {
    $line = "{0}  {1}" -f (Get-Date -Format 'yyyy-MM-dd HH:mm:ss'), $msg
    Write-Host $line
    Add-Content -Path $logFile -Value $line -Encoding utf8
}

# Wait up to $TimeoutSec for the user to press y or n. Returns 'yes', 'no', or
# 'timeout'. [Console]::KeyAvailable throws when there's no attached console or
# stdin is redirected (an unattended run) — caught as 'timeout' so we fall
# through to the run-anyway path instead of blocking for the full window.
function Read-TimedChoice([int]$TimeoutSec) {
    $deadline = (Get-Date).AddSeconds($TimeoutSec)
    while ((Get-Date) -lt $deadline) {
        try {
            $avail = [Console]::KeyAvailable
        } catch {
            Write-Log "No interactive console available - treating as no answer."
            return 'timeout'
        }
        if ($avail) {
            $key = [Console]::ReadKey($true)
            $c = "$($key.KeyChar)".ToLower()
            if ($c -eq 'y') { return 'yes' }
            if ($c -eq 'n') { return 'no' }
        }
        Start-Sleep -Milliseconds 250
    }
    return 'timeout'
}

function Invoke-LegalDiaryRun {
    Write-Log "Running legal-diary chain: $chains"
    $env:PYTHONUTF8 = '1'
    # No `2>&1 |`: piping a native exe's stderr through the PS pipeline wraps each
    # line as a NativeCommandError (which $ErrorActionPreference='Stop' would make
    # terminating, killing the run mid-chain). Let stdout/stderr flow straight to
    # the console (visible in the interactive window); pipeline.py keeps its own
    # per-run logs under logs/runs/. Out-Host keeps stdout off the return value.
    & $py (Join-Path $repo 'pipeline.py') '--select' $chains | Out-Host
    $code = $LASTEXITCODE
    Write-Log ("Chain finished with exit code {0}." -f $code)
    return $code
}

Write-Log "=== Legal Diary daily prompt started ==="

for ($attempt = 1; $attempt -le $maxAttempts; $attempt++) {
    Write-Host ""
    Write-Host "[$attempt/$maxAttempts] Pull the Courts Service Legal Diary now?"
    Write-Host "    y = run now    n = skip today    (no answer for 15 min = ask again; auto-runs after attempt $maxAttempts)"
    $answer = Read-TimedChoice $timeoutSec

    switch ($answer) {
        'yes' {
            Write-Log "User approved (attempt $attempt)."
            exit (Invoke-LegalDiaryRun)
        }
        'no' {
            Write-Log "User declined (attempt $attempt) - skipping today's run."
            exit 0
        }
        'timeout' {
            Write-Log "No answer on attempt $attempt of $maxAttempts."
        }
    }
}

Write-Log "No answer after $maxAttempts attempts - running anyway (diary is day-or-lost)."
exit (Invoke-LegalDiaryRun)
