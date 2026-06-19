<#
run_legal_diary_daily.ps1 — daily refresh bundle (Legal Diary + the year-round /
frequently-updating pipeline chains).

Originally a Legal-Diary-only wrapper; now also runs the chains whose upstream
sources legitimately change daily/weekly (per tools/check_freshness.py thresholds):

    bootstrap     Members API poll + flatten (prerequisite for the rest)
    members       Wikidata/socials + ministerial tenure (votes/questions feed; 14d)
    iris          Iris Oifigiúil gazette (Tue/Fri, 14d)
    legislation   bills + questions + amendments + votes (year-round when sitting)
    ted           TED EU award notices (Ireland)            — weekly-ish
    ted_tenders   TED Irish competition/tender notices      — weekly-ish
    legal_diary   poller -> extract -> roster link (forward-accumulating, day-or-lost)
    freshness     data-age signal       -> data/_meta/freshness.json   (read-only)
    source_health per-source staleness  -> data/_meta/source_health.json (read-only)

RESILIENCE — each step is run via Invoke-WithRetry: up to 3 attempts, then the
bundle PROCEEDS ANYWAY to the next step (one flaky source never blocks the rest).
The app-data chains go through `pipeline.py --select <chain>` so they keep the
orchestrator's per-chain logging + manifest; each chain logs to logs/runs/<id>/.

LOCAL REFRESH ONLY — this rebuilds local gold/silver; it does NOT commit/push.
Publishing to Streamlit Cloud stays the separate, integrity-gated tools/refresh.ps1
path, so a degraded daily run can never reach the live app.

Legal-Diary gate preserved: the poller returns 0 for "archived a new day" AND
"already current", 1 for a transient network error, 2 for source drift. We rebuild
the judiciary gold (extract + roster link) only on 0; on 1/2 we skip the rebuild
(the existing archive is untouched) and still proceed to the remaining steps.

Logs: each Python step logs under logs/standalone/ or logs/runs/<id>/; this wrapper
appends a one-line run summary to logs/standalone/legal_diary_daily.log.

Register/refresh the scheduled task with:  tools/register_legal_diary_task.ps1
#>

# We handle every failure ourselves (retry, then proceed) — never let a non-zero
# child abort the bundle.
$ErrorActionPreference = 'Continue'
$root = Split-Path -Parent $PSScriptRoot           # repo root (tools/ -> ..)
$py   = Join-Path $root '.venv\Scripts\python.exe'
if (-not (Test-Path $py)) { $py = 'python' }
$log  = Join-Path $root 'logs\standalone\legal_diary_daily.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

Set-Location $root

# Run $Action up to $Retries times; treat any code in $SuccessCodes as success and
# return immediately. On exhaustion, log and return the last exit code — the CALLER
# proceeds regardless. Native stdout is sent to the host (visible + logged by the
# child itself) so the ONLY pipeline output is the returned exit code.
function Invoke-WithRetry {
    param(
        [Parameter(Mandatory)][string]$Name,
        [Parameter(Mandatory)][scriptblock]$Action,
        [int]$Retries = 3,
        [int[]]$SuccessCodes = @(0),
        [int]$DelaySeconds = 20
    )
    $code = $null
    for ($attempt = 1; $attempt -le $Retries; $attempt++) {
        Write-Host "--- $Name (attempt $attempt/$Retries) ---"
        & $Action | Out-Host
        $code = $LASTEXITCODE
        if ($SuccessCodes -contains $code) {
            Write-Host "${Name}: ok (exit $code) on attempt $attempt"
            return $code
        }
        Write-Host "${Name}: FAILED (exit $code) on attempt $attempt"
        if ($attempt -lt $Retries) { Start-Sleep -Seconds $DelaySeconds }
    }
    Write-Host "${Name}: giving up after $Retries attempt(s) (exit $code) - proceeding anyway"
    return $code
}

$results = [ordered]@{}

# 1. App-data chains in dependency order. bootstrap MUST be first (every chain
#    assumes it refreshed flattened_members; iris needs members.ministerial_tenure;
#    legislation needs bootstrap's questions/votes JSON). ted/ted_tenders are
#    standalone and skip gracefully on an API outage.
foreach ($chain in @('bootstrap', 'members', 'iris', 'legislation', 'ted', 'ted_tenders')) {
    $results[$chain] = Invoke-WithRetry -Name "pipeline:$chain" -Action {
        & $py 'pipeline.py' '--select' $chain
    }
}

# 2. Legal Diary — poll (retry), then rebuild gold (extract + roster link) ONLY if
#    the poll is current/archived (exit 0). Exit 1 (transient) / 2 (source drift):
#    skip the rebuild, still proceed.
$pollExit = Invoke-WithRetry -Name 'legal_diary_poller' -Action {
    & $py '-m' 'pdf_infra.legal_diary_poller'
}
$results['legal_diary_poller'] = $pollExit
if ($pollExit -eq 0) {
    $results['legal_diary_extract'] = Invoke-WithRetry -Name 'legal_diary_extract' -Action {
        & $py 'extractors\legal_diary_extract.py'
    }
    $results['judiciary_diary_link'] = Invoke-WithRetry -Name 'judiciary_diary_link' -Action {
        & $py 'extractors\judiciary_diary_link.py'
    }
}
else {
    $results['legal_diary_extract'] = 'skipped'
    $results['judiciary_diary_link'] = 'skipped'
}

# 3. Monitoring (read-only) runs last so it sees everything the chains above built.
foreach ($chain in @('freshness', 'source_health')) {
    $results[$chain] = Invoke-WithRetry -Name "pipeline:$chain" -Action {
        & $py 'pipeline.py' '--select' $chain
    }
}

# Per-lane freshness beat: record that the LOCAL daily bundle's legal-diary lane ran
# (poll current/archived AND the gold rebuild succeeded). LOCAL-only — this task never
# publishes, so the beat reflects local gold; the DEPLOYED app's freshness still needs a
# publish (a cloud Action or a manual push). tools/freshness_status.py reads this beat.
if ($pollExit -eq 0 -and $results['legal_diary_extract'] -eq 0) {
    & $py 'tools\freshness_heartbeat.py' 'legal_diary_docx' '--runner' 'local' '--cadence-hours' '24' | Out-Host
}

# One-line run summary, e.g.  bootstrap=0  members=0  ...  legal_diary_poller=0  ...
$summary = ($results.GetEnumerator() | ForEach-Object { "$($_.Key)=$($_.Value)" }) -join '  '
Add-Content $log "$stamp  $summary"
Write-Host "`nrun summary: $summary"

# Health signal for Task Scheduler: 0 if every step succeeded, 1 if any failed
# (the bundle still ran end-to-end — this only flags that something needs a look).
$failed = $results.Values | Where-Object { $_ -ne 'skipped' -and $_ -ne 0 }
if ($failed) { exit 1 } else { exit 0 }
