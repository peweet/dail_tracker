<#
.SYNOPSIS
  One-command pipeline health, drift & freshness gate - with an optional gated push.

.DESCRIPTION
  Runs the whole stability sweep in stages, mirroring CI exactly (so a green run here
  means a green CI), then the data-freshness / drift / source-health / supply-chain checks,
  and finally - only if everything gating is green AND you ask for it - the existing gated
  data publish (tools/refresh.py -> publish_data.py).

  It NEVER auto-commits code and NEVER force-pushes. The only thing that can be pushed is the
  allow-listed DATA, through tools/publish_data.py (which runs its own integrity gate). Code
  problems (ruff/tests/firewall) are FLAGGED and abort the push - they are never "fixed and
  pushed" silently.

  Stages (each PASS/FAIL/SKIP is collected - read-only checks all run so you get the FULL
  picture, not just the first failure):

    CODE      ruff check . | ruff format --check . | uv lock --check | logic firewall
              | basedpyright | pytest (CI subset)                       [gating]
    DATA      gold drift guard (check_output_regressions --strict)
              | sql contracts (pytest -m sql)                           [gating]
    FRESH     freshness_status --strict (did every lane run on time?)   [gating w/ -Strict]
    SOURCES   source_health_report (committed health: what's DOWN?)     [informational]
              + build_source_health --strict (LIVE re-probe)            [only with -ProbeSources]
    AUDIT     pip-audit (supply-chain CVEs)                             [informational]
    PUBLISH   tools/refresh.py (refresh + gated commit + push)          [only with -Push]

  Exit code is 0 only if no gating stage failed.

.PARAMETER Push
  After all gating checks pass, run tools/refresh.py to refresh data and PUSH the gated
  commit to GitHub. Omit for a read-only health report (the default, safe).

.PARAMETER Refresh
  Run tools/refresh.py --dry-run (ETL + preview the publish, commit nothing). Use to test the
  refresh path without pushing. Ignored if -Push is set.

.PARAMETER Fast
  Skip the slow stages (basedpyright, full pytest, sql contracts) for a quick read.

.PARAMETER Strict
  Treat FRESH (lane staleness) as gating - a LATE/MISSING lane then fails the run.

.PARAMETER ProbeSources
  Also LIVE re-probe every source (build_source_health --strict). Network-heavy/slow; off by
  default (SOURCES otherwise reads the committed health snapshot).

.EXAMPLE
  pwsh tools/health_check.ps1                 # full read-only health report
.EXAMPLE
  pwsh tools/health_check.ps1 -Fast           # quick code+drift+freshness read
.EXAMPLE
  pwsh tools/health_check.ps1 -Push -Strict   # gate hard, then refresh + push data
#>
[CmdletBinding()]
param(
    [switch]$Push,
    [switch]$Refresh,
    [switch]$Fast,
    [switch]$Strict,
    [switch]$ProbeSources
)

$ErrorActionPreference = 'Continue'
$repo = Split-Path -Parent $PSScriptRoot          # repo root (tools/..)
Set-Location $repo

# Use the project .venv directly. We deliberately do NOT use `uv run` for the checks: it re-syncs
# the environment on each call and can uninstall/replace packages (env churn). `uv` is used ONLY
# for the read-only `uv lock --check`, which inspects the lockfile and never touches the venv.
$uv = (Get-Command uv -ErrorAction SilentlyContinue)
$venvDir = Join-Path $repo '.venv/Scripts'
$venvPy = Join-Path $venvDir 'python.exe'
function Py { @($venvPy) }
function Tool($exe) { @((Join-Path $venvDir "$exe.exe")) }

$results = [System.Collections.Generic.List[object]]::new()

function Invoke-Stage {
    param(
        [string]$Stage,
        [string]$Name,
        [string[]]$Cmd,        # argv: first element is the exe, rest are args
        [switch]$Gating,
        [switch]$Skip,
        [string]$SkipReason = ''
    )
    if ($Skip) {
        $results.Add([pscustomobject]@{ Stage = $Stage; Check = $Name; Status = 'SKIP'; Secs = 0; Gating = [bool]$Gating; Note = $SkipReason })
        Write-Host "  - [SKIP] $Name  ($SkipReason)" -ForegroundColor DarkGray
        return
    }
    # Graceful skip if a tool binary is absent (e.g. basedpyright not installed in this venv) — a
    # missing optional checker should not look like a real failure.
    if (($Cmd[0] -match '[\\/]') -and -not (Test-Path $Cmd[0])) {
        $results.Add([pscustomobject]@{ Stage = $Stage; Check = $Name; Status = 'SKIP'; Secs = 0; Gating = $false; Note = 'tool not installed' })
        Write-Host "  - [SKIP] $Name  (not installed)" -ForegroundColor DarkGray
        return
    }
    Write-Host "  > $Name ..." -ForegroundColor Cyan
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $rest = if ($Cmd.Count -gt 1) { $Cmd[1..($Cmd.Count - 1)] } else { @() }
    & $Cmd[0] @rest
    $code = $LASTEXITCODE
    $sw.Stop()
    $ok = ($code -eq 0)
    $status = if ($ok) { 'PASS' } else { 'FAIL' }
    $colour = if ($ok) { 'Green' } else { 'Red' }
    $results.Add([pscustomobject]@{ Stage = $Stage; Check = $Name; Status = $status; Secs = [math]::Round($sw.Elapsed.TotalSeconds, 1); Gating = [bool]$Gating; Note = "exit $code" })
    Write-Host ("    [{0}] {1}  ({2}s)" -f $status, $Name, [math]::Round($sw.Elapsed.TotalSeconds, 1)) -ForegroundColor $colour
}

Write-Host "`n=== Pipeline health check ===  repo=$repo  uv=$([bool]$uv)  $(if($Push){'PUSH'}elseif($Refresh){'REFRESH(dry-run)'}else{'READ-ONLY'})`n" -ForegroundColor White

# ---- CODE (mirrors .github/workflows/ci.yml) -------------------------------------------------
Write-Host "[CODE]" -ForegroundColor Yellow
Invoke-Stage CODE 'ruff check'        (@(Tool 'ruff') + @('check', '.'))            -Gating
Invoke-Stage CODE 'ruff format check' (@(Tool 'ruff') + @('format', '--check', '.')) -Gating
if ($uv) { Invoke-Stage CODE 'uv.lock in sync' @('uv', 'lock', '--check') -Gating }
else { Invoke-Stage CODE 'uv.lock in sync' @() -Gating -Skip -SkipReason 'uv not found' }
Invoke-Stage CODE 'logic firewall'    (@(Py) + @('tools/check_streamlit_logic_firewall.py')) -Gating
Invoke-Stage CODE 'basedpyright'      (@(Tool 'basedpyright'))   -Gating -Skip:$Fast -SkipReason '-Fast'
Invoke-Stage CODE 'pytest (CI subset)' (@(Tool 'pytest') + @('-q', '-m', 'not integration and not sql and not sources and not bronze')) -Gating -Skip:$Fast -SkipReason '-Fast'

# ---- DATA: drift + sql contracts -------------------------------------------------------------
Write-Host "`n[DATA]" -ForegroundColor Yellow
Invoke-Stage DATA 'gold drift guard'  (@(Py) + @('tools/check_output_regressions.py', '--strict')) -Gating
Invoke-Stage DATA 'sql contracts'     (@(Tool 'pytest') + @('-q', '-m', 'sql')) -Gating -Skip:$Fast -SkipReason '-Fast'

# ---- FRESH: did every lane run on time? ------------------------------------------------------
Write-Host "`n[FRESH]" -ForegroundColor Yellow
$freshArgs = @('tools/freshness_status.py'); if ($Strict) { $freshArgs += '--strict' }
Invoke-Stage FRESH 'lane freshness' (@(Py) + $freshArgs) -Gating:$Strict

# ---- SOURCES: what's reachable / down --------------------------------------------------------
Write-Host "`n[SOURCES]" -ForegroundColor Yellow
Invoke-Stage SOURCES 'source health (committed)' (@(Py) + @('tools/source_health_report.py'))
if ($ProbeSources) {
    Invoke-Stage SOURCES 'source health (LIVE probe)' (@(Py) + @('tools/build_source_health.py', '--strict'))
}
else {
    Invoke-Stage SOURCES 'source health (LIVE probe)' @() -Skip -SkipReason 'pass -ProbeSources to re-probe'
}

# ---- AUDIT: supply-chain CVEs ----------------------------------------------------------------
Write-Host "`n[AUDIT]" -ForegroundColor Yellow
$pipAudit = Join-Path $venvDir 'pip-audit.exe'
if (Test-Path $pipAudit) { Invoke-Stage AUDIT 'pip-audit' @($pipAudit) }
else { Invoke-Stage AUDIT 'pip-audit' @() -Skip -SkipReason 'pip-audit not in .venv (uv sync --group audit)' }

# ---- Gate decision ---------------------------------------------------------------------------
$gatingFails = @($results | Where-Object { $_.Gating -and $_.Status -eq 'FAIL' })
$anyFail = @($results | Where-Object { $_.Status -eq 'FAIL' })

Write-Host "`n=== Summary ===" -ForegroundColor White
$results | Format-Table Stage, Check, Status, Secs, Gating, Note -AutoSize | Out-String | Write-Host

# ---- PUBLISH / REFRESH (only if gating is clean) ---------------------------------------------
if ($Push -or $Refresh) {
    if ($gatingFails.Count -gt 0) {
        Write-Host "REFUSING to refresh/publish - $($gatingFails.Count) gating check(s) failed:" -ForegroundColor Red
        $gatingFails | ForEach-Object { Write-Host "    - $($_.Stage)/$($_.Check)" -ForegroundColor Red }
    }
    else {
        $mode = if ($Push) { 'PUSH' } else { 'DRY-RUN' }
        Write-Host "All gating checks green -> running tools/refresh.py ($mode)" -ForegroundColor Green
        $refreshArgs = @('tools/refresh.py'); if (-not $Push) { $refreshArgs += '--dry-run' }
        Invoke-Stage PUBLISH "refresh.py ($mode)" (@(Py) + $refreshArgs) -Gating
    }
}

# ---- Verdict ---------------------------------------------------------------------------------
$downSources = @($results | Where-Object { $_.Stage -eq 'SOURCES' -and $_.Status -eq 'FAIL' })
if ($downSources.Count -gt 0) { Write-Host "[!] Source health flagged problems - see [SOURCES] above." -ForegroundColor Yellow }

if ($gatingFails.Count -eq 0) {
    $warn = if ($anyFail.Count -gt 0) { " ($($anyFail.Count) non-gating warning(s))" } else { '' }
    Write-Host "`nVERDICT: STABLE - no gating failures.$warn`n" -ForegroundColor Green
    exit 0
}
else {
    Write-Host "`nVERDICT: UNSTABLE - $($gatingFails.Count) gating failure(s). Nothing was pushed.`n" -ForegroundColor Red
    exit 1
}
