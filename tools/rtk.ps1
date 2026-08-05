<#
.SYNOPSIS
  Run the deliberately narrow, project-local RTK failure-triage pilot.

.DESCRIPTION
  Provides compact output only for the last known pytest failures while preserving
  this repository's .venv selection. Telemetry is disabled for every invocation;
  token history and failure tees stay under the ignored .cache/rtk directory.

  Allowed commands:
  pytest-last-failed [args]  Triage only the last known failures; it never falls
                              back to a full green suite and stops after one failure.
  gain                       Project-filtered pilot statistics.
  version                    Verify the pinned executable.
  adoption [gate args]       Measure the pilot or record a review; never auto-adopts.

  Ruff is excluded because v0.44.2 misclassified unsafe hidden fixes during parity
  testing. Git is excluded because the repository's existing short status/log forms
  were already as compact as RTK. Search remains on the repository's scoped MCP/rg
  paths so the large-data read boundaries are not weakened.

  RTK suppressed a FastAPI/Starlette deprecation warning from a green API suite
  during this pilot. Therefore it is not available for normal or API test runs.
  Keep coverage, collection, diagnostic/interactive output, expected-failure
  ledgers, machine-parsed output, final diffs, and authoritative final verification
  on the repository's raw commands. If compact failure output is insufficient,
  inspect the full tee named by RTK instead of rerunning the command.

.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File tools/rtk.ps1 pytest-last-failed
.EXAMPLE
  powershell -NoProfile -ExecutionPolicy Bypass -File tools/rtk.ps1 adoption --require-eligible
#>
[CmdletBinding(PositionalBinding = $false)]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$Command,

    [Parameter(Position = 1, ValueFromRemainingArguments = $true)]
    [string[]]$CommandArgs = @()
)

Set-StrictMode -Version 3.0
$ErrorActionPreference = 'Stop'

$rtkVersion = '0.44.2'
$executableSha256 = '60640b970fdf10451813ab4d9d24deb5c6370e43a5192eb14c6ba101a15b633c'
$repoRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot '..')).Path
$rtkExe = Join-Path $repoRoot ".cache\rtk\v$rtkVersion\rtk.exe"
$venvScripts = Join-Path $repoRoot '.venv\Scripts'
$venvPython = Join-Path $venvScripts 'python.exe'
$teeDir = Join-Path $repoRoot '.cache\rtk\tee'
$historyDb = Join-Path $repoRoot '.cache\rtk\history.db'
$pilotEvents = Join-Path $repoRoot '.cache\rtk\pilot-events.jsonl'
$hookFreeConfigPath = Join-Path ([IO.Path]::GetTempPath()) (
    'dail-extractor-rtk-no-claude-' + [Guid]::NewGuid().ToString('N')
)

function Assert-NoBlockedArgument {
    param(
        [AllowEmptyCollection()][string[]]$Values = @(),
        [Parameter(Mandatory = $true)][string[]]$Patterns,
        [Parameter(Mandatory = $true)][string]$RawAlternative
    )

    foreach ($value in $Values) {
        foreach ($pattern in $Patterns) {
            if ($value -match $pattern) {
                throw "'$value' is outside the RTK pilot. Use the raw command instead: $RawAlternative"
            }
        }
    }
}

function Restore-ProcessEnvironment {
    param([Parameter(Mandatory = $true)][hashtable]$Previous)

    foreach ($name in $Previous.Keys) {
        if ($null -eq $Previous[$name]) {
            Remove-Item -LiteralPath "Env:$name" -ErrorAction SilentlyContinue
        }
        else {
            [Environment]::SetEnvironmentVariable($name, [string]$Previous[$name], 'Process')
        }
    }
}

function Write-PilotRunEvent {
    param(
        [Parameter(Mandatory = $true)][string]$RunId,
        [Parameter(Mandatory = $true)][datetime]$StartedAt,
        [Parameter(Mandatory = $true)][int]$ExitCode,
        [Parameter(Mandatory = $true)][long]$ElapsedMs
    )

    # Pilot telemetry is deliberately local, tiny, and fail-open: a receipt must
    # never change pytest's result. Test arguments are intentionally not retained.
    try {
        [IO.Directory]::CreateDirectory((Split-Path -Parent $pilotEvents)) | Out-Null
        $row = [ordered]@{
            schema = 'rtk-pilot-event/v1'
            at_utc = [DateTime]::UtcNow.ToString('o')
            started_at_utc = $StartedAt.ToUniversalTime().ToString('o')
            run_id = $RunId
            kind = 'rtk_run'
            command = 'pytest'
            exit_code = $ExitCode
            elapsed_ms = $ElapsedMs
            rtk_version = $rtkVersion
        }
        $json = $row | ConvertTo-Json -Compress
        [IO.File]::AppendAllText($pilotEvents, "$json`n", [Text.UTF8Encoding]::new($false))
    }
    catch {
        # Measurement is advisory. Preserve the child command's exit code even if
        # the ignored local ledger cannot be written.
    }
}

$previousEnvironment = @{}
foreach ($name in @('Path', 'CLAUDE_CONFIG_DIR', 'RTK_DB_PATH', 'RTK_TELEMETRY_DISABLED', 'RTK_TEE_DIR', 'PYTHONUTF8', 'PYTHONIOENCODING')) {
    $previousEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
}

$exitCode = 1
$originalLocation = Get-Location
$rtkRunId = $null
$rtkRunStartedAt = $null
$rtkRunTimer = $null

try {
    if ($Command -notin @('pytest-last-failed', 'gain', 'version', 'adoption')) {
        throw "Unsupported command '$Command'. The RTK pilot allows only pytest-last-failed, gain, version, and adoption."
    }
    if (-not (Test-Path -LiteralPath $rtkExe -PathType Leaf)) {
        throw "Pinned RTK is not installed. Run: powershell -NoProfile -ExecutionPolicy Bypass -File tools/install_rtk.ps1"
    }
    $actualHash = (Get-FileHash -LiteralPath $rtkExe -Algorithm SHA256).Hash.ToLowerInvariant()
    if ($actualHash -ne $executableSha256) {
        throw "RTK executable checksum mismatch at $rtkExe. Refusing to run it."
    }

    New-Item -ItemType Directory -Force -Path $teeDir | Out-Null
    # v0.44.2 treats a nonexistent CLAUDE_CONFIG_DIR as an explicit no-hook
    # environment. Do not create this path: it prevents hook prompts/checks without
    # touching the user's real Claude configuration or changing command output.
    $env:CLAUDE_CONFIG_DIR = $hookFreeConfigPath
    $env:RTK_DB_PATH = $historyDb
    $env:RTK_TELEMETRY_DISABLED = '1'
    $env:RTK_TEE_DIR = $teeDir
    $env:PYTHONUTF8 = '1'
    $env:PYTHONIOENCODING = 'utf-8'
    $env:Path = "$venvScripts$([IO.Path]::PathSeparator)$env:Path"
    Set-Location -LiteralPath $repoRoot

    switch ($Command) {
        'pytest-last-failed' {
            if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf) -or
                -not (Test-Path -LiteralPath (Join-Path $venvScripts 'pytest.exe') -PathType Leaf)) {
                throw 'The project .venv Python/pytest launchers are missing; run the raw environment setup first.'
            }
            Assert-NoBlockedArgument -Values $CommandArgs -Patterns @(
                '^--(?:no-)?cov(?:$|[=-])',
                '^--collect-only$',
                '^--co$',
                '^--junit(?:xml|-prefix)?(?:$|=)',
                '^--json-report(?:$|=)',
                '^--report-log(?:$|=)',
                '^-h$',
                '^--help$',
                '^--version$',
                '^--fixtures(?:$|=)',
                '^--fixtures-per-test$',
                '^--markers$',
                '^--trace-config$',
                '^--setup-(?:only|show|plan)$',
                '^--durations(?:$|=)',
                '^--durations-min(?:$|=)',
                '^-s$',
                '^--capture(?:$|=)',
                '^--show-capture(?:$|=)',
                '^--log-cli-level(?:$|=)',
                '^--pdb$',
                '^--pdbcls(?:$|=)',
                '^--trace$'
            ) -RawAlternative '.venv/Scripts/python -m pytest ...'
            Assert-NoBlockedArgument -Values $CommandArgs -Patterns @(
                '^--(?:last-failed|lf)(?:$|=)',
                '^--(?:last-failed-no-failures|lfnf)(?:$|=)',
                '^--maxfail(?:$|=)',
                '^-x$'
            ) -RawAlternative '.venv/Scripts/python -m pytest --last-failed --last-failed-no-failures=none --maxfail=1 ...'
            $rtkRunId = [Guid]::NewGuid().ToString('N')
            $rtkRunStartedAt = [DateTime]::UtcNow
            $rtkRunTimer = [Diagnostics.Stopwatch]::StartNew()
            & $rtkExe pytest --last-failed --last-failed-no-failures=none --maxfail=1 @CommandArgs
            $exitCode = $LASTEXITCODE
            $rtkRunTimer.Stop()
        }
        'gain' {
            if ($CommandArgs.Count -ne 0) {
                throw "The pilot exposes only project-filtered 'rtk gain' without extra arguments."
            }
            & $rtkExe gain --project
            $exitCode = $LASTEXITCODE
        }
        'version' {
            if ($CommandArgs.Count -ne 0) {
                throw "The 'version' command takes no arguments."
            }
            & $rtkExe --version
            $exitCode = $LASTEXITCODE
        }
        'adoption' {
            if (-not (Test-Path -LiteralPath $venvPython -PathType Leaf)) {
                throw 'The project .venv Python launcher is missing; cannot run the adoption gate.'
            }
            & $venvPython (Join-Path $repoRoot 'tools\rtk_pilot_gate.py') @CommandArgs
            $exitCode = $LASTEXITCODE
        }
    }
}
catch {
    $exitCode = 64
    [Console]::Error.WriteLine("rtk pilot: $($_.Exception.Message)")
}
finally {
    if ($null -ne $rtkRunTimer) {
        if ($rtkRunTimer.IsRunning) {
            $rtkRunTimer.Stop()
        }
        Write-PilotRunEvent -RunId $rtkRunId -StartedAt $rtkRunStartedAt -ExitCode $exitCode -ElapsedMs ([math]::Round($rtkRunTimer.Elapsed.TotalMilliseconds))
    }
    Set-Location -LiteralPath $originalLocation
    Restore-ProcessEnvironment -Previous $previousEnvironment
}

exit $exitCode
