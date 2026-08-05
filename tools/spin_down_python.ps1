<#
spin_down_python.ps1 - scan for (and optionally kill) stale Python process
clusters on this Windows dev box.

Why this exists
---------------
Long dev sessions leave a pileup of orphaned python.exe: dail-tracker MCP servers
from closed Claude windows, abandoned Streamlit runs, and sandbox/pipeline scripts
that were Ctrl-C'd or TaskStop'd (which orphans their child python). The box gets
lethargic. This tool finds them, categorises them, and - only when you ask - kills
the stale ones while protecting anything live.

Protections (always on)
------------------------------
  1. CURRENT SESSION  - every process in this script's own ancestry (the shell,
     its Claude/node host, etc.) is never touched.
  2. AGE GUARD        - processes younger than -MinAgeMinutes (default 20) are
     skipped, so a refresh/run you kicked off moments ago survives. Override with
     -IncludeYoung.

MCP clients launched under a shared VS Code/Codex extension host cannot be attributed
reliably to one chat session: all their `uv -> python -> python` trees have the same
parent. The age guard is therefore the safe protection for those MCP sessions. The
scan reports their logical-server count and warns above -McpWarnAbove (default 7).

Usage
-----
  tools/spin_down_python.ps1                      # SCAN only (default) - kills nothing
  tools/spin_down_python.ps1 -SpinDown            # kill the stale candidates
  tools/spin_down_python.ps1 -SpinDown -WhatIf    # show exactly what -SpinDown would kill
  tools/spin_down_python.ps1 -Category mcp,streamlit -SpinDown   # only those kinds
  tools/spin_down_python.ps1 -SpinDown -All       # also include uncategorised python
  tools/spin_down_python.ps1 -SpinDown -MinAgeMinutes 0 -IncludeYoung  # nuke everything stale-or-not
  tools/spin_down_python.ps1 -Category mcp -McpWarnAbove 7       # scan and alert only

Categories: mcp, streamlit, pipeline, extractor, sandbox, other.
By default the spin-downable kinds are mcp/streamlit/pipeline/extractor/sandbox;
'other' (uncategorised python) is included only with -All or an explicit -Category.
#>
[CmdletBinding(SupportsShouldProcess)]
param(
    [int]$MinAgeMinutes = 20,
    [string[]]$Category,
    [switch]$SpinDown,
    [switch]$IncludeYoung,
    [switch]$All,
    [ValidateRange(0, 1000)]
    [int]$McpWarnAbove = 7
)

$ErrorActionPreference = 'Stop'

# Walk up from this process to the root; these PIDs are the current session and are
# never killed.
function Get-AncestryPids {
    $ids = @()
    $cur = $PID
    $seen = @{}
    while ($cur -and -not $seen[$cur]) {
        $seen[$cur] = $true
        $ids += $cur
        $p = Get-CimInstance Win32_Process -Filter "ProcessId=$cur" -ErrorAction SilentlyContinue
        if (-not $p) { break }
        $cur = $p.ParentProcessId
    }
    return $ids
}

function Get-Category([string]$cmd) {
    if (-not $cmd) { return 'other' }
    if ($cmd -match 'dail_mcp|mcp_server') { return 'mcp' }
    if ($cmd -match 'streamlit\s+run') { return 'streamlit' }
    if ($cmd -match '\bpipeline\.py|_refresh\.py') { return 'pipeline' }
    if ($cmd -match 'pipeline_sandbox|[cC]:[\\/]tmp') { return 'sandbox' }
    if ($cmd -match '[\\/]extractors[\\/]') { return 'extractor' }
    return 'other'
}

function Get-McpSessionSummary([object[]]$allProcs, [object[]]$pythonProcs) {
    # The shipped configuration launches `uv -> venv python -> system python`.
    # Count the uv process as one logical stdio server. Also support a future direct
    # Python configuration so this alert remains truthful if we remove the uv wrapper.
    $byPid = @{}
    foreach ($p in $allProcs) { $byPid[[int]$p.ProcessId] = $p }
    $uvRoots = @($allProcs | Where-Object {
        $_.Name -ieq 'uv.exe' -and (Get-Category $_.CommandLine) -eq 'mcp'
    })
    $mcpPython = @($pythonProcs | Where-Object { (Get-Category $_.CommandLine) -eq 'mcp' })
    $directPythonRoots = @($mcpPython | Where-Object {
        $parent = $byPid[[int]$_.ParentProcessId]
        -not $parent -or (Get-Category $parent.CommandLine) -ne 'mcp'
    })
    return [pscustomobject]@{
        LogicalServers = $uvRoots.Count + $directPythonRoots.Count
        UvRoots = $uvRoots.Count
        DirectPythonRoots = $directPythonRoots.Count
        PythonProcesses = $mcpPython.Count
    }
}

$allProcs = @(Get-CimInstance Win32_Process)
$procs = @($allProcs | Where-Object { $_.Name -ieq 'python.exe' })
$mcpSummary = Get-McpSessionSummary $allProcs $procs
$mcpStatus = "MCP status: {0} logical server(s) ({1} uv root(s), {2} direct Python root(s); {3} Python process(es))." -f $mcpSummary.LogicalServers, $mcpSummary.UvRoots, $mcpSummary.DirectPythonRoots, $mcpSummary.PythonProcesses
Write-Host $mcpStatus
if ($McpWarnAbove -gt 0 -and $mcpSummary.LogicalServers -gt $McpWarnAbove) {
    $mcpWarning = "MCP session pileup: {0} logical server(s), above the warning threshold of {1}. " -f $mcpSummary.LogicalServers, $McpWarnAbove
    Write-Warning ($mcpWarning + "Preview cleanup with: tools/spin_down_python.ps1 -Category mcp -SpinDown -WhatIf")
}
if (-not $procs) {
    Write-Host "No python.exe processes running. Nothing to do."
    return
}

$ancestry = Get-AncestryPids
$byPid = @{}
foreach ($p in $procs) { $byPid[[int]$p.ProcessId] = $p }

# Protect Python processes that are direct descendants of this shell/session. A shared
# IDE host launches MCP through an intervening uv.exe process and can own many chats,
# so those processes rely on the age guard instead of unsafe per-session attribution.
$protected = [System.Collections.Generic.HashSet[int]]::new()
foreach ($id in $ancestry) { [void]$protected.Add([int]$id) }
$changed = $true
while ($changed) {
    $changed = $false
    foreach ($p in $procs) {
        $pid_ = [int]$p.ProcessId
        if ($protected.Contains($pid_)) { continue }
        if ($protected.Contains([int]$p.ParentProcessId)) {
            [void]$protected.Add($pid_); $changed = $true
        }
    }
}

# Which categories are spin-downable this run.
$spinKinds = if ($Category) { $Category } elseif ($All) {
    @('mcp', 'streamlit', 'pipeline', 'extractor', 'sandbox', 'other')
} else {
    @('mcp', 'streamlit', 'pipeline', 'extractor', 'sandbox')
}

$now = Get-Date
$rows = foreach ($p in $procs) {
    $pid_ = [int]$p.ProcessId
    $cat = Get-Category $p.CommandLine
    $age = [int]($now - $p.CreationDate).TotalMinutes
    $status =
    if ($protected.Contains($pid_)) { 'PROTECTED' }
    elseif ($spinKinds -notcontains $cat) { 'skip (category)' }
    elseif (-not $IncludeYoung -and $age -lt $MinAgeMinutes) { 'skip (young)' }
    else { 'CANDIDATE' }
    [pscustomobject]@{
        PID    = $pid_
        AgeMin = $age
        Cat    = $cat
        Status = $status
        Cmd    = if ($p.CommandLine) { ($p.CommandLine -replace '^.*python(312)?\.exe"?\s*', '') } else { '' }
    }
}

Write-Host ("Found {0} python.exe | ancestry-protected session PID(s): {1}" -f $procs.Count, ($ancestry -join ', '))
$rows | Sort-Object Status, Cat, AgeMin |
    Format-Table PID, AgeMin, Cat, Status, @{n = 'Cmd'; e = { $_.Cmd.Substring(0, [Math]::Min(70, $_.Cmd.Length)) } } -AutoSize |
    Out-String -Width 200 | Write-Host

$candidates = @($rows | Where-Object { $_.Status -eq 'CANDIDATE' })
if (-not $candidates) {
    Write-Host "No spin-down candidates (everything is protected, young, or out of category)."
    return
}

if (-not $SpinDown) {
    Write-Host ("SCAN ONLY: {0} candidate(s) above. Re-run with -SpinDown to kill them (add -WhatIf to preview)." -f $candidates.Count)
    return
}

# Kill children before parents so a parent can't be seen re-forking. ($byPid depth
# is unknown, so sort by descending PID is a cheap good-enough proxy; orphaned
# python rarely re-fork anyway.)
$killed = 0; $failed = 0
foreach ($c in ($candidates | Sort-Object PID -Descending)) {
    if ($PSCmdlet.ShouldProcess("PID $($c.PID) [$($c.Cat), $($c.AgeMin)m]", "Stop-Process")) {
        try {
            Stop-Process -Id $c.PID -Force -ErrorAction Stop
            Write-Host "  killed $($c.PID)  [$($c.Cat)]"
            $killed++
        }
        catch {
            Write-Host "  FAILED $($c.PID): $($_.Exception.Message)"
            $failed++
        }
    }
}
Write-Host ("spin-down done: killed={0} failed={1} (protected the live MCP server + this session)." -f $killed, $failed)
