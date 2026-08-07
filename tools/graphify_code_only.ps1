<#
.SYNOPSIS
Build a local, code-only Graphify graph outside this repository.

.DESCRIPTION
Uses the pinned, Apache-2.0 Graphify CLI without its installer, hooks, skill
files, LLM backends, or query log. The graph is an advisory developer-navigation
artifact; the repository's own read-only MCP tools remain authoritative.

.EXAMPLE
powershell.exe -NoProfile -ExecutionPolicy Bypass -File tools/graphify_code_only.ps1 -OutputRoot C:\Temp\dail-graphify
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]$OutputRoot,

    [switch]$Force
)

$ErrorActionPreference = "Stop"
$repositoryRoot = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
$resolvedOutput = [IO.Path]::GetFullPath($OutputRoot)
$trimChars = [char[]]@([IO.Path]::DirectorySeparatorChar, [IO.Path]::AltDirectorySeparatorChar)
$rootPrefix = $repositoryRoot.TrimEnd($trimChars) + [IO.Path]::DirectorySeparatorChar

if ($resolvedOutput.StartsWith($rootPrefix, [StringComparison]::OrdinalIgnoreCase)) {
    throw "OutputRoot must be outside the repository so graph artifacts cannot enter this worktree."
}

$graphify = Get-Command -Name graphify -CommandType Application -ErrorAction SilentlyContinue
if ($null -eq $graphify) {
    throw "Graphify 0.9.35 is not installed. Run: uv tool install --python .venv\\Scripts\\python.exe graphifyy==0.9.35"
}
$installedVersion = (& $graphify.Source --version).Trim()
if ($LASTEXITCODE -ne 0 -or $installedVersion -ne "graphify 0.9.35") {
    throw "Expected Graphify 0.9.35, found '$installedVersion'. Reinstall the pinned graphifyy==0.9.35 tool."
}

$env:GRAPHIFY_QUERY_LOG_DISABLE = "1"
$arguments = @("extract", $repositoryRoot, "--code-only", "--out", $resolvedOutput)
if ($Force) {
    $arguments += "--force"
}

& $graphify.Source @arguments
exit $LASTEXITCODE
