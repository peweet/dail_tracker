<#
.SYNOPSIS
Registers the Codex Windows sandbox self-healing guard as a per-user task.

.DESCRIPTION
The task runs at logon and at a short interval while the user is logged on.
It launches outside the Codex command runner, which is essential because the
failure being repaired occurs before that runner can start any process.

Re-run to update the task. Remove with:
  powershell -File tools/install_codex_windows_sandbox_guard.ps1 -Uninstall
#>

[CmdletBinding()]
param(
    [ValidateRange(1, 60)]
    [int]$IntervalMinutes = 2,
    [switch]$RunNow,
    [switch]$Uninstall
)

$ErrorActionPreference = 'Stop'
$taskName = 'DailTracker-CodexSandboxGuard'

if ($Uninstall) {
    Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
    Write-Host "Removed scheduled task '$taskName'. Preserved repair logs and backups."
    exit 0
}

$root = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$guard = Join-Path $root 'tools\codex_windows_sandbox_guard.ps1'
if (-not (Test-Path -LiteralPath $guard -PathType Leaf)) {
    throw "Guard script not found at $guard."
}

$windowsPowerShell = Join-Path $env:SystemRoot 'System32\WindowsPowerShell\v1.0\powershell.exe'
if (-not (Test-Path -LiteralPath $windowsPowerShell -PathType Leaf)) {
    throw "Windows PowerShell not found at $windowsPowerShell."
}

$action = New-ScheduledTaskAction -Execute $windowsPowerShell `
    -Argument "-NoProfile -NonInteractive -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$guard`"" `
    -WorkingDirectory $root
$logonTrigger = New-ScheduledTaskTrigger -AtLogOn -User ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name)
$repeatTrigger = New-ScheduledTaskTrigger -Once -At (Get-Date).AddMinutes(1) `
    -RepetitionInterval (New-TimeSpan -Minutes $IntervalMinutes) `
    -RepetitionDuration (New-TimeSpan -Days 3650)
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -MultipleInstances IgnoreNew -ExecutionTimeLimit (New-TimeSpan -Minutes 1)
$principal = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive -RunLevel Limited

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName -Action $action `
    -Trigger @($logonTrigger, $repeatTrigger) -Settings $settings -Principal $principal `
    -Description 'Per-user Codex Windows sandbox guard. Quarantines only malformed deny_read_acl_state.json after retrying validation; never resets ACLs or reads secrets.' | Out-Null

$registered = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
if ($registered.State -eq 'Disabled') {
    throw "Scheduled task '$taskName' was registered disabled."
}
if ($RunNow) {
    Start-ScheduledTask -TaskName $taskName -ErrorAction Stop
}

Write-Host "Registered '$taskName' at logon and every $IntervalMinutes minute(s), least privilege."
$registered | Select-Object TaskName, State | Format-Table -AutoSize
