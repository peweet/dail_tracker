<#
.SYNOPSIS
Hides Windows Terminal windows that Codex's Windows sandbox opens per shell command.

.DESCRIPTION
Codex's unelevated Windows sandbox launches a fresh, visible
"...\WindowsPowerShell\v1.0\powershell.exe" process per shell command it runs; the OS
default-terminal handoff opens each one as its own Windows Terminal window, titled with
the raw exe path (not a profile name like "Windows PowerShell"). This script polls for
Windows Terminal windows whose title matches that exact pattern and hides them with the
Win32 ShowWindow API. Hiding does not suspend or kill the process — its console/conpty
keeps running and Codex still reads its output normally.

Deliberately narrow match: only windows titled with the literal
"WindowsPowerShell\v1.0\powershell.exe" path are touched, so a Windows Terminal window
you open yourself (titled by its profile name, e.g. "Windows PowerShell") is never hidden.
This assumes Windows Terminal's windowingBehavior stays at its default "useNew" (one
process per spawned window); if that setting changes to reuse one window across tabs,
this script could hide a shared window along with a tab you're using.
#>

[CmdletBinding()]
param(
    [ValidateRange(50, 5000)]
    [int]$PollMilliseconds = 200
)

Add-Type @"
using System;
using System.Runtime.InteropServices;
public static class CodexWindowHiderNativeMethods {
    [DllImport("user32.dll")]
    public static extern bool ShowWindowAsync(IntPtr hWnd, int nCmdShow);
}
"@

$SW_HIDE = 0
$titlePattern = '*WindowsPowerShell\v1.0\powershell.exe*'

Write-Output "Watching for Codex sandbox terminal windows (poll every ${PollMilliseconds}ms). Ctrl+C to stop."

while ($true) {
    Get-Process -Name WindowsTerminal -ErrorAction SilentlyContinue | Where-Object {
        $_.MainWindowHandle -ne [IntPtr]::Zero -and $_.MainWindowTitle -like $titlePattern
    } | ForEach-Object {
        [CodexWindowHiderNativeMethods]::ShowWindowAsync($_.MainWindowHandle, $SW_HIDE) | Out-Null
    }
    Start-Sleep -Milliseconds $PollMilliseconds
}
