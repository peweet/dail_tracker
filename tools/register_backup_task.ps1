<#
register_backup_task.ps1 — register (or refresh) the weekly off-box data backup
as a per-user Windows Scheduled Task. No admin rights needed: it runs under the
current user when logged on.

Task name : DailTracker-BackupR2
Schedule  : every Sunday at 02:00 local (idempotent; -StartWhenAvailable catches
            up if the laptop was asleep — better late than never for a backup).
Action    : powershell -File tools/backup_to_r2.ps1

Weekly, not daily: bronze/silver change in bursts when you run ETLs, not
continuously, and rclone sync is incremental so a missed week costs nothing.
Run tools/backup_to_r2.ps1 by hand right after a big ingest if you don't want to
wait for Sunday.

Re-run this script any time to update the schedule (it unregisters first).
Remove with:  Unregister-ScheduledTask -TaskName 'DailTracker-BackupR2' -Confirm:$false
#>

$ErrorActionPreference = 'Stop'
$taskName = 'DailTracker-BackupR2'
$root     = Split-Path -Parent $PSScriptRoot
$wrapper  = Join-Path $root 'tools\backup_to_r2.ps1'

$action  = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$wrapper`"" `
    -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 2:00am
# Run on AC or battery; catch up a missed run; allow up to 2h for a large first sync.
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Description 'Weekly rclone sync of data/bronze + data/silver to Cloudflare R2.' | Out-Null

Write-Host "Registered scheduled task '$taskName' (weekly Sun 02:00)."
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName, State |
    Format-Table -AutoSize
