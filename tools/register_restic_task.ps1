<#
register_restic_task.ps1 - register (or refresh) the daily restic backup of the trees
that backup_to_r2.ps1 does NOT cover, as a per-user Windows Scheduled Task. No admin
rights needed: it runs under the current user when logged on.

Task name : DailTracker-BackupRestic
Schedule  : every day at 03:00 local - one hour AFTER DailTracker-BackupR2 so the two
            lanes never compete for bandwidth or for the rclone binary.
Action    : powershell -File tools/backup_restic_to_r2.ps1

WHY THIS EXISTS: the 2026-08-01 audit found 2.58 GB on exactly one disk, and the restic
lane that fixed it was hand-run. A hand-run backup decays the moment you stop thinking
about it - and planning/ is the most actively-changing tree it covers. Scheduling it
removes the human from the freshness loop, which is the whole point.

Daily matches the rclone lane's recovery-point objective. Restic is incremental and
deduplicating, and -StartWhenAvailable catches up after a sleeping laptop.

PREREQ: C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt must exist (the script
exits 3 without it). Unattended runs cannot prompt for a password.

Re-run this script any time to update the schedule (it unregisters first).
Remove with:  Unregister-ScheduledTask -TaskName 'DailTracker-BackupRestic' -Confirm:$false
#>

$ErrorActionPreference = 'Stop'
$taskName = 'DailTracker-BackupRestic'
$root     = Split-Path -Parent $PSScriptRoot
$wrapper  = Join-Path $root 'tools\backup_restic_to_r2.ps1'

if (-not (Test-Path $wrapper)) { Write-Error "Not found: $wrapper"; exit 1 }

$pwFile = 'C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt'
if (-not (Test-Path $pwFile)) {
    Write-Warning "Password file missing: $pwFile - the task will exit 3 until it exists."
}

$action  = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$wrapper`"" `
    -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Daily -At 3:00am
# Same settings rationale as register_backup_task.ps1; 2h ceiling covers a full re-upload.
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 2)

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings `
    -Description 'Daily restic snapshot + R2 upload of planning/ and the sandbox/doc/ida/out trees.' | Out-Null

Write-Host "Registered scheduled task '$taskName' (daily 03:00)."
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName, State | Format-Table -AutoSize
