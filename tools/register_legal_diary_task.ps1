<#
register_legal_diary_task.ps1 — register (or refresh) the daily refresh bundle
(Legal Diary + the year-round / frequently-updating pipeline chains; see the
header of run_legal_diary_daily.ps1) as a per-user Windows Scheduled Task. No
admin rights needed: it runs under the current user when logged on.

Task name : DailTracker-LegalDiary
Schedule  : every day at 07:00 local (the diary for a court day is published the
            prior evening / early morning; daily is harmless — the poller is
            idempotent and exits 0 on "already current", and every bundled chain
            is a no-op when its source has not changed).
Action    : powershell -File tools/run_legal_diary_daily.ps1

Re-run this script any time to update the schedule (it unregisters first).
Remove with:  Unregister-ScheduledTask -TaskName 'DailTracker-LegalDiary' -Confirm:$false
#>

$ErrorActionPreference = 'Stop'
$taskName = 'DailTracker-LegalDiary'
$root     = Split-Path -Parent $PSScriptRoot
$wrapper  = Join-Path $root 'tools\run_legal_diary_daily.ps1'

$action  = New-ScheduledTaskAction -Execute 'powershell.exe' `
    -Argument "-NoProfile -ExecutionPolicy Bypass -File `"$wrapper`"" `
    -WorkingDirectory $root
$trigger = New-ScheduledTaskTrigger -Daily -At 7:00am
# Run on AC or battery; allow start if a scheduled run was missed (laptop asleep).
# Time limit raised to 3h: the bundle now runs bootstrap/members/legislation/iris/
# ted chains too, each with up to 3 retries, so 20 min is no longer enough.
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable `
    -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries `
    -ExecutionTimeLimit (New-TimeSpan -Hours 3)

Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger `
    -Settings $settings -Description 'Daily refresh bundle: Legal Diary + year-round pipeline chains (bootstrap/members/legislation/iris/ted) -> local gold; no publish.' | Out-Null

Write-Host "Registered scheduled task '$taskName' (daily 07:00)."
Get-ScheduledTask -TaskName $taskName | Select-Object TaskName, State |
    Format-Table -AutoSize
