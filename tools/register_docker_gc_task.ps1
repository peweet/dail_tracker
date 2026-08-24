<#
Registers the weekly Docker disk reclaim (report + prune of unreferenced images,
week-old build cache, anonymous volumes). Runs Sundays 04:00 local and catches up
after sleep. Re-run this script to update the registration.

It deliberately does NOT compact docker_data.vhdx: that needs `wsl --shutdown`,
which stops every WSL distro on the box, not just Docker's. Run the compaction by
hand when you mean it:  python tools/docker_gc.py --compact

Remove with: Unregister-ScheduledTask -TaskName 'DailTracker-DockerGC' -Confirm:$false
#>

param(
    [switch]$RunNow
)

$ErrorActionPreference = 'Stop'
$taskName = 'DailTracker-DockerGC'
$repo = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$python = Join-Path $repo '.venv\Scripts\python.exe'
if (-not (Test-Path $python)) {
    throw "Interpreter not found at $python - create the venv before registering the task."
}
$script = Join-Path $repo 'tools\docker_gc.py'
if (-not (Test-Path $script)) {
    throw "Script not found at $script."
}
$action = New-ScheduledTaskAction -Execute $python -Argument "`"$script`" --reclaim" -WorkingDirectory $repo
$trigger = New-ScheduledTaskTrigger -Weekly -DaysOfWeek Sunday -At 4:00am
$settings = New-ScheduledTaskSettingsSet -StartWhenAvailable -DontStopIfGoingOnBatteries -ExecutionTimeLimit (New-TimeSpan -Hours 1)
Unregister-ScheduledTask -TaskName $taskName -Confirm:$false -ErrorAction SilentlyContinue
Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Settings $settings -Description 'Weekly Docker disk reclaim: prunes unreferenced images, week-old build cache and anonymous volumes. Never compacts the vhdx (that needs wsl --shutdown) and never removes named volumes.' | Out-Null
$registered = Get-ScheduledTask -TaskName $taskName -ErrorAction Stop
if ($registered.State -eq 'Disabled') {
    throw "Scheduled task '$taskName' was registered disabled."
}
if ($RunNow) {
    Start-ScheduledTask -TaskName $taskName -ErrorAction Stop
}
$registered | Select-Object TaskName, State | Format-Table -AutoSize
