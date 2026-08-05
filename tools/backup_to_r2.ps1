<#
backup_to_r2.ps1 - mirror the raw + derived data trees to Cloudflare R2.

Steps (both idempotent, safe to run by hand):
  1. Regenerate data/_meta/backup_manifest.tsv (content hashes; lets a restore be
     verified and shows what changed). Skip with -SkipManifest.
  2. rclone sync data/bronze, data/silver and data/raw_bq into the R2 bucket,
     retaining displaced remote objects in a dated version archive.

Version-preserving by design: `rclone sync --backup-dir` makes the normal R2 tree
match the current local tree, but moves every overwritten or deleted remote object
to `versions/<UTC run id>/` first. This matters for canonical silver outputs and
any upstream file that changes at the same path: `--ignore-existing` would leave
the old bytes in place forever.

The 9 GB of bronze/silver is NOT in git (correctly gitignored); this is its only
off-box copy. Code, the curated data/_meta files, and the runtime gold slice are
already backed up by `git push`.

Prereqs (one-time): install rclone and create an `r2` remote - see doc/DATA_BACKUP.md.
Register as a weekly scheduled task with:  tools/register_backup_task.ps1
RESTORE after a laptop loss: see doc/DISASTER_RECOVERY.md

Usage:
  tools/backup_to_r2.ps1                 # manifest + real copy
  tools/backup_to_r2.ps1 -DryRun         # manifest + rclone --dry-run (no upload)
  tools/backup_to_r2.ps1 -SkipManifest   # copy only
#>

param(
    [switch]$DryRun,
    [switch]$SkipManifest
)

$ErrorActionPreference = 'Stop'

# --- config: change BUCKET if you named it differently in doc/DATA_BACKUP.md ---
$remote = 'r2'
$bucket = 'dail-tracker-backup'

$root  = Split-Path -Parent $PSScriptRoot             # repo root (tools/ -> ..)
$py    = Join-Path $root '.venv\Scripts\python.exe'
$log   = Join-Path $root 'logs\standalone\backup_to_r2.log'
$rclog = Join-Path $root 'logs\standalone\backup_to_r2.rclone.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
$runId = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHHmmssZ')

Set-Location $root

# Resolve rclone: prefer PATH, else fall back to the winget install location. A
# freshly-installed rclone only updates PATH for NEW shells, so a same-session run
# (or a Task Scheduler env that predates the install) may not see it on PATH yet.
$rclone = (Get-Command rclone -ErrorAction SilentlyContinue).Source
if (-not $rclone) {
    $rclone = (Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" `
        -Recurse -Filter 'rclone.exe' -ErrorAction SilentlyContinue |
        Select-Object -First 1).FullName
}
if (-not $rclone) {
    Add-Content $log "$stamp  ERROR rclone not found - see doc/DATA_BACKUP.md"
    Write-Error 'rclone is not installed / not found. See doc/DATA_BACKUP.md.'
    exit 3
}

# --- step 1: manifest ---
if (-not $SkipManifest) {
    & $py tools\data_manifest.py
    if ($LASTEXITCODE -ne 0) {
        Add-Content $log "$stamp  manifest=FAIL copy=skipped"
        exit $LASTEXITCODE
    }
}

# --- step 2: sync each tree, preserving every displaced remote object ---
# R2 has no object versioning. `--backup-dir` is the version archive: rclone moves
# a changed/deleted destination object there before writing the current source bytes.
$common = @(
    '--fast-list', '--transfers', '8', '--checkers', '16',
    '--stats-one-line', '--log-file', $rclog, '--log-level', 'INFO'
)
if ($DryRun) { $common += '--dry-run' }

$failed = 0
foreach ($tree in 'bronze', 'silver', 'raw_bq') {
    $src = Join-Path $root "data\$tree"
    if (-not (Test-Path $src)) { continue }
    $dest = "${remote}:${bucket}/$tree"
    $versionDest = "${remote}:${bucket}/versions/$runId/$tree"
    & $rclone sync $src $dest '--backup-dir' $versionDest @common
    if ($LASTEXITCODE -ne 0) { $failed = 1 }
}

# Re-scan without writing the baseline, so a success means the manifest still
# describes the copied source rather than a tree that changed mid-backup.
if (-not $DryRun -and -not $SkipManifest) {
    & $py tools\data_manifest.py --check
    if ($LASTEXITCODE -ne 0) {
        Add-Content $log "$stamp  manifest=CHANGED_DURING_BACKUP copy=unverified"
        Write-Error 'Source data changed while the backup ran; rerun the backup to capture a stable manifest.'
        exit 1
    }
}

if (-not $DryRun) {
    foreach ($tree in 'bronze', 'silver', 'raw_bq') {
        $src = Join-Path $root "data\$tree"
        if (-not (Test-Path $src)) { continue }
        & $rclone check $src "${remote}:${bucket}/$tree" '--size-only' '--one-way' '--fast-list' `
            '--log-file' $rclog '--log-level' 'INFO'
        if ($LASTEXITCODE -ne 0) { $failed = 1 }
    }
}

# Keep the exact baseline with the backup itself. A fresh-machine recovery must
# not depend on an uncommitted local manifest having been pushed to GitHub.
if (-not $DryRun -and -not $SkipManifest -and -not $failed) {
    $manifest = Join-Path $root 'data\_meta\backup_manifest.tsv'
    & $rclone copyto $manifest "${remote}:${bucket}/manifests/$runId.tsv" @common
    if ($LASTEXITCODE -ne 0) { $failed = 1 }
    if (-not $failed) {
        & $rclone copyto $manifest "${remote}:${bucket}/manifests/latest.tsv" @common
        if ($LASTEXITCODE -ne 0) { $failed = 1 }
    }
}

$mode   = if ($DryRun) { 'dryrun' } else { 'live' }
$status = if ($failed)  { 'FAIL' }   else { 'ok' }
Add-Content $log "$stamp  mode=$mode  copy=$status  rclone-log=$rclog"
if ($failed) {
    Write-Error 'rclone backup reported errors - see logs/standalone/backup_to_r2.rclone.log'
    exit 1
}
Write-Host "Backup $mode complete -> ${remote}:${bucket} (bronze + silver + raw_bq; run=$runId)."
exit 0
