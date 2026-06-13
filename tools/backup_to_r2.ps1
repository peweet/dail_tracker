<#
backup_to_r2.ps1 — mirror the raw + derived data trees to Cloudflare R2.

Steps (both idempotent, safe to run by hand):
  1. Regenerate data/_meta/backup_manifest.tsv (content hashes + drift report).
     The drift lines flag any source PDF that was re-published in place.
  2. rclone sync  data/bronze  and  data/silver  to the R2 bucket.

Why sync (not copy): the bucket has OBJECT VERSIONING enabled (see
doc/DATA_BACKUP.md), so when a council/SIPO PDF changes, sync overwrites the
live object but R2 retains the previous bytes as a prior version — you get a
current mirror AND full history without accumulating dead files. Do NOT run this
against a non-versioned bucket; an upstream deletion would then erase your only
copy of an ephemeral source.

The 9 GB of bronze/silver is NOT in git (correctly gitignored); this is its only
off-box copy. Code, the curated data/_meta files, and the runtime gold slice are
already backed up by `git push`.

Prereqs (one-time): install rclone and create an `r2` remote — see doc/DATA_BACKUP.md.
Register as a weekly scheduled task with:  tools/register_backup_task.ps1

Usage:
  tools/backup_to_r2.ps1                 # manifest + real sync
  tools/backup_to_r2.ps1 -DryRun         # manifest + rclone --dry-run (no upload)
  tools/backup_to_r2.ps1 -SkipManifest   # sync only
#>

param(
    [switch]$DryRun,
    [switch]$SkipManifest
)

$ErrorActionPreference = 'Stop'

# --- config: change BUCKET if you named it differently in doc/DATA_BACKUP.md ---
$remote = 'r2'
$bucket = 'dail-tracker-backup'

$root = Split-Path -Parent $PSScriptRoot              # repo root (tools/ -> ..)
$py   = Join-Path $root '.venv\Scripts\python.exe'
$log  = Join-Path $root 'logs\standalone\backup_to_r2.log'
$rclog = Join-Path $root 'logs\standalone\backup_to_r2.rclone.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

Set-Location $root

if (-not (Get-Command rclone -ErrorAction SilentlyContinue)) {
    Add-Content $log "$stamp  ERROR rclone not on PATH — see doc/DATA_BACKUP.md"
    Write-Error 'rclone is not installed / not on PATH. See doc/DATA_BACKUP.md.'
    exit 3
}

# --- step 1: manifest + drift ---
if (-not $SkipManifest) {
    & $py tools\data_manifest.py
    if ($LASTEXITCODE -ne 0) {
        Add-Content $log "$stamp  manifest=FAIL sync=skipped"
        exit $LASTEXITCODE
    }
}

# --- step 2: sync each tree ---
$common = @(
    '--fast-list', '--transfers', '8', '--checkers', '16',
    '--stats-one-line', '--log-file', $rclog, '--log-level', 'INFO'
)
if ($DryRun) { $common += '--dry-run' }

$failed = 0
foreach ($tree in 'bronze', 'silver') {
    $src = Join-Path $root "data\$tree"
    if (-not (Test-Path $src)) { continue }
    & rclone sync $src "${remote}:${bucket}/$tree" @common
    if ($LASTEXITCODE -ne 0) { $failed = 1 }
}

$mode = if ($DryRun) { 'dryrun' } else { 'live' }
Add-Content $log "$stamp  mode=$mode  sync=$(if ($failed) {'FAIL'} else {'ok'})  rclone-log=$rclog"
if ($failed) {
    Write-Error 'rclone sync reported errors — see logs/standalone/backup_to_r2.rclone.log'
    exit 1
}
Write-Host "Backup $mode complete -> ${remote}:${bucket} (bronze + silver)."
exit 0
