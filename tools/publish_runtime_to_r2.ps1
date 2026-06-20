<#
publish_runtime_to_r2.ps1 - publish the runtime data working set to Cloudflare R2.

The R2-lane publisher (sibling to tools/publish_data.py, which is the GIT-lane publisher that
commits the gold parquet so Streamlit Cloud redeploys). This pushes every `retention: runtime`
file in data/_meta/runtime_data_manifest.json to the `runtime/` prefix of the R2 bucket, so a
clean checkout can rehydrate via tools/fetch_runtime_data.py. See doc/DATA_DISTRIBUTION_PLAN.md.

How it differs from tools/backup_to_r2.ps1 (the append-only bronze/silver ARCHIVE):
  * Targets a separate `runtime/` prefix - the archive (bronze/, silver/) is never touched.
  * INCLUDES gold (the archive omits gold; git carries it today).
  * Hash-sync, NOT append-only: `rclone copy --checksum` (no --ignore-existing) so a refreshed
    parquet overwrites the stale R2 object. The whole point is to serve CURRENT data.

Steps:
  1. Regenerate the manifest (so the published set matches the code's actual reads).
  2. Integrity gate: tools/check_output_regressions.py --strict - the same whole-gold completeness
     check publish_data.py runs, so a corrupt/incomplete table can't reach R2.
  3. rclone copy the runtime set (via --files-from) into r2:<bucket>/runtime/.

Usage:
  tools/publish_runtime_to_r2.ps1            # regenerate manifest + gate + real copy
  tools/publish_runtime_to_r2.ps1 -DryRun    # regenerate manifest + gate + rclone --dry-run
  tools/publish_runtime_to_r2.ps1 -SkipGate  # DANGER: skip the completeness gate
#>

param(
    [switch]$DryRun,
    [switch]$SkipGate
)

$ErrorActionPreference = 'Stop'

# --- config: keep BUCKET in sync with tools/backup_to_r2.ps1 + doc/DATA_BACKUP.md ---
$remote = 'r2'
$bucket = 'dail-tracker-backup'
$prefix = 'runtime'

$root  = Split-Path -Parent $PSScriptRoot             # repo root (tools/ -> ..)
$py    = Join-Path $root '.venv\Scripts\python.exe'
$log   = Join-Path $root 'logs\standalone\publish_runtime_to_r2.log'
$rclog = Join-Path $root 'logs\standalone\publish_runtime_to_r2.rclone.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'

Set-Location $root

# Resolve rclone: prefer PATH, else the winget install location (mirrors backup_to_r2.ps1).
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

# --- step 1: regenerate the manifest ---
& $py tools\build_runtime_manifest.py
if ($LASTEXITCODE -ne 0) {
    Add-Content $log "$stamp  manifest=FAIL publish=skipped"
    Write-Error 'manifest build failed - nothing published.'
    exit $LASTEXITCODE
}

# --- step 2: integrity gate (whole-gold completeness vs the committed baseline) ---
if (-not $SkipGate) {
    & $py tools\check_output_regressions.py --strict
    if ($LASTEXITCODE -ne 0) {
        Add-Content $log "$stamp  gate=FAIL publish=skipped"
        Write-Error 'completeness gate failed (see output) - nothing published. Re-baseline only if intended.'
        exit 1
    }
} else {
    Write-Host 'WARNING - completeness gate SKIPPED (-SkipGate).'
}

# --- step 3: build the files-from list (runtime relpaths under data/) and copy ---
# rclone copies `data/<rel>` -> `r2:<bucket>/runtime/<rel>`, preserving the tree under the prefix.
$listFile = Join-Path $env:TEMP "runtime_files_from_$PID.txt"
& $py -c "import json,sys; m=json.load(open('data/_meta/runtime_data_manifest.json',encoding='utf-8')); sys.stdout.write('\n'.join(f['path'][len('data/'):] for f in m['files'] if f['retention']=='runtime'))" |
    Out-File -FilePath $listFile -Encoding utf8
$n = (Get-Content $listFile | Where-Object { $_.Trim() }).Count
Write-Host "publishing $n runtime file(s) -> ${remote}:${bucket}/${prefix}/"

# --checksum => decide re-upload by hash (not size/modtime); no --ignore-existing => overwrites land.
$common = @(
    '--files-from', $listFile, '--checksum', '--fast-list', '--transfers', '8', '--checkers', '16',
    '--stats-one-line', '--log-file', $rclog, '--log-level', 'INFO'
)
if ($DryRun) { $common += '--dry-run' }

& $rclone copy (Join-Path $root 'data') "${remote}:${bucket}/${prefix}" @common
$failed = ($LASTEXITCODE -ne 0)

Remove-Item $listFile -ErrorAction SilentlyContinue

$mode   = if ($DryRun) { 'dryrun' } else { 'live' }
$status = if ($failed)  { 'FAIL' }   else { 'ok' }
Add-Content $log "$stamp  mode=$mode  publish=$status  files=$n  rclone-log=$rclog"
if ($failed) {
    Write-Error 'rclone copy reported errors - see logs/standalone/publish_runtime_to_r2.rclone.log'
    exit 1
}
Write-Host "Runtime publish $mode complete -> ${remote}:${bucket}/${prefix} ($n files)."
exit 0
