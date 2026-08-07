<#
backup_restic_to_r2.ps1 - snapshot the trees that backup_to_r2.ps1 does NOT cover,
then mirror those restic repositories to Cloudflare R2.

WHY THIS EXISTS (2026-08-01 audit):
  tools/backup_to_r2.ps1 mirrors data/bronze + data/silver + data/raw_bq only. An audit
  found 2.58 GB across 4,064 files that was neither git-tracked nor in R2 - i.e. it
  existed on exactly one disk. The largest slice was planning/product (~1.2 GB), which
  is deliberately kept OUT of the public GitHub repo, so git can never be its backup.

  restic is used here (not plain rclone) because these trees need encryption at rest:
  planning/product is commercial IP going into cloud storage. restic also dedupes and
  compresses, which plain `rclone copy` does not.

TWO REPOSITORIES, TWO BUCKETS - deliberate isolation:
  restic_private  -> $bucketPrivate   planning/  (commercial IP)
  restic_sandbox  -> $bucketSandbox   sandbox / doc / ida / out trees
  Separate R2 buckets each with their own scoped API token, so a leaked token for the
  public-data backup grants NO access to the private tree. Separate repository passwords
  for the same reason at the encryption layer.

TIERING: the local repos under $backupRoot are tier 1 (fast restore, survives a bad
  refactor). The R2 copies are tier 2 (survives laptop loss). Local alone is NOT a
  backup - a dead disk takes both the source and the local repo.

PREREQS (one-time):
  1. restic on PATH or at the winget location (winget install --id restic.restic)
  2. rclone `r2` remote already configured - see doc/DATA_BACKUP.md
  3. Both R2 buckets created, and $pwFile populated (see doc/DISASTER_RECOVERY.md)

USAGE:
  tools/backup_restic_to_r2.ps1 -DryRun    # snapshot + check, no upload
  tools/backup_restic_to_r2.ps1            # snapshot + check + upload
  tools/backup_restic_to_r2.ps1 -SkipUpload
#>

param(
    [switch]$DryRun,
    [switch]$SkipUpload
)

$ErrorActionPreference = 'Stop'

# --- config ---
$backupRoot     = 'C:\Users\pglyn\dail_tracker_backup'
$pwFile         = Join-Path $backupRoot 'restic_passwords.txt'   # ACL-restrict this file
# Two rclone remotes, NOT one. The existing `r2` remote's API token is scoped to the
# `dail-tracker-backup` bucket (doc/DATA_BACKUP.md step 3), so it CANNOT write to the
# private bucket - and giving it account-wide reach would defeat the isolation this
# split exists for. Create a second remote holding the private bucket's own token:
#   rclone config create r2private s3 provider=Cloudflare region=auto `
#     access_key_id=... secret_access_key=... `
#     endpoint=https://<accountid>.r2.cloudflarestorage.com
$remoteSandbox  = 'r2'
$remotePrivate  = 'r2private'
$bucketPrivate  = 'dail-siting'          # created 2026-08-02; holds the planning/siting IP
$bucketSandbox  = 'dail-tracker-backup'      # sandbox trees share the existing bucket

$root  = Split-Path -Parent $PSScriptRoot
$log   = Join-Path $root 'logs\standalone\backup_restic_to_r2.log'
$rclog = Join-Path $root 'logs\standalone\backup_restic_to_r2.rclone.log'
New-Item -ItemType Directory -Force -Path (Split-Path $log) | Out-Null
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
Set-Location $root

# --- resolve restic (PATH first, then the winget install location) ---
$restic = (Get-Command restic -ErrorAction SilentlyContinue).Source
if (-not $restic) {
    $restic = (Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" `
        -Recurse -Filter 'restic*.exe' -ErrorAction SilentlyContinue |
        Select-Object -First 1).FullName
}
if (-not $restic) {
    Add-Content $log "$stamp  ERROR restic not found - winget install --id restic.restic"
    Write-Error 'restic is not installed / not found.'
    exit 3
}

# --- resolve rclone (same fallback as backup_to_r2.ps1) ---
$rclone = (Get-Command rclone -ErrorAction SilentlyContinue).Source
if (-not $rclone) {
    $rclone = (Get-ChildItem -Path "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" `
        -Recurse -Filter 'rclone.exe' -ErrorAction SilentlyContinue |
        Select-Object -First 1).FullName
}
if (-not $rclone -and -not $SkipUpload) {
    Add-Content $log "$stamp  ERROR rclone not found - see doc/DATA_BACKUP.md"
    Write-Error 'rclone is not installed / not found.'
    exit 3
}

if (-not (Test-Path $pwFile)) {
    Add-Content $log "$stamp  ERROR password file missing: $pwFile"
    Write-Error "Password file not found: $pwFile - see doc/DISASTER_RECOVERY.md"
    exit 3
}
$pwLines   = Get-Content $pwFile
$pwPrivate = $pwLines[($pwLines | Select-String -Pattern '^restic_private').LineNumber]
$pwSandbox = $pwLines[($pwLines | Select-String -Pattern '^restic_sandbox').LineNumber]

# Each job: repo dir, its password, the trees it snapshots, its destination bucket.
# NOTE the exclusions - __pycache__ is regenerable noise; .venv would balloon the repo.
# The sandbox job takes THE WHOLE REPO and subtracts what is deliberately covered
# elsewhere. Deliberately a subtraction, not a list: the 2026-08-01 audit found 2.58 GB
# stranded precisely because new directories kept appearing that no backup enumerated, and
# enumerating them again just reset the same trap. A denylist protects tomorrow's new
# top-level dir automatically; an allowlist cannot.
#
# What is subtracted from the sandbox job, and why each is safe to drop:
#   data\bronze|silver|raw_bq  - already mirrored by backup_to_r2.ps1 (the rclone lane)
#   planning, .git-siting, apps - go to the PRIVATE repo/bucket instead. Excluding them here
#                                is what keeps the commercial IP out of the public-data
#                                bucket; without these lines the isolation silently
#                                collapses. `.git-siting` is the SEPARATE git dir for the
#                                private siting repo (remote: peweet/pre-siting-private) -
#                                3.1 GB of private history that a whole-repo sweep picks up
#                                silently if you don't name it. This was caught by a
#                                -DryRun on 2026-08-02; it is exactly why you run one.
#                                `apps` (PublicSignal + planspec-demo, overlay-tracked) was
#                                added 2026-08-07: the overlay had grown beyond planning\
#                                and the sandbox sweep was carrying the commercial app's
#                                working files into the public-data bucket.
#   .cache                     - 2.4 GB of regenerable FTS indexes (precedent_fts.duckdb,
#                                project_fts.sqlite, text_fts.duckdb); rebuilt from data
#   .venv                      - regenerable with `uv sync`, and huge
#   node_modules, __pycache__  - regenerable build/import noise
# .git IS kept: it carries commits that may not be pushed yet, so it is the only copy of
# that work. Git objects are immutable, so restic dedupes it well run-to-run.
#
# ALWAYS -DryRun after editing these: if an exclude stops matching, the job silently tries
# to swallow 18 GB of bronze/silver, or leaks private IP into the public bucket.
$excludeSandbox = @(
    '--exclude', "$root\data\bronze",
    '--exclude', "$root\data\silver",
    '--exclude', "$root\data\raw_bq",
    '--exclude', "$root\planning",
    '--exclude', "$root\.git-siting",
    '--exclude', "$root\.git-publicsignal",
    '--exclude', "$root\apps",
    '--exclude', "$root\.cache",
    '--exclude', "$root\.venv",
    '--exclude', '**/node_modules/**',
    '--exclude', '**/__pycache__/**'
)

$jobs = @(
    @{ name='restic_private'; pw=$pwPrivate; remote=$remotePrivate; bucket=$bucketPrivate;
       tag='private-ip'; excl=@('--exclude','**/__pycache__/**');
       paths=@("$root\planning", "$root\.git-siting", "$root\.git-publicsignal", "$root\apps") },
    @{ name='restic_sandbox'; pw=$pwSandbox; remote=$remoteSandbox; bucket=$bucketSandbox;
       tag='whole-repo'; excl=$excludeSandbox; paths=@($root) }
)

$failed = 0
foreach ($job in $jobs) {
    $repoPath = Join-Path $backupRoot $job.name
    $env:RESTIC_REPOSITORY = $repoPath
    $env:RESTIC_PASSWORD   = $job.pw

    if (-not (Test-Path $repoPath)) {
        Write-Host "Initializing $($job.name) ..."
        & $restic init
        if ($LASTEXITCODE -ne 0) { $failed = 1; continue }
    }

    Write-Host "Snapshotting $($job.name) ..."
    $existing = @($job.paths | Where-Object { Test-Path $_ })
    & $restic backup @existing --tag $job.tag @($job.excl)
    if ($LASTEXITCODE -ne 0) { $failed = 1; continue }

    # Retention: keep enough history to recover from a mistake noticed weeks later,
    # without unbounded growth. --prune actually reclaims the space.
    & $restic forget --keep-daily 7 --keep-weekly 8 --keep-monthly 12 --prune
    if ($LASTEXITCODE -ne 0) { $failed = 1 }

    # Metadata-only check here; the full --read-data pass is expensive, run it
    # periodically by hand (see doc/DISASTER_RECOVERY.md).
    & $restic check
    if ($LASTEXITCODE -ne 0) { $failed = 1; continue }

    if ($SkipUpload) { continue }

    $common = @('--fast-list','--transfers','8','--checkers','16',
                '--stats-one-line','--log-file',$rclog,'--log-level','INFO')
    if ($DryRun) { $common += '--dry-run' }

    # `sync` (not `copy`) because a restic repo is a self-consistent object store:
    # after `forget --prune` some packs are legitimately GONE, and leaving stale packs
    # in the remote would bloat it and confuse `restic check` run against the remote.
    # Safe precisely because restic - not this script - decides what may be removed.
    Write-Host "Uploading $($job.name) -> $($job.remote):$($job.bucket) ..."
    & $rclone sync $repoPath "$($job.remote):$($job.bucket)/$($job.name)" @common
    if ($LASTEXITCODE -ne 0) { $failed = 1 }
}

$env:RESTIC_PASSWORD = $null
$mode   = if ($DryRun) { 'dryrun' } elseif ($SkipUpload) { 'local-only' } else { 'live' }
$status = if ($failed) { 'FAIL' } else { 'ok' }
Add-Content $log "$stamp  mode=$mode  result=$status  rclone-log=$rclog"
if ($failed) {
    Write-Error 'restic/rclone reported errors - see logs/standalone/backup_restic_to_r2.log'
    exit 1
}
Write-Host "restic backup $mode complete."
exit 0
