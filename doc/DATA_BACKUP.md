---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-08-14
supersedes: []
read_when: setting up or verifying the R2 backup of data/bronze and data/silver before it's lost
key: PLAN|LIVE|infra
---

# Data backup — off-box durability for the 9 GB of raw + derived data

> **Two backup lanes:** this document covers the R2 data lane. The encrypted restic
> lane separately protects `planning/` and the rest of the checkout; register it
> with `tools\register_restic_task.ps1` and restore it as described in
> [DISASTER_RECOVERY.md](DISASTER_RECOVERY.md).

## The problem this solves

If this laptop is destroyed, what is lost?

| Asset | Where it lives now | Safe? |
|---|---|---|
| Code | GitHub (`peweet/dail_tracker`) | ✅ `git push` |
| Curated reference (`data/_meta/*.csv`) | git-tracked | ✅ in repo |
| Runtime gold slice (~16 MB) | git-tracked for Streamlit Cloud | ✅ in repo |
| **`data/bronze/` (~7.4 GB raw captures)** | **this disk only** | ❌ |
| **`data/silver/` (~1.7 GB derived)** | **this disk only** | ❌ |

The bronze tree is the exposure. Most of it comes from sources that **mutate or
vanish** — council and public-body procurement PDFs are re-published in place, and
some SIPO candidate documents already return 403. Once gone upstream, a lost local
copy is gone for good. The silver OCR layer is reproducible in theory but
PaddleOCR hard-crashes this box, so regenerating it is genuinely painful.

This setup mirrors `data/bronze`, `data/silver`, and `data/raw_bq` to **Cloudflare
R2** — first 10 GB free, zero egress fees, S3-compatible.

The backup is **version-preserving**: `rclone sync --backup-dir` updates the normal
restore path, but moves every displaced remote object to `versions/<UTC run id>/`
first. This preserves a source or silver output that changes at the same path;
`--ignore-existing` would not. R2 object versioning is unavailable, so do not remove
the `versions/` archive by hand. A dated manifest is uploaded to `manifests/` for
each successful run.

## One-time setup

### 1. Install rclone

```powershell
winget install Rclone.Rclone
# or: scoop install rclone   /   choco install rclone
rclone version   # confirm it's on PATH
```

### 2. Create the R2 bucket

1. Cloudflare dashboard → **R2** → *Create bucket* → name it **`dail-tracker-backup`**
   (must match `$bucket` in [tools/backup_to_r2.ps1](../tools/backup_to_r2.ps1)).
2. R2 does not provide ordinary object versioning. After the first backup, add a
   Bucket Lock rule for `versions/` and another for `manifests/archive/` objects, with
   a retention at least as long as the recovery window. Do **not** lock the mutable
   current data paths or `manifests/latest.tsv`: `rclone sync` must overwrite them.
   Do not lock active Restic repository prefixes either, because Restic pruning
   legitimately deletes old packs.

### 3. Create an S3 API token

R2 → **Manage R2 API Tokens** → *Create token* → **Object Read & Write**, scoped to
this bucket. Copy the **Access Key ID**, **Secret Access Key**, and your
**account ID** (the S3 endpoint is `https://<accountid>.r2.cloudflarestorage.com`).

### 4. Configure the rclone remote

```powershell
rclone config
#  n) new remote
#  name> r2                      <-- must match $remote in backup_to_r2.ps1
#  Storage> s3
#  provider> Cloudflare
#  access_key_id>  <Access Key ID>
#  secret_access_key>  <Secret Access Key>
#  region>  auto
#  endpoint>  https://<accountid>.r2.cloudflarestorage.com
#  (accept defaults for the rest)
```

Verify: `rclone lsd r2:dail-tracker-backup` should return cleanly (empty is fine).

### 5. First backup + schedule it

```powershell
tools\backup_to_r2.ps1 -DryRun     # see what would upload, no transfer
tools\backup_to_r2.ps1             # the real first copy (~9 GB, one-off)
tools\register_backup_task.ps1     # daily 02:00 thereafter
```

## What runs each day

[tools/backup_to_r2.ps1](../tools/backup_to_r2.ps1) does two things:

1. **`python tools/data_manifest.py`** — rewrites the git-tracked
   `data/_meta/backup_manifest.tsv` (one `sha256<TAB>size<TAB>relpath` line per
   bronze/silver file). It's the restore-verification record and a change log:
   `git diff` on it shows exactly which files were added or changed since last run.
   Optional — skip with `-SkipManifest` if you want the leanest possible backup.
2. **`rclone sync --backup-dir`** of `data/bronze`, `data/silver`, and
   `data/raw_bq` into `r2:dail-tracker-backup/`. The normal paths are current; prior
   versions displaced by an update are retained under `versions/<UTC run id>/`.
   The script then size-checks every current source path against R2 and uploads its
   matching SHA-256 manifest under `manifests/`.

Logs: a one-line run summary at `logs/standalone/backup_to_r2.log`; full rclone
transfer detail at `logs/standalone/backup_to_r2.rclone.log`.

## Restoring after a laptop loss

> Full standalone runbook: **[DISASTER_RECOVERY.md](DISASTER_RECOVERY.md)**. Quick version:

On a fresh machine:

```powershell
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
# ... recreate the venv (uv sync), reinstall + reconfigure rclone (steps 1 & 4) ...
rclone copy r2:dail-tracker-backup/bronze data\bronze
rclone copy r2:dail-tracker-backup/silver data\silver
rclone copy r2:dail-tracker-backup/raw_bq data\raw_bq
rclone copyto r2:dail-tracker-backup/manifests/latest.tsv data\_meta\backup_manifest.tsv
python tools\data_manifest.py --check   # exit 0 == every file matches the backup hashes
```

The `--check` step is the proof of a clean restore: it re-hashes the restored
trees and fails if any file differs from the committed manifest.

Older bytes displaced by a refresh are in `versions/<UTC run id>/`; recover a
specific older object from that archive rather than replacing the current restore
path blindly.

## Cost

~9 GB sits at or just over R2's free 10 GB tier; beyond that it is **$0.015/GB-month**
(approx $0.15/mo per extra 10 GB) with **zero egress fees**. The bucket only grows
as new captures accumulate (old objects are never overwritten); if it ever gets
large, add an R2 **Object Lifecycle** rule to expire objects older than N days.

## Scope notes

- **Not backed up here:** `data/gold/` and the rest of `data/silver` that is cheap
  to rebuild from code + bronze. The runtime gold slice and curated `_meta` are
  already in git. If you'd rather have a fully self-contained restore without git,
  add `_meta` and `gold` as extra `foreach` trees in `backup_to_r2.ps1`.
- This is a **version-preserving mirror**. The ordinary paths are current and the
  dated `versions/` archive retains remote objects displaced by an update or delete.
