# Data backup — off-box durability for the 9 GB of raw + derived data

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

This setup mirrors `data/bronze` + `data/silver` to **Cloudflare R2** — first 10 GB
free, zero egress fees, S3-compatible.

The backup is **append-only**: `rclone copy --ignore-existing` uploads anything not
already in the bucket and never overwrites or deletes what is. Our captures are
date/run-stamped (e.g. `companies_20260504.csv`), so when a council re-publishes a
PDF it arrives under a new filename and lands as a new object next to the old one —
nothing is lost, and we don't need object versioning (which R2 lacks anyway:
`PutBucketVersioning` is [unimplemented](https://developers.cloudflare.com/r2/api/s3/api/)).
Leave **Bucket Lock Rules** off.

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
2. That's it — no toggles to set. Leave **Object versioning** (R2 has none) and
   **Bucket Lock Rules** alone; the append-only `copy` model needs neither.

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
tools\register_backup_task.ps1     # weekly Sun 02:00 thereafter
```

## What runs each week

[tools/backup_to_r2.ps1](../tools/backup_to_r2.ps1) does two things:

1. **`python tools/data_manifest.py`** — rewrites the git-tracked
   `data/_meta/backup_manifest.tsv` (one `sha256<TAB>size<TAB>relpath` line per
   bronze/silver file). It's the restore-verification record and a change log:
   `git diff` on it shows exactly which files were added or changed since last run.
   Optional — skip with `-SkipManifest` if you want the leanest possible backup.
2. **`rclone copy --ignore-existing`** of `data/bronze` and `data/silver` into
   `r2:dail-tracker-backup/`. Additive only: new files go up, existing objects are
   never touched, nothing is deleted.

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
python tools\data_manifest.py --check   # exit 0 == every file matches the committed hashes
```

The `--check` step is the proof of a clean restore: it re-hashes the restored
trees and fails if any file differs from the committed manifest.

Because the backup is append-only, **every version of a file you ever captured is
already in the bucket under its own (date-stamped) name** — recovering an old
council PDF is just copying that specific object back; there is no separate archive
to dig through.

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
- This is an **append-only mirror**: it accumulates every capture and never deletes.
  Combined with the date-stamped capture names, that gives you full history for free
  without object versioning.
