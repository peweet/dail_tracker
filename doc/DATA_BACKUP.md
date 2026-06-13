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
free, zero egress fees, S3-compatible — with **object versioning on**, so a
re-published PDF keeps its old bytes instead of silently overwriting them.

## One-time setup

### 1. Install rclone

```powershell
winget install Rclone.Rclone
# or: scoop install rclone   /   choco install rclone
rclone version   # confirm it's on PATH
```

### 2. Create the R2 bucket (with versioning)

1. Cloudflare dashboard → **R2** → *Create bucket* → name it **`dail-tracker-backup`**
   (must match `$bucket` in [tools/backup_to_r2.ps1](../tools/backup_to_r2.ps1)).
2. Open the bucket → **Settings** → **Object versioning** → **Enable**.
   *This is the part that protects against in-place PDF rewrites — do not skip it.*

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
tools\backup_to_r2.ps1             # the real first sync (~9 GB, one-off)
tools\register_backup_task.ps1     # weekly Sun 02:00 thereafter
```

## What runs each week

[tools/backup_to_r2.ps1](../tools/backup_to_r2.ps1) does two things:

1. **`python tools/data_manifest.py`** — rewrites the git-tracked
   `data/_meta/backup_manifest.tsv` (one `sha256<TAB>size<TAB>relpath` line per
   bronze/silver file) and logs a drift summary. Because the manifest is committed,
   `git diff` on it is a precise log of *which source files changed* — a mutated
   council PDF shows up as a changed sha line, not a silent overwrite. Commit it
   periodically so the change history persists.
2. **`rclone sync`** of `data/bronze` and `data/silver` to `r2:dail-tracker-backup/`.

Logs: a one-line run summary at `logs/standalone/backup_to_r2.log`; full rclone
transfer detail at `logs/standalone/backup_to_r2.rclone.log`.

## Restoring after a laptop loss

On a fresh machine:

```powershell
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
# ... recreate the venv (uv sync), reinstall + reconfigure rclone (steps 1 & 4) ...
rclone sync r2:dail-tracker-backup/bronze data\bronze
rclone sync r2:dail-tracker-backup/silver data\silver
python tools\data_manifest.py --check   # exit 0 == every file matches the committed hashes
```

The `--check` step is the proof of a clean restore: it re-hashes the restored
trees and fails if any file differs from the committed manifest.

### Recovering a specific old version of a mutated PDF

Because versioning is on, R2 still holds prior bytes after an overwrite:

```powershell
rclone --s3-versions ls r2:dail-tracker-backup/bronze/pdfs/la_procurement/cork_city/
# copy a specific dated version back:
rclone --s3-versions copy "r2:dail-tracker-backup/bronze/pdfs/.../file-v2026-05-01-....pdf" .\restore\
```

## Cost

~9 GB sits at or just over R2's free 10 GB tier; beyond that it is **$0.015/GB-month**
(≈ $0.15/mo per extra 10 GB) with **zero egress fees**. Versioned history adds a
little as PDFs churn — trim old versions with a bucket lifecycle rule (e.g. expire
non-current versions after 365 days) if it ever grows.

## Scope notes

- **Not backed up here:** `data/gold/` and the rest of `data/silver` that is cheap
  to rebuild from code + bronze. The runtime gold slice and curated `_meta` are
  already in git. If you'd rather have a fully self-contained restore without git,
  add `_meta` and `gold` as extra `foreach` trees in `backup_to_r2.ps1`.
- This is a **mirror + version history**, not a snapshot archive. For a true
  point-in-time archive of the irreplaceable raw PDFs, a second cold bucket with
  `rclone copy` (never deletes) is the next step up — not needed yet at this scale.
