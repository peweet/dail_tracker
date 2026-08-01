---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-06-13
supersedes: []
read_when: the dev laptop is lost or destroyed and you need to restore a working machine from GitHub + R2
key: PLAN|LIVE|infra
---

# Disaster recovery — "the laptop died, now what?"

Read this first. It tells you where everything lives and how to get back to a
working machine. The backup side (what runs, how it's configured) is in
[DATA_BACKUP.md](DATA_BACKUP.md) — this doc is the **restore** side.

> **Reassurance:** nothing on the laptop is a single point of failure. Code is on
> GitHub, data is in Cloudflare R2 (not your disk), and the R2 access keys can be
> re-minted from the Cloudflare dashboard anytime. A destroyed laptop loses nothing
> permanently.

## Where everything is

| What | Backed up where | How to get it back |
|---|---|---|
| Code, curated `data/_meta/*`, runtime gold slice (~16 MB) | GitHub `peweet/dail_tracker` | `git clone` |
| `data/_meta/backup_manifest.tsv` (restore-verification baseline) | GitHub (committed) | comes with the clone |
| `data/bronze/` (~7.4 GB raw captures) | R2 bucket `dail-tracker-backup/bronze/` | `rclone copy` (below) |
| `data/silver/` (~1.7 GB derived) | R2 bucket `dail-tracker-backup/silver/` | `rclone copy` (below) |
| `data/gold/` (beyond the git slice), rest of `data/silver` | **not backed up** — regenerable | rebuild via the pipeline from bronze |
| `dailtracker.ie` custom domain (Cloudflare Worker + DNS) | GitHub (`deploy/cloudflare/`) + your Cloudflare account | re-run the one-time setup in [CUSTOM_DOMAIN_CLOUDFLARE.md](CUSTOM_DOMAIN_CLOUDFLARE.md) — DNS/Worker config isn't itself a file to restore, just re-created from that doc |
| `planning/` (~1.2 GB, kept out of the public repo — see `.gitignore`) | restic repo `restic_private` → R2 bucket `dail-tracker-private` | `restic restore` (below) |
| Sandbox / `doc` / `ida` / `out` trees (~760 MB, neither git nor the rclone mirror) | restic repo `restic_sandbox` → R2 bucket `dail-tracker-backup` | `restic restore` (below) |

R2 account ID: `dda75db5c9db02954a7b45e69052c742`
S3 endpoint: `https://dda75db5c9db02954a7b45e69052c742.r2.cloudflarestorage.com`

## What you need before starting

- Your **GitHub** login (to clone).
- Your **Cloudflare** login. The R2 **Access Key ID + Secret** are ideal to have
  saved (password manager), but if you lost them: log into Cloudflare → R2 →
  **{ } API → Manage API tokens → Create API token** (Account token, *Object Read &
  Write*, scoped to `dail-tracker-backup`) to mint fresh ones. The bucket and its
  contents are untouched by the laptop loss.

## Scenario A — total loss, fresh machine

```powershell
# 1. Prerequisites
winget install Git.Git Rclone.Rclone        # + Python / uv as you normally install

# 2. Code + curated data + the manifest (all from git)
git clone https://github.com/peweet/dail_tracker.git
cd dail_tracker
uv sync                                       # recreate the venv

# 3. Reconnect rclone to R2 (paste saved keys, or freshly-minted ones)
rclone config create r2 s3 provider=Cloudflare region=auto `
  access_key_id=PASTE_ACCESS_KEY_ID `
  secret_access_key=PASTE_SECRET_ACCESS_KEY `
  endpoint=https://dda75db5c9db02954a7b45e69052c742.r2.cloudflarestorage.com
rclone lsd r2:dail-tracker-backup            # sanity check: lists cleanly

# 4. Pull the data back (R2 egress is free)
rclone copy r2:dail-tracker-backup/bronze data\bronze
rclone copy r2:dail-tracker-backup/silver data\silver

# 5. PROVE the restore is bit-perfect — re-hashes every file vs the committed manifest
python tools\data_manifest.py --check        # exit 0 = all match; exit 1 = something differs
```

Step 5 is the whole point of the manifest: exit 0 means all ~29k files came back
byte-identical to what you backed up.

## Scenario A2 — restoring the restic-backed trees (`planning/`, sandbox, `doc`, `ida`, `out`)

These are **not** covered by the `rclone copy` in step 4 — they live in encrypted restic
repositories. You need the repository password from your password manager.

```powershell
winget install --id restic.restic

# Pull the repository down from R2 first (it is just a directory of files)
rclone copy r2:dail-tracker-private/restic_private  C:\restore\restic_private
rclone copy r2:dail-tracker-backup/restic_sandbox   C:\restore\restic_sandbox

# Restore. --target is a PREFIX: restic recreates the original absolute path beneath it,
# e.g. C:\restore\out\c\Users\...\dail_extractor\planning - move it into place afterwards.
$env:RESTIC_PASSWORD = '<private password from your password manager>'
restic -r C:\restore\restic_private restore latest --target C:\restore\out

restic -r C:\restore\restic_private snapshots     # list what is available
restic -r C:\restore\restic_private check         # verify repo integrity
```

Restore a single file instead of everything with
`restic -r <repo> restore latest --target <dir> --include "*/planning/product/<file>"`.

**Verify a restore properly** with `restic check --read-data` — it re-reads and re-hashes
every pack rather than just the metadata. It is slow, so it is not in the weekly script;
run it by hand after a real restore, and periodically as a drill.

## Scenario B — repo is fine, only `data/` is lost or corrupted

Skip steps 1–3. Run steps 4 and 5 only.

## Restore a single file (accidental delete)

```powershell
rclone copy "r2:dail-tracker-backup/bronze/pdfs/la_procurement/cork_city/<file>.pdf" `
  .\data\bronze\pdfs\la_procurement\cork_city\
```

## Re-arm the scheduled tasks on the new machine

Once restored and verified, both scheduled tasks this project depends on need
re-registering — not just the backup one:

```powershell
tools\register_backup_task.ps1                # DailTracker-BackupR2, weekly Sun 02:00
tools\register_legal_diary_task.ps1            # DailTracker-LegalDiary
tools\backup_restic_to_r2.ps1 -SkipUpload      # re-seed the restic repos locally
```

The restic lane has **no scheduled task yet** — it is run by hand. Recreate
`C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt` from your password manager
first (format: a line starting `restic_private`, then the password on the next line;
same for `restic_sandbox`) or the script exits 3.

Check `Get-ScheduledTask | Where-Object {$_.TaskName -like "DailTracker-*"}` afterward
— both should show `Ready`.

## Notes

- The backup is **append-only** (`rclone copy --ignore-existing`): every capture you
  ever made is still in the bucket under its own date-stamped name, so old versions
  of a re-published council PDF are recoverable too — just copy the older-named
  object back.
- `data/gold/` (beyond the committed runtime slice) and intermediate silver are
  **not** in R2 by design — they're cheap to rebuild from bronze via the pipeline.
  If you want a no-rebuild restore, add `gold` to the `foreach` trees in
  [../tools/backup_to_r2.ps1](../tools/backup_to_r2.ps1).
- Verified working: a live restore drill on 2026-06-13 pulled a file from R2 and its
  SHA-256 matched the committed manifest exactly.
- **2026-08-01 audit:** an inventory of every file that is neither git-tracked nor
  under an rclone-mirrored tree found **2.58 GB across 4,064 files** living on exactly
  one disk — dominated by `planning/` (~1.2 GB, deliberately out of the public repo, so
  git can never be its backup). [tools/backup_restic_to_r2.ps1](../tools/backup_restic_to_r2.ps1)
  now covers that set. It is a **separate lane** from `backup_to_r2.ps1`, not a
  replacement: bronze/silver/raw_bq still go up as a plain rclone mirror.
- **Two repositories, two buckets, two passwords — deliberate.** `planning/` is
  commercial IP; the sandbox trees are not. Separate R2 buckets with separately-scoped
  API tokens mean a leaked token for the public-data backup grants no access to the
  private tree, and separate repository passwords give the same isolation at the
  encryption layer.
- **The repository passwords are now a single point of failure.** Unlike the plain
  rclone mirror (where a lost credential just means minting a new R2 token), losing a
  restic password makes that repository permanently unreadable — the data is encrypted
  at rest and there is no recovery path. Both passwords must live in your password
  manager. `C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt` is the copy the
  unattended script reads; it is ACL-restricted to your user, and it is **not** a backup
  of the passwords — it dies with the laptop.
