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
| `planning/product/` (~1.2 GB, kept out of the public repo — see `.gitignore`) | **not backed up as of 2026-08-01** — see note below | none yet |

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
```

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
- **Open gap (2026-08-01): `planning/product/`** (~1.2 GB) is deliberately excluded
  from GitHub (`.gitignore` — kept out of the public repo) but is *not* mirrored to
  R2 either, since `backup_to_r2.ps1` only covers `bronze`/`silver`/`raw_bq`. Right
  now this tree has no off-box copy at all. A local restic smoke test (encrypted,
  local-only, this session) proved restic backs it up and restores it byte-for-byte
  (1,140/1,140 files verified, `restic check` clean) — the natural fix is a second
  restic repository targeting the same private R2 bucket under a distinct prefix,
  which keeps it out of the public GitHub repo while giving it real off-box
  durability. Not yet built: needs its own R2 credential (reuse or a new
  least-privilege token) and a decision on where its repository password is stored
  for unattended weekly runs.
