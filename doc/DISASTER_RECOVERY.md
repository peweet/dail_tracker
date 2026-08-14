---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-08-14
supersedes: []
read_when: the dev laptop is lost or destroyed and you need to restore a working machine, Hetzner access, or data from GitHub + R2
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
| `data/_meta/backup_manifest.tsv` (restore-verification baseline) | GitHub when committed and R2 `manifests/latest.tsv` | pull the R2 copy before verification |
| `data/bronze/` (~7.4 GB raw captures) | R2 bucket `dail-tracker-backup/bronze/` | `rclone copy` (below) |
| `data/silver/` (~1.7 GB derived) | R2 bucket `dail-tracker-backup/silver/` | `rclone copy` (below) |
| `data/raw_bq/` (raw bulk-query captures) | R2 bucket `dail-tracker-backup/raw_bq/` | `rclone copy` (below) |
| `data/gold/` (beyond the git slice), rest of `data/silver` | **not backed up** — regenerable | rebuild via the pipeline from bronze |
| `dailtracker.ie` custom domain (Cloudflare Worker + DNS) | GitHub (`deploy/cloudflare/`) + your Cloudflare account | re-run the one-time setup in [CUSTOM_DOMAIN_CLOUDFLARE.md](CUSTOM_DOMAIN_CLOUDFLARE.md) — DNS/Worker config isn't itself a file to restore, just re-created from that doc |
| `planning/` **+ `.git-siting/`** (~4.1 GB private IP: the working tree *and* the separate git history for `peweet/pre-siting-private`) | restic repo `restic_private` → R2 bucket **`dail-siting`** | `restic restore` (below) |
| **The whole repo otherwise** (~3.8 GB: `.git`, the non-mirrored parts of `data/`, `pipeline_sandbox`, `doc`, `ida`, `out`, `logs`, `audit_screenshots`, `.claude`, tests, tools) | restic repo `restic_sandbox` → R2 bucket `dail-tracker-backup` | `restic restore` (below) |

The sandbox job is defined as **the repo minus a denylist**, not as a list of directories —
so a new top-level directory is protected the day it appears. That is deliberate: the
2026-08-01 audit found 2.58 GB stranded precisely because every backup enumerated an
allowlist that reality had moved past. The denylist and the reason for each entry are in
[tools/backup_restic_to_r2.ps1](../tools/backup_restic_to_r2.ps1).

⚠ **`.git` is backed up, `.git-siting` is backed up to the *other* bucket.** Both carry
commits that may not be pushed (2 unpushed in `.git-siting` as of 2026-08-02), so they are
the only copy of that work. Routing `.git-siting` to the private bucket is what stops a
whole-repo sweep from copying 3.1 GB of private siting history into the public-data bucket
— a leak a `-DryRun` caught on 2026-08-02 and the reason to always run one after editing
the excludes.

R2 account ID: `dda75db5c9db02954a7b45e69052c742`
S3 endpoint: `https://dda75db5c9db02954a7b45e69052c742.r2.cloudflarestorage.com`

## What you need before starting

- Your **GitHub** login (to clone).
- Your **Cloudflare** login. The R2 **Access Key ID + Secret** are ideal to have
  saved (password manager), but if you lost them: log into Cloudflare → R2 →
  **{ } API → Manage API tokens → Create API token** (Account token, *Object Read &
  Write*, scoped to `dail-tracker-backup`) to mint fresh ones. The bucket and its
  contents are untouched by the laptop loss.

## Restore Hetzner SSH administration

Hetzner administration has three independent recovery controls:

- a passphrase-protected break-glass SSH private key stored in encrypted
  off-laptop storage;
- that key's public half in the live server's `deploy` account; and
- a recoverable Hetzner account with 2FA and its recovery key stored offline.

The copy of an SSH key or `hcloud` token on the laptop is convenient, but it is
not a backup because it is lost with the laptop. Do not keep a long-lived
read-write Hetzner API token merely as a disaster-recovery mechanism.

The normal administrative account is `deploy`; direct SSH as `root` is disabled.
On 2026-08-14, a live drill proved that the break-glass key could authenticate as
`deploy` and that `sudo -n` succeeded. Keep this least-privilege route: do not make
direct root SSH the recovery mechanism.

The expected recovery-key fingerprint from that drill is:

```text
SHA256:sqhruQ5/xEXDRgPjSSksSQPD+E98lpoZ8Y9M/ZoiN1M
```

### If the off-laptop recovery key is available

1. Restore the encrypted private key to the new machine's `.ssh` directory and
   restrict it to the current Windows user.
2. Discover the current server address from Hetzner Console. Create a new
   least-privilege `hcloud` context from the recovered Hetzner account if CLI
   access is useful.
3. Verify the recovered public key before connecting:

   ```powershell
   ssh-keygen -lf "$env:USERPROFILE\.ssh\id_ed25519_hetzner_recovery_20260814.pub"
   ```

4. Connect and prove both the account and elevation path:

   ```powershell
   ssh -o IdentitiesOnly=yes `
     -i "$env:USERPROFILE\.ssh\id_ed25519_hetzner_recovery_20260814" `
     deploy@<server-ip> "id -un; hostname; sudo -n true && echo SUDO_OK"
   ```

Do not call the recovery complete unless the result names `deploy`, identifies the
expected host, and prints `SUDO_OK`.

### If every usable client private key is lost

Adding an SSH public key in Hetzner Console does not inject it into an existing
server. Use the server's Hetzner Console instead:

1. Generate a new passphrase-protected Ed25519 key on the replacement machine and
   put its private key in encrypted off-laptop storage before installing it.
2. In Hetzner Console, reset the root password and open the VNC console. Keep the
   generated password private. If the running system cannot be reached this way,
   use Hetzner Rescue; Rescue requires a power cycle and therefore causes downtime.
3. Become root in the console and append only the new public key to
   `/home/deploy/.ssh/authorized_keys`. Preserve every existing line and set:

   ```bash
   install -d -m 700 -o deploy -g deploy /home/deploy/.ssh
   chown deploy:deploy /home/deploy/.ssh/authorized_keys
   chmod 600 /home/deploy/.ssh/authorized_keys
   ssh-keygen -lf /home/deploy/.ssh/authorized_keys
   ```

4. Run the independent `deploy`/hostname/`SUDO_OK` test above from the replacement
   machine.
5. After that test succeeds, lock the temporary root password. Hetzner Console can
   reset it again during a future emergency:

   ```bash
   sudo passwd -l root
   sudo passwd -S root
   ```

6. Treat a lost laptop as potentially compromised: remove its old public key from
   `deploy`'s `authorized_keys`, and rotate any other credentials that were present
   on it.

Register the recovery **public** key in Hetzner Console as well so it can be selected
for a future Rescue environment or new server. That registry entry supplements the
live server's `authorized_keys`; it does not replace it. Never paste a private key or
passphrase into chat, a shell command argument, Git, or Hetzner's public-key registry.

Drill this route at least every six months from a second machine. A fingerprint in a
file or a successful Hetzner login is not sufficient evidence; the drill must prove
SSH authentication and the `sudo` path without changing production state.

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
rclone copy r2:dail-tracker-backup/raw_bq data\raw_bq
rclone copyto r2:dail-tracker-backup/manifests/latest.tsv data\_meta\backup_manifest.tsv

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

# Option A - point restic straight at R2 (no download step; this is how the 2026-08-02
# verification ran). AWS_* are the R2 token for that bucket; the r2private token covers
# dail-siting, the original r2 token covers dail-tracker-backup.
$env:AWS_ACCESS_KEY_ID     = '<access key id>'
$env:AWS_SECRET_ACCESS_KEY = '<secret access key>'
$env:RESTIC_PASSWORD       = '<repository password from your password manager>'
restic -r s3:https://dda75db5c9db02954a7b45e69052c742.r2.cloudflarestorage.com/dail-siting/restic_private snapshots

# Option B - pull the repository down first (it is just a directory of files)
rclone copy r2private:dail-siting/restic_private   C:\restore\restic_private
rclone copy r2:dail-tracker-backup/restic_sandbox  C:\restore\restic_sandbox

# Restore. --target is a PREFIX: restic recreates the original absolute path beneath it,
# e.g. C:\restore\out\c\Users\...\dail_extractor\planning - move it into place afterwards.
$env:RESTIC_PASSWORD = '<private password from your password manager>'
restic -r C:\restore\restic_private restore latest --target C:\restore\out

restic -r C:\restore\restic_private snapshots     # list what is available
restic -r C:\restore\restic_private check         # verify repo integrity
```

Restore a single file instead of everything with
`restic -r <repo> restore latest --target <dir> --include "*/planning/product/<file>"`.

### Three Windows quirks that look like failures but are not (all hit in the 2026-08-02 drill)

1. **`restic restore` exits 1 on a cosmetic error.** It reports
   `failed to restore timestamp of "...\C\Users": Access is denied` → `Fatal: There were 1
   errors`, *after* successfully restoring everything (`Restored 1192 / 1193 files/dirs`).
   The failure is a timestamp write on the synthetic drive-letter directory, not on your
   data. **Read the Summary line, not the exit code** — during a real disaster this reads
   as a failed restore when it isn't.
2. **The restored tree can refuse to delete.** That same `C\Users` directory inherits
   restrictive permissions, so `Remove-Item -Recurse -Force` fails with *Access is denied /
   directory not empty*. Fix:
   `takeown /F <dir> /R /D O; icacls <dir> /grant "$env:USERNAME:(OI)(CI)F" /T /Q`, then delete.
3. **RESTORE GIT REPOS TO A SHORT PATH.** `--target` recreates the *full absolute source
   path* underneath it, so a nested target produces very deep paths. Restoring `.git-siting`
   to a scratch dir gave a 201-character git-dir; git then appends `objects/xx/<40 hex>` and
   breaches Windows' 260-char MAX_PATH. The symptom is alarming and looks exactly like data
   loss — `fatal: cannot read commit object`, `fsck: invalid sha1 pointer` — while the packs
   are provably byte-identical to the source. Two fixes, either works:
   restore to a short target such as `C:\restore`, or pass `git -c core.longpaths=true`.
   **Do not conclude the backup is corrupt from these errors** — verify by hashing a pack
   against the source first (2026-08-02: all 21 pack files identical, 22,275 loose objects
   on both sides, yet git failed until longpaths was enabled).

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

Once restored and verified, all scheduled tasks this project depends on need
re-registering — not just the backup one:

```powershell
tools\register_backup_task.ps1                # DailTracker-BackupR2, weekly Sun 02:00
tools\register_restic_task.ps1                # DailTracker-BackupRestic, weekly Sun 03:00
tools\register_legal_diary_task.ps1            # DailTracker-LegalDiary
```

Recreate
`C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt` from your password manager
first (format: a line starting `restic_private`, then the password on the next line;
same for `restic_sandbox`) or the script exits 3.

Check `Get-ScheduledTask | Where-Object {$_.TaskName -like "DailTracker-*"}` afterward
— both should show `Ready`.

## Notes

- The data backup is **version-preserving**: its normal R2 paths hold the latest
  bytes and any displaced object is moved to `versions/<UTC run id>/` first. Restore
  an older source file from that version archive if needed.
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
- **Verified live 2026-08-02, three ways.** (a) Both repositories checked *from R2* with
  `restic check --read-data`, which re-reads and re-hashes every pack in the bucket rather
  than trusting rclone's transfer report. After the scope expansion to the whole repo:
  `dail-siting/restic_private` **248/248 packs, 6 snapshots**;
  `dail-tracker-backup/restic_sandbox` **204/204 packs, 7 snapshots**; no errors, both exit 0.
  (c) **`.git-siting` functional restore** — 3.003 GiB pulled from R2 and proved to be a
  *working* git repository, not merely matching bytes: `git log` returns the same three
  commits as live, `fsck --connectivity-only` is clean, and **both unpushed commits survived**
  (`f1c2cc5`, `1e4c1e0`). This needed `core.longpaths=true` — see quirk 3 below, and read it
  before concluding anything is corrupt.
  (b) **Full restore drill** — `restic restore latest` pulled `planning/` straight out of
  R2 into a scratch dir and every restored file was SHA-256 compared against the live tree:
  **1,052 identical, 0 corrupt**. The only divergences were 23 modified + 23 new files
  written by an active session in the ~2 h since the snapshot, and 342 `__pycache__` files
  the backup excludes on purpose. This is the restic-lane equivalent of the 2026-06-13
  rclone-lane drill.
- **Freshness is the real limit, not integrity.** That drill quantified it: an actively-
  developed tree drifted by ~46 files in two hours. `DailTracker-BackupRestic` runs weekly
  (Sun 03:00), so worst-case loss on `planning/` is a week of work, not a week of data
  corruption. Run `tools/backup_restic_to_r2.ps1` by hand after any significant session if
  that is too coarse.
- **The repository passwords are now a single point of failure.** Unlike the plain
  rclone mirror (where a lost credential just means minting a new R2 token), losing a
  restic password makes that repository permanently unreadable — the data is encrypted
  at rest and there is no recovery path. Both passwords must live in your password
  manager. `C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt` is the copy the
  unattended script reads; it is ACL-restricted to your user, and it is **not** a backup
  of the passwords — it dies with the laptop.
