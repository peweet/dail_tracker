---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-11-07
supersedes: []
read_when: setting up or auditing the off-box copy of the credentials a restore needs, or checking whether the escrow is still complete
key: PLAN|LIVE|infra
---

# Credential escrow — what must exist somewhere other than the laptop

The backups are verified restorable (see [DISASTER_RECOVERY.md](DISASTER_RECOVERY.md)).
This document covers the part backups cannot solve: **the credentials without which those
backups are unreadable.**

Both restic repositories are AES-256 encrypted. Hetzner Object Storage holds the private
`restic_private` repository in `specplan-ie-private-restic-nbg1`; R2 retains the separate
public-data `restic_sandbox` repository in `dail-tracker-backup`. Lose a repository password and
that data is **permanently unrecoverable** —
there is no reset, no support ticket, no brute force. This is the one failure mode where a
dead laptop becomes permanent loss of the product rather than an afternoon of restoring.

**Laptop copies (since 2026-11-16).** No repository password or storage key sits in a
plain-text file on the laptop any more. `restic_passwords.txt`, the `[r2]` keys in
`rclone.conf` and the two Hetzner profiles in `~/.aws/credentials` moved into the DPAPI store
`%LOCALAPPDATA%\dail-secrets\` (`tools/secret_store.ps1`). The backup scripts read the
passwords from it. rclone and the AWS CLI get the storage keys through `credential_process` in
`~/.aws/config`, and `rclone.conf` holds no keys. The store stops file grabbers and secret
scanners. It does not stop code that already runs as the owner. It **dies with the Windows
profile**, so the password-manager copy is still the real one. The pre-migration files are
kept, encrypted, as `orig_*` entries for rollback. `~/.aws/credentials` deliberately holds
only a decoy `[default]` key; never call AWS with it.

To re-store a secret on a rebuilt laptop, pipe it in from the password manager, for example
`Get-Clipboard | powershell -File tools\secret_store.ps1 -Name restic_private -Set`.
The storage-key entries (`s3_r2`, `s3_hetzner_backup`, `s3_hetzner_siting`) take the JSON
`{"AccessKeyId":"...","SecretAccessKey":"..."}`.

## What to store — Bitwarden, two secure notes

Bitwarden is installed at `%LOCALAPPDATA%\Programs\Bitwarden` and syncs off-box, so it
survives the machine. Create **two** items so a leak of one does not expose the other —
the same isolation reason the two repositories exist at all.

### Item 1 — `restic_private` (COMMERCIAL IP)

| Field | Value |
|---|---|
| Password | DPAPI store entry `restic_private` (44 chars; was line 7 of `restic_passwords.txt`) |
| Repository | `rclone:hetznerbackup:specplan-ie-private-restic-nbg1/restic_private` |
| Local repo | `C:\Users\pglyn\dail_tracker_backup\restic_private` |
| Covers | `planning/`, `apps/`, `.git-siting` — the siting engine and its git history |

### Item 2 — `restic_sandbox`

| Field | Value |
|---|---|
| Password | DPAPI store entry `restic_sandbox` (44 chars; was line 10 of `restic_passwords.txt`) |
| Repository | `rclone:r2:dail-tracker-backup/restic_sandbox` |
| Local repo | `C:\Users\pglyn\dail_tracker_backup\restic_sandbox` |
| Covers | the whole repo minus bronze/silver/raw_bq, minus the private trees above |

⚠ **The four secrets on lines 1-4 of the old file (now only in the encrypted
`orig_restic_passwords_txt` store entry) are NOT restic passwords, and are NOT the
R2 tokens either.** Their lengths are 50/49/67/74 chars; the R2 credentials are 32-char
`access_key_id` + 64-char `secret_access_key` [Verified — redacted field-length dump of
`rclone.conf`, 2026-08-21]. So lines 1-4 are some other unlabelled credential. Identify
them and store each with a note saying what it opens — an unlabelled secret in a file that
dies with the laptop is a gap whether or not anyone remembers what it was for.

### Item 3 — object-storage credentials (REQUIRED; escrowed in Bitwarden 2026-11-07)

Status, 7 November 2026: the owner saved both credentials below to Bitwarden as one secure note
[Reported — owner]. The note was not opened from a second device in the session that recorded this.

On the laptop both keys live in the DPAPI store (`s3_r2`, `s3_hetzner_backup`; before
2026-11-16 they were plain text in `rclone.conf` and `~/.aws/credentials`). The non-secret
`[r2]` and `[hetznerbackup]` rclone remotes select the `r2` and `hetzner-backup` AWS profiles,
whose `credential_process` fetches the key. Neither credential is in the Restic password
file or Git. Without the matching storage credential, a correctly escrowed repository password
still restores **nothing**.

Store both credentials as one secure note:

- `[r2]` for bucket `dail-tracker-backup`: `access_key_id`, `secret_access_key`, Cloudflare
  endpoint, `provider = Cloudflare`, `region = auto`.
- `hetzner-backup` for bucket `specplan-ie-private-restic-nbg1`: `aws_access_key_id`,
  `aws_secret_access_key`, endpoint `https://nbg1.your-objectstorage.com`, region `nbg1`.

The retired `[r2private]` credential is no longer a recovery dependency; the `dail-siting` bucket
was deleted on 2026-08-21 after both encrypted repositories were migrated and fully checked.

## Also needed for a from-nothing restore

A password alone does not get the data back. The escrow is only complete with:

1. **Object-storage credentials** for both providers — the R2 public-data token and the Hetzner
   `hetzner-backup` key described above. Without these, Restic cannot reach the repositories.
2. **GitHub access** — the public repo and both private remotes
   (`peweet/dail-siting-private`, `peweet/public-signal`). Recovers source and history,
   but no data and no generated artefacts.
3. **The bucket names**, which are easy to misremember: the private bucket is
   **`specplan-ie-private-restic-nbg1`**; the public-data bucket is `dail-tracker-backup`.

## Verifying the escrow still works

Test it the way you would in a real recovery — from the stored copy, not from the laptop
file. Read the password out of Bitwarden by hand, then:

```powershell
$env:RESTIC_PASSWORD   = '<paste from Bitwarden>'
$env:RESTIC_REPOSITORY = 'rclone:hetznerbackup:specplan-ie-private-restic-nbg1/restic_private'
restic snapshots --latest 1        # lists = the escrowed password is correct
```

`rclone` must be on PATH or restic fails with `exec: "rclone": executable file not found`,
which looks like a bad repository and is not.

Re-test whenever the password file changes, and after any password rotation. An escrow
copy that was never read back is a guess, not a backup.

## What this does not protect against

Losing the Bitwarden master password itself. If that matters to you — and for a commercial
product it reasonably might — the only route that survives it is a physical copy: print the
two secrets, date the page, store it somewhere sensible. Paper has no sync, no battery and
no expiry.

Related: [DISASTER_RECOVERY.md](DISASTER_RECOVERY.md) (the restore procedures these
credentials unlock), [DATA_BACKUP.md](DATA_BACKUP.md) (the rclone lane and its R2 remotes).
