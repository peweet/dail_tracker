---
tier: PLAN
status: LIVE
domain: infra
updated: 2026-08-21
supersedes: []
read_when: setting up or auditing the off-box copy of the credentials a restore needs, or checking whether the escrow is still complete
key: PLAN|LIVE|infra
---

# Credential escrow — what must exist somewhere other than the laptop

The backups are verified restorable (see [DISASTER_RECOVERY.md](DISASTER_RECOVERY.md)).
This document covers the part backups cannot solve: **the credentials without which those
backups are unreadable.**

Both restic repositories are AES-256 encrypted. R2 holds 15.2 GB of commercial IP in
`dail-siting` and 45.6 GB in `dail-tracker-backup` [Verified — `rclone size --json`,
2026-08-21]. Lose the repository password and that data is **permanently unrecoverable** —
there is no reset, no support ticket, no brute force. This is the one failure mode where a
dead laptop becomes permanent loss of the product rather than an afternoon of restoring.

`C:\Users\pglyn\dail_tracker_backup\restic_passwords.txt` is ACL-restricted to the owner
and **dies with the laptop**. The password-manager copy is the real one.

## What to store — Bitwarden, two secure notes

Bitwarden is installed at `%LOCALAPPDATA%\Programs\Bitwarden` and syncs off-box, so it
survives the machine. Create **two** items so a leak of one does not expose the other —
the same isolation reason the two repositories exist at all.

### Item 1 — `restic_private` (COMMERCIAL IP)

| Field | Value |
|---|---|
| Password | line 7 of `restic_passwords.txt` (44 chars) |
| Repository | `rclone:r2private:dail-siting/restic_private` |
| Local repo | `C:\Users\pglyn\dail_tracker_backup\restic_private` |
| Covers | `planning/`, `apps/`, `.git-siting` — the siting engine and its git history |

### Item 2 — `restic_sandbox`

| Field | Value |
|---|---|
| Password | line 10 of `restic_passwords.txt` (44 chars) |
| Repository | `rclone:r2:dail-tracker-backup/restic_sandbox` |
| Local repo | `C:\Users\pglyn\dail_tracker_backup\restic_sandbox` |
| Covers | the whole repo minus bronze/silver/raw_bq, minus the private trees above |

⚠ **The four secrets on lines 1-4 of that file are NOT restic passwords, and are NOT the
R2 tokens either.** Their lengths are 50/49/67/74 chars; the R2 credentials are 32-char
`access_key_id` + 64-char `secret_access_key` [Verified — redacted field-length dump of
`rclone.conf`, 2026-08-21]. So lines 1-4 are some other unlabelled credential. Identify
them and store each with a note saying what it opens — an unlabelled secret in a file that
dies with the laptop is a gap whether or not anyone remembers what it was for.

### Item 3 — the R2 API tokens (REQUIRED, and currently laptop-only)

The tokens live **only** in `C:\Users\pglyn\AppData\Roaming\rclone\rclone.conf`. They are
not in the password file and not in git. Without them restic cannot reach the bucket at
all, so a correctly-escrowed repository password still restores **nothing**.

Store both remotes as one secure note — for each of `[r2]` (bucket
`dail-tracker-backup`) and `[r2private]` (bucket `dail-siting`):
`access_key_id`, `secret_access_key`, `endpoint`, `provider = Cloudflare`, `region = auto`.

Simplest capture: copy `rclone.conf` wholesale into a Bitwarden secure-note attachment. It
is a small text file and holds both remotes complete.

## Also needed for a from-nothing restore

A password alone does not get the data back. The escrow is only complete with:

1. **R2 API tokens** for both buckets — `rclone.conf` holds them
   (`rclone config file` names its path). Without these, restic cannot reach the
   repository the password would decrypt.
2. **GitHub access** — the public repo and both private remotes
   (`peweet/dail-siting-private`, `peweet/public-signal`). Recovers source and history,
   but no data and no generated artefacts.
3. **The bucket names**, which are easy to misremember: the private bucket is
   **`dail-siting`**, *not* `dail-tracker-private`.

## Verifying the escrow still works

Test it the way you would in a real recovery — from the stored copy, not from the laptop
file. Read the password out of Bitwarden by hand, then:

```powershell
$env:RESTIC_PASSWORD   = '<paste from Bitwarden>'
$env:RESTIC_REPOSITORY = 'rclone:r2private:dail-siting/restic_private'
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
