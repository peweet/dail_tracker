"""Check that a restic password opens its repository, WITHOUT reading the laptop's copy.

WHY THIS EXISTS (2026-08-21): both restic repositories are AES-256 encrypted and their
only password copy lived in one ACL'd file on the laptop being backed up. Lose it and
the commercial IP in object storage becomes permanently unreadable. The fix is an escrow copy
in a password manager -- but an escrow nobody has ever read back is a guess, not a backup.

So this tool deliberately does NOT read restic_passwords.txt. You paste the secret from
the password manager; it proves THAT value opens the repository. Testing the laptop file
against the laptop repo proves nothing about what you would actually have after the
laptop is gone.

The secret is read from a prompt (never echoed, never logged, never taken as an argv
value, since argv lands in shell history and process listings).

Usage:
    python tools/check_credential_escrow.py private
    python tools/check_credential_escrow.py sandbox --local
"""

from __future__ import annotations

import argparse
import getpass
import os
import shutil
import subprocess
import sys
from pathlib import Path

BACKUP_ROOT = Path(r"C:\Users\pglyn\dail_tracker_backup")

#: repo name -> (off-site repository URL, local repository path)
REPOS = {
    "private": (
        "rclone:hetznerbackup:specplan-ie-private-restic-nbg1/restic_private",
        BACKUP_ROOT / "restic_private",
    ),
    "sandbox": ("rclone:r2:dail-tracker-backup/restic_sandbox", BACKUP_ROOT / "restic_sandbox"),
}


def _find(name: str, pattern: str) -> str | None:
    found = shutil.which(name)
    if found:
        return found
    packages = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet" / "Packages"
    if not packages.is_dir():
        return None
    return next((str(p) for p in packages.rglob(pattern)), None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", choices=sorted(REPOS))
    parser.add_argument(
        "--local",
        action="store_true",
        help="test the local repository instead of the R2 copy (faster, but proves less)",
    )
    args = parser.parse_args()

    restic = _find("restic", "restic*.exe")
    if restic is None:
        print("restic not found - winget install --id restic.restic", file=sys.stderr)
        return 2

    remote_url, local_path = REPOS[args.repo]
    repository = str(local_path) if args.local else remote_url

    # restic shells out to rclone for an `rclone:` backend; without it on PATH the failure
    # reads as a broken repository rather than a missing binary.
    environment = dict(os.environ)
    if not args.local:
        rclone = _find("rclone", "rclone.exe")
        if rclone is None:
            print("rclone not found, and the off-site check needs it - pass --local or install it", file=sys.stderr)
            return 2
        environment["PATH"] = f"{Path(rclone).parent}{os.pathsep}{environment.get('PATH', '')}"

    print(f"Repository: {repository}")
    print("Paste the password from your PASSWORD MANAGER (not the laptop file).")
    # getpass wants a console: piped or redirected stdin makes it block forever rather
    # than fail, so read the pipe directly when there is no tty (CI, a test harness).
    if sys.stdin is not None and not sys.stdin.isatty():
        # PowerShell prepends a UTF-8 BOM when it pipes a string into a native command,
        # which silently makes a correct password one character too long and reports as
        # "wrong password or no key found".
        secret = sys.stdin.readline().rstrip("\r\n").lstrip("﻿")
    else:
        secret = getpass.getpass("Password: ")
    if not secret.strip():
        print("No password entered.", file=sys.stderr)
        return 2

    environment["RESTIC_REPOSITORY"] = repository
    environment["RESTIC_PASSWORD"] = secret
    try:
        result = subprocess.run(
            [restic, "snapshots", "--latest", "1", "--compact"],
            capture_output=True,
            text=True,
            timeout=600,
            env=environment,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        print(f"Could not run restic: {exc}", file=sys.stderr)
        return 2
    finally:
        environment["RESTIC_PASSWORD"] = ""

    if result.returncode != 0:
        # "wrong password" and "cannot reach the bucket" are different failures with
        # different fixes, so show what restic actually said rather than summarising it.
        print("\nESCROW CHECK FAILED - this password did not open the repository.\n", file=sys.stderr)
        print((result.stderr or result.stdout).strip()[:1000], file=sys.stderr)
        return 1

    print("\nESCROW OK - the password you pasted opens the repository.")
    print(result.stdout.strip()[-600:])
    return 0


if __name__ == "__main__":
    sys.exit(main())
