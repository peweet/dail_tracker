"""tools/publish_data.py — commit + push ONLY the app-read data artifacts.

This is the automated version of the manual "git add the gold parquet, commit,
push, let Streamlit Cloud redeploy" you already do by hand. It is built for an
unattended pipeline run (a Path 2 cron) but behaves identically run locally.

Why it can NEVER push code
--------------------------
* It only ever stages/commits the explicit ``PUBLISH_PATHS`` allow-list below.
  It never runs ``git add .`` / ``git add -A``.
* The commit is pathspec-limited (``git commit -- <paths>``), so even if you
  happen to have unrelated changes staged, they are NOT included in the
  publish commit — only the allow-listed data paths are.
* Change detection uses ``git status`` (read-only); the index is not touched
  until the real commit, and ``--dry-run`` touches nothing at all.
* It aborts if any file to be published is 0 bytes (a failed/partial pipeline
  write) — better to ship nothing than ship a broken table to the live app.

Scope (PUBLISH_PATHS): the committed gold parquet + the freshness badge file.
Tracked silver CSVs are deliberately NOT published by default — add
``"data/silver"`` to the list if the app's silver inputs need to ship too.

Usage:
    python tools/publish_data.py --dry-run     # preview only, change nothing
    python tools/publish_data.py               # stage + commit, then push
    python tools/publish_data.py --no-push     # commit locally, do not push
    python tools/publish_data.py -m "..."      # override the commit message
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path

# The ONLY paths this script will ever stage, commit, or push. Directories are
# matched recursively by git. Anything not under one of these — every .py, every
# config, every doc — is impossible for this script to commit.
PUBLISH_PATHS: list[str] = [
    "data/gold/parquet",  # the gold parquet the app reads
    "data/_meta/freshness.json",  # data-age badge file (data, not code)
]


def _git(args: list[str], root: Path, *, check: bool = True, capture: bool = True):
    """Run a git command in ``root``. Raises SystemExit on failure when check."""
    r = subprocess.run(["git", *args], cwd=root, text=True, capture_output=capture)
    if check and r.returncode != 0:
        if r.stderr:
            sys.stderr.write(r.stderr)
        raise SystemExit(f"git {' '.join(args)} failed (exit {r.returncode})")
    return r


def _repo_root() -> Path:
    here = Path(__file__).resolve().parent
    r = subprocess.run(["git", "rev-parse", "--show-toplevel"], cwd=here, text=True, capture_output=True)
    if r.returncode != 0:
        raise SystemExit("not inside a git repository")
    return Path(r.stdout.strip())


def _changed_paths(root: Path) -> list[str]:
    """Repo-relative paths under PUBLISH_PATHS with pending changes (read-only).

    Uses ``git status --porcelain`` so new (untracked) parquet AND modified
    tracked files are both detected, without staging anything.
    """
    out = _git(["status", "--porcelain", "--", *PUBLISH_PATHS], root).stdout
    files: list[str] = []
    for line in out.splitlines():
        if not line.strip():
            continue
        path = line[3:]  # porcelain format: "XY <path>"
        if " -> " in path:  # rename: "old -> new"
            path = path.split(" -> ", 1)[1]
        files.append(path.strip().strip('"'))
    return files


def _build_message(root: Path) -> str:
    """A dated commit message, enriched with latest dates from freshness.json."""
    stamp = datetime.now(UTC).strftime("%Y-%m-%d")
    fp = root / "data" / "_meta" / "freshness.json"
    detail = ""
    if fp.exists():
        try:
            datasets = json.loads(fp.read_text(encoding="utf-8")).get("datasets", {})
            bits = []
            for key in ("votes", "questions", "iris", "statutory_instruments"):
                entry = datasets.get(key, {})
                val = entry.get("latest_record_date") or entry.get("latest_period_end_date")
                if val:
                    bits.append(f"{key}->{val}")
            if bits:
                detail = " — " + ", ".join(bits)
        except Exception:  # noqa: BLE001 — message enrichment must never block a publish
            pass
    return f"data refresh {stamp}{detail} [auto]"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="preview only; change nothing")
    parser.add_argument("--no-push", action="store_true", help="commit locally; do not push")
    parser.add_argument("-m", "--message", help="override the commit message")
    args = parser.parse_args(argv)

    root = _repo_root()
    changed = _changed_paths(root)

    if not changed:
        print("publish: no data changes under the publish scope — nothing to do.")
        return 0

    # Empty-file guard: a 0-byte output means a failed/partial write upstream.
    for rel in changed:
        p = root / rel
        if p.exists() and p.stat().st_size == 0:
            raise SystemExit(f"publish: ABORT — {rel} is 0 bytes (failed write?). Nothing committed.")

    print(f"publish: {len(changed)} data file(s) changed:")
    for rel in changed:
        print(f"  {rel}")

    if args.dry_run:
        print("publish: --dry-run — index untouched, nothing committed or pushed.")
        return 0

    message = args.message or _build_message(root)
    _git(["add", "--", *PUBLISH_PATHS], root)
    # Pathspec-limited commit: ONLY these paths, regardless of what else is staged.
    _git(["commit", "-m", message, "--", *PUBLISH_PATHS], root)
    print(f"publish: committed — {message}")

    if args.no_push:
        print("publish: --no-push — commit kept local.")
        return 0

    _git(["push"], root, capture=False)
    print("publish: pushed — Streamlit Cloud will redeploy.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
