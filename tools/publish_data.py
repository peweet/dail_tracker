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

Pre-publish integrity gate (the cron safety net)
------------------------------------------------
Before it commits ANYTHING, it runs a gate that aborts on corrupt/incomplete
data — so an unattended cron run that parsed without error but produced junk
can never reach the live app:
* every changed parquet must be READABLE and have > 0 rows (the byte guard
  above misses a well-formed-but-empty or row-count-collapsed table);
* the whole gold layer must pass the COMPLETENESS baseline
  (``tools/check_output_regressions.py --strict``): no table MISSING, EMPTIED,
  ROW_DROP (>tolerance), or COL_REMOVED vs the committed baseline.
``--skip-validate`` bypasses the gate (emergencies / a deliberate re-baseline).

Scope (PUBLISH_PATHS): the committed gold parquet + the freshness badge file.
Tracked silver CSVs are deliberately NOT published by default — add
``"data/silver"`` to the list if the app's silver inputs need to ship too.

Usage:
    python tools/publish_data.py --dry-run     # preview + run the gate, change nothing
    python tools/publish_data.py               # validate, then stage + commit + push
    python tools/publish_data.py --no-push     # validate + commit locally, do not push
    python tools/publish_data.py --skip-validate  # DANGER: bypass the integrity gate
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
    # The live national tender snapshot (silver). It is an app-read artifact like the gold
    # parquet — the Procurement page reads it directly — and is refreshed on its own daily
    # cadence (the live-tenders GitHub Action), so it ships through the same gated publish.
    "data/silver/parquet/etenders_live_tenders.parquet",
    # TED (EU-journal) silver the procurement views read directly at runtime: the tender pipeline,
    # the EU-award lens, and the 2016-2023 winner backfill. (buyer_history is a pipeline-only
    # intermediate — no view reads it — so it is NOT shipped.)
    "data/silver/parquet/ted_ie_tenders.parquet",
    "data/silver/parquet/ted_ie_awards.parquet",
    "data/silver/parquet/ted_ie_winner_history.parquet",
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


def _validate(root: Path, changed: list[str], *, tolerance: float) -> None:
    """Pre-publish integrity gate. Raises SystemExit on any failure so a broken
    pipeline run can never commit/push corrupt or incomplete data.

    Two layers: (1) each changed parquet is readable and non-empty; (2) the gold
    layer as a whole passes the committed completeness baseline.
    """
    import pyarrow.parquet as pq  # core runtime dep; lazy so --help stays light

    parquets = [r for r in changed if r.endswith(".parquet")]
    for rel in parquets:
        p = root / rel
        if not p.exists():
            continue
        try:
            n_rows = pq.ParquetFile(p).metadata.num_rows
        except Exception as e:  # noqa: BLE001 — an unreadable output must abort, not crash
            raise SystemExit(f"publish: ABORT — {rel} is not a readable parquet ({type(e).__name__}). Nothing committed.")
        if n_rows == 0:
            raise SystemExit(f"publish: ABORT — {rel} has 0 rows (failed/partial write?). Nothing committed.")
    print(f"publish: gate — {len(parquets)} parquet(s) readable + non-empty.")

    # Whole-gold completeness vs the committed baseline. Run in-venv so polars +
    # config resolve exactly as they do for the rest of the pipeline.
    guard = root / "tools" / "check_output_regressions.py"
    r = subprocess.run(
        [sys.executable, str(guard), "--strict", "--tolerance", str(tolerance)],
        cwd=root,
        text=True,
        capture_output=True,
    )
    if r.stdout:
        sys.stdout.write(r.stdout)
    if r.returncode != 0:
        if r.stderr:
            sys.stderr.write(r.stderr)
        raise SystemExit(
            "publish: ABORT — gold completeness regression vs baseline (see above). Nothing committed. "
            "Re-baseline only if the change is intended: python tools/check_output_regressions.py --update-baseline"
        )
    print("publish: gate — completeness OK.")


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
    parser.add_argument("--dry-run", action="store_true", help="preview + run the gate; change nothing")
    parser.add_argument("--no-push", action="store_true", help="commit locally; do not push")
    parser.add_argument("--skip-validate", action="store_true", help="DANGER: bypass the pre-publish integrity gate")
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.5,
        help="row-drop fraction the completeness gate tolerates (default 0.5)",
    )
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

    # Integrity gate — runs for dry-run AND real publishes, before anything is staged.
    if args.skip_validate:
        print("publish: WARNING — integrity gate SKIPPED (--skip-validate).")
    else:
        _validate(root, changed, tolerance=args.tolerance)

    if args.dry_run:
        print("publish: --dry-run — gate ran, index untouched, nothing committed or pushed.")
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
