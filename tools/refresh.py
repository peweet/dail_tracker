"""tools/refresh.py — one-command data refresh for the deployed Streamlit app.

Chains the two steps you'd otherwise run by hand:
    1. python pipeline.py [--select ...]   # ETL: pull sources + build gold/silver
    2. python tools/publish_data.py        # gated commit + push -> Cloud redeploys

The publish step runs ONLY if the pipeline exits 0, and publish_data itself gates
on the data-integrity checks (readable + non-empty parquet, no completeness
regression vs the committed baseline). So a broken/partial ETL run can never reach
the live app. Built for an unattended cron / Task Scheduler run; behaves
identically by hand.

Usage:
    python tools/refresh.py                       # full pipeline, then publish (commit + push)
    python tools/refresh.py --select iris,votes   # only these chains, then publish
    python tools/refresh.py --exclude lobbying    # everything except a chain, then publish
    python tools/refresh.py --no-push             # refresh + commit locally, do not push
    python tools/refresh.py --dry-run             # refresh, then PREVIEW the publish (no commit)
    python tools/refresh.py --skip-pipeline       # publish only (skip ETL) — e.g. re-publish
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(args: list[str]) -> int:
    """Run a child python step from the repo root, streaming its output live."""
    print(f"\n>>> {sys.executable} {' '.join(args)}\n", flush=True)
    return subprocess.run([sys.executable, *args], cwd=ROOT).returncode


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument("--select", help="comma-separated pipeline chains to run (default: all)")
    ap.add_argument("--exclude", help="comma-separated pipeline chains to skip")
    ap.add_argument("--skip-pipeline", action="store_true", help="skip the ETL; go straight to publish")
    ap.add_argument("--no-push", action="store_true", help="commit locally; do not push")
    ap.add_argument("--dry-run", action="store_true", help="preview the publish; commit nothing")
    ap.add_argument("--skip-validate", action="store_true", help="DANGER: bypass the publish integrity gate")
    args = ap.parse_args(argv)

    # 1. ETL — pull + build. Abort the whole refresh if it fails (never publish junk).
    if not args.skip_pipeline:
        pipe = ["pipeline.py"]
        if args.select:
            pipe += ["--select", args.select]
        if args.exclude:
            pipe += ["--exclude", args.exclude]
        rc = _run(pipe)
        if rc != 0:
            print(f"\nrefresh: ABORT — pipeline exited {rc}; NOT publishing.", file=sys.stderr)
            return rc
    else:
        print("refresh: --skip-pipeline — going straight to publish.")

    # 2. Publish — the gated commit + push. publish_data.py runs the integrity gate.
    pub = ["tools/publish_data.py"]
    if args.no_push:
        pub.append("--no-push")
    if args.dry_run:
        pub.append("--dry-run")
    if args.skip_validate:
        pub.append("--skip-validate")
    rc = _run(pub)
    if rc != 0:
        print(f"\nrefresh: publish step exited {rc}.", file=sys.stderr)
        return rc

    print("\nrefresh: done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
