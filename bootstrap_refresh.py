"""bootstrap_refresh.py — shared inputs every other refresh chain depends on.

Run this first (or trust pipeline.py to). Produces:

    1. oireachtas_pdf_poller        new PDFs across payments / attendance / interests
    2. pdf_downloader               historical URL list catch-up
    3. Members API                  fresh members, votes, debates, questions JSON
                                    (services.oireachtas_api_main, in-process)
    4. flatten_members_json_to_csv  silver/parquet/flattened_members.parquet
    5. dbsect_listings_flatten      silver debate-section listings

Anything below this in pipeline.py assumes flattened_members.parquet is current
— it's the cross-source join key carrier. If you run a downstream chain standalone
without bootstrap, member-attached fields may be stale.

CLI:
    python bootstrap_refresh.py
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_log = logging.getLogger("bootstrap_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_poll_oireachtas() -> bool:
    _hr("[1/5] oireachtas_pdf_poller — payments / attendance / interests PDFs")
    return _subprocess("oireachtas_pdf_poller.py")


def step_pdf_downloader() -> bool:
    _hr("[2/5] pdf_downloader — historical URL list catch-up")
    return _subprocess("pdf_downloader.py")


def step_members_api() -> bool:
    _hr("[3/5] Members API — members / votes / debates / questions JSON")
    t = time.monotonic()
    try:
        from services.oireachtas_api_main import main as run_oireachtas_api
        run_oireachtas_api()
    except Exception as exc:
        _log.exception("Members API failed: %s", exc)
        return False
    print(f"  done in {time.monotonic() - t:.1f}s")
    return True


def step_flatten_members() -> bool:
    _hr("[4/5] flatten_members_json_to_csv — silver flattened members")
    return _subprocess("flatten_members_json_to_csv.py")


def step_flatten_debates() -> bool:
    _hr("[5/5] dbsect_listings_flatten — silver debate-section listings")
    return _subprocess("dbsect_listings_flatten.py")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    started = time.monotonic()
    failures: list[str] = []
    for name, fn in [
        ("poll_oireachtas", step_poll_oireachtas),
        ("pdf_downloader", step_pdf_downloader),
        ("members_api", step_members_api),
        ("flatten_members", step_flatten_members),
        ("flatten_debates", step_flatten_debates),
    ]:
        if not fn():
            failures.append(name)
    _hr(f"[done] bootstrap_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
