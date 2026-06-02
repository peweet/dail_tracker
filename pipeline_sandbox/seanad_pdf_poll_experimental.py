"""Seanad payments/attendance PDF poller — EXPERIMENTAL PROTOTYPE.

# DELETE AFTER PROMOTION: add two PollSource entries to oireachtas_pdf_poller.SOURCES

Discovers and downloads the Senator equivalents of the TD payment/attendance
PDFs, which the production poller does not yet fetch (its SOURCES carry only the
`…to-deputies` / `deputies-verification…` filename hints).

Confirmed on the live index 2026-06-01:
  - parliamentary-allowances topic also lists
      parliamentary-standard-allowance-payments-to-senators-for-<month>-<year>
  - record-of-attendance topic also lists
      senators-verification-of-attendance-for-the-payment-of-taa-<range>

This prototype reuses oireachtas_pdf_poller's fetch/parse/filter/download
verbatim — only the PollSource config differs — and writes into SANDBOX-ONLY
sibling dirs so the deputies-format ETL globs never see Senator PDFs:
  data/bronze/pdfs/payments_seanad_experimental/
  data/bronze/pdfs/attendance_seanad_experimental/

Promotion = move these two PollSource dicts into oireachtas_pdf_poller.SOURCES
with their real target_dir (a house-aware dir the promoted parser reads).

Usage:
  python pipeline_sandbox/seanad_pdf_poll_experimental.py            # sample (2 each)
  python pipeline_sandbox/seanad_pdf_poll_experimental.py --limit 0  # all available
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

import requests  # noqa: E402

from oireachtas_pdf_poller import (  # noqa: E402 — reuse production fetch/parse/download
    USER_AGENT,
    PollSource,
    download,
    fetch_index_html,
    filter_new,
    parse_index,
)

_ROOT = Path(__file__).resolve().parents[1]
_BRONZE = _ROOT / "data" / "bronze" / "pdfs"

SEANAD_SOURCES = {
    "payments_seanad": PollSource(
        name="payments_seanad",
        topic_slug="parliamentary-allowances",
        target_dir=_BRONZE / "payments_seanad_experimental",
        filename_hint="parliamentary-standard-allowance-payments-to-senators",
    ),
    "attendance_seanad": PollSource(
        name="attendance_seanad",
        topic_slug="record-of-attendance",
        target_dir=_BRONZE / "attendance_seanad_experimental",
        filename_hint="senators-verification-of-attendance",
    ),
}


def poll_sample(source: PollSource, limit: int) -> list[Path]:
    """Fetch index → parse with the Senator hint → download up to `limit` new PDFs.

    limit=0 means download everything new. Mirrors run_one but caps the
    download set so exploration doesn't pull every monthly PDF.
    """
    print(f"\n=== {source.name} — index {source.index_url} ===")
    entries = parse_index(source, fetch_index_html(source))
    print(f"matched {len(entries)} Senator entries by hint {source.filename_hint!r}")
    for e in entries[:5]:
        print("   ", e.pub_date_raw, "|", e.filename[:80])

    new = filter_new(source, entries)
    if limit:
        new = new[:limit]
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    saved: list[Path] = []
    for e in new:
        try:
            saved.append(download(source, e, session))
            print(f"   downloaded -> {saved[-1].name}")
        except Exception as exc:  # keep going; report at end
            print(f"   FAILED {e.filename}: {exc}")
    return saved


def main() -> int:
    ap = argparse.ArgumentParser(description="Seanad PDF poll (experimental)")
    ap.add_argument("--limit", type=int, default=2, help="max downloads per source (0 = all)")
    args = ap.parse_args()

    total = 0
    for src in SEANAD_SOURCES.values():
        total += len(poll_sample(src, args.limit))
    print(f"\nDownloaded {total} Senator PDF(s) into sandbox bronze dirs.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
