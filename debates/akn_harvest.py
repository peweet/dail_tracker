"""
akn_harvest.py

Stage 2a of the debates integration: download the AKN debate transcript XML so
debates/speech_parse.py can extract member contributions from it.

Harvest grain is the **whole sitting day** (`main.xml`), NOT per debate section.
Why: the per-section `dbsect_<n>.xml` key the listings flattener *constructs*
(dbsect_listings_flatten.py:101) frequently does not exist in the Oireachtas S3
bucket — S3 then answers 403 AccessDenied (its response for a missing key), which
looks like a block but is not. The authoritative, always-present object is the
day-level `main.xml`, published in each debateRecord as
`debateRecord.formats.xml.uri`. One file per (chamber, sitting-day) — a few
thousand total — instead of ~32k flaky per-section fetches.

Worklist source: the debates_listings bronze JSON already fetched by
services/oireachtas_api_main.py STEP 5. We read the day-level
`debateRecord.formats.xml.uri` straight from it — no extra /v1/debates calls and
no URL construction. (Refresh that bronze first if you need recent sitting days;
the listings scenario is delta-cheap.)

Reuses services.http_engine.fetch_all_text (the shared retry/session/concurrency
machinery, text-decoding sibling of fetch_all). Already-downloaded days are
skipped so re-runs are incremental.

Input  : data/bronze/debates/listings/debates_listings_results.json
Output : data/bronze/debates/akn/<chamber>_<date>_main.xml  (one per sitting day)

Run standalone:
  python -m debates.akn_harvest --limit 5          # smoke-test a few days
  python -m debates.akn_harvest --since 2025-01-01 # current term onward
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from config import AKN_DIR, API_BASE, DEBATES_LISTINGS_DIR
from services.http_engine import fetch_all_text, fetch_json

logger = logging.getLogger(__name__)

_BRONZE = DEBATES_LISTINGS_DIR / "debates_listings_results.json"


def _chamber_from_uri(xml_uri: str) -> str:
    """Authoritative chamber from the transcript uri path (drops committees)."""
    parts = xml_uri.split("/debateRecord/", 1)
    return parts[1].split("/", 1)[0] if len(parts) == 2 else ""


def _api_day_targets(
    since: str,
    until: str = "2099-01-01",
    chambers: tuple[str, ...] = ("dail", "seanad"),
) -> list[dict]:
    """Enumerate (chamber, date, main.xml uri) from the LIVE /v1/debates API.

    The authoritative, current worklist source — independent of the (stale)
    listings bronze. Paginates a date-range query per chamber and keeps only
    genuine chamber transcripts (committees, whose records share a house code,
    are dropped by reading the chamber from the transcript uri path).
    """
    seen: set[tuple[str, str]] = set()
    targets: list[dict] = []
    for chamber in chambers:
        skip, limit = 0, 500
        while True:
            url = (
                f"{API_BASE}/debates?chamber={chamber}&date_start={since}"
                f"&date_end={until}&limit={limit}&skip={skip}&lang=en"
            )
            payload, _ = fetch_json(url)
            results = payload.get("results") or []
            total = int((payload.get("head") or {}).get("counts", {}).get("resultCount", 0) or 0)
            if not results:
                break
            for r in results:
                rec = r.get("debateRecord") or {}
                date = rec.get("date") or ""
                xml_uri = ((rec.get("formats") or {}).get("xml") or {}).get("uri") or ""
                if not date or not xml_uri:
                    continue
                ch = _chamber_from_uri(xml_uri)
                if ch not in chambers:
                    continue
                key = (ch, date)
                if key in seen:
                    continue
                seen.add(key)
                targets.append({"chamber": ch, "date": date, "xml_uri": xml_uri})
            skip += limit
            if skip >= total:
                break
    logger.info("akn_harvest: API worklist since %s -> %d chamber sitting-days", since, len(targets))
    return targets


def _day_targets() -> list[dict]:
    """Distinct (chamber, date, xml_uri) day-level transcripts from the listings.

    One entry per sitting day per chamber. Skips committee/blank rows (no
    chamber) and any record missing the day-level main.xml uri.
    """
    if not _BRONZE.exists():
        logger.warning("akn_harvest: listings bronze not found at %s — run STEP 5 first", _BRONZE)
        return []

    with open(_BRONZE, encoding="utf-8") as f:
        raw = json.load(f)

    seen: set[tuple[str, str]] = set()
    targets: list[dict] = []
    for page in raw:
        for r in page.get("results") or []:
            rec = r.get("debateRecord")
            if not isinstance(rec, dict):
                continue
            date = rec.get("date") or ""
            xml_uri = ((rec.get("formats") or {}).get("xml") or {}).get("uri") or ""
            if not date or not xml_uri:
                continue
            # The authoritative chamber is the path segment of the transcript
            # uri (/akn/ie/debateRecord/<chamber>/...), NOT house.houseCode:
            # committees belong to a house (houseCode dail/seanad) but publish a
            # committee_* transcript. Keying off the uri keeps ONLY genuine
            # chamber debates and drops every committee record cleanly.
            parts = xml_uri.split("/debateRecord/", 1)
            chamber = parts[1].split("/", 1)[0] if len(parts) == 2 else ""
            if chamber not in ("dail", "seanad"):
                continue
            key = (chamber, date)
            if key in seen:
                continue
            seen.add(key)
            targets.append({"chamber": chamber, "date": date, "xml_uri": xml_uri})
    return targets


def _out_path(chamber: str, date: str) -> Path:
    return AKN_DIR / f"{chamber}_{date}_main.xml"


def run(
    limit: int | None = None,
    since: str | None = None,
    chambers: tuple[str, ...] = ("dail", "seanad"),
    overwrite: bool = False,
    api_since: str | None = None,
    dry_run: bool = False,
) -> int:
    """Download day-level AKN transcripts into AKN_DIR. Returns files written.

    Worklist source: the live /v1/debates API when ``api_since`` is given
    (authoritative + current — the recommended path), else the listings bronze.
    ``since`` keeps days with date >= since (ISO string compares correctly).
    Existing files are skipped unless ``overwrite``. ``limit`` caps NEW downloads
    (after the skip filter). ``dry_run`` enumerates + reports without downloading.
    """
    AKN_DIR.mkdir(parents=True, exist_ok=True)
    if api_since:
        targets = _api_day_targets(api_since, chambers=chambers)
    else:
        targets = [t for t in _day_targets() if t["chamber"] in chambers]
    if since:
        targets = [t for t in targets if t["date"] >= since]
    targets.sort(key=lambda t: (t["date"], t["chamber"]), reverse=True)

    pending = [t for t in targets if overwrite or not _out_path(t["chamber"], t["date"]).exists()]
    skipped = len(targets) - len(pending)
    if limit is not None:
        pending = pending[:limit]

    if dry_run:
        from collections import Counter

        by_ch = Counter(t["chamber"] for t in targets)
        logger.info(
            "akn_harvest DRY-RUN: %d sitting-days total %s | %d already on disk | %d would download",
            len(targets),
            dict(by_ch),
            skipped,
            len(pending),
        )
        return len(pending)

    if not pending:
        logger.info("akn_harvest: nothing to fetch (%d already present)", skipped)
        return 0

    logger.info(
        "akn_harvest: %d day-transcripts to fetch (%d already present, %d total candidates)",
        len(pending),
        skipped,
        len(targets),
    )

    by_url = {t["xml_uri"]: t for t in pending}
    results, total_bytes, failures = fetch_all_text(list(by_url.keys()), max_workers=5)

    written = 0
    for url, text in results:
        t = by_url[url]
        out = _out_path(t["chamber"], t["date"])
        out.write_text(text, encoding="utf-8")
        written += 1

    logger.info(
        "akn_harvest: wrote %d files to %s | failures=%d | bytes=%s",
        written,
        AKN_DIR,
        failures,
        f"{total_bytes:,}",
    )
    return written


def main(argv: list[str] | None = None) -> int:
    from services.logging_setup import setup_logging

    setup_logging()
    p = argparse.ArgumentParser(description="Harvest day-level AKN debate transcripts.")
    p.add_argument("--limit", type=int, default=None, help="Cap NEW downloads (smoke-test).")
    p.add_argument("--since", type=str, default=None, help="Keep days with date >= YYYY-MM-DD.")
    p.add_argument(
        "--api-since",
        type=str,
        default=None,
        help="Source the worklist from the live /v1/debates API since this date (recommended; bronze is stale).",
    )
    p.add_argument("--chamber", choices=("dail", "seanad"), default=None, help="Restrict to one chamber.")
    p.add_argument("--overwrite", action="store_true", help="Re-download days already on disk.")
    p.add_argument("--dry-run", action="store_true", help="Enumerate + report counts without downloading.")
    args = p.parse_args(argv)
    chambers = (args.chamber,) if args.chamber else ("dail", "seanad")
    run(
        limit=args.limit,
        since=args.since,
        chambers=chambers,
        overwrite=args.overwrite,
        api_since=args.api_since,
        dry_run=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
