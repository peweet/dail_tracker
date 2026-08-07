"""Harvest Chief Executive / Monthly Management Reports into a separate page-aware ledger.

This lane never writes ``meetings_clean.jsonl`` or the council-minutes corpus. CE reports are
officer reports, not resolved council minutes, and retain their own document type and provenance.

Examples:
    python ce_report_harvest.py --council Leitrim,Kildare --latest-per-council 2
    python ce_report_harvest.py --all-confirmed --latest-per-council 1
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import unquote, urljoin, urlsplit

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from services import runtime_env as _runtime_env  # noqa: E402,F401,I001

import pymupdf as fitz  # noqa: E402
from bs4 import BeautifulSoup  # noqa: E402

from extractors.council_minutes_contract import classify_document  # noqa: E402
from services.http_engine import fetch_bytes, fetch_text, polite_headers  # noqa: E402

HERE = Path(__file__).resolve().parent
SEEDS = HERE / "ce_report_seeds.csv"
MANIFEST = HERE / "ce_reports_manifest.jsonl"
PAGE_ROOT = HERE / "ce_reports_corpus"
CENSUS = HERE / "ce_report_census.json"

_REPORT_HINT = re.compile(
    r"chief executive|monthly management|management report|\bce\s+(?:monthly\s+)?report\b",
    re.I,
)
_ARCHIVE_HINT = re.compile(r"chief executive|management report|archive|20(?:1\d|2\d)", re.I)
_MONTHS = {
    name.lower(): index
    for index, name in enumerate(
        (
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ),
        start=1,
    )
}


def report_month(value: str) -> tuple[str, str]:
    """Return (YYYY-MM, status) only when the source label states both month and year."""
    raw = unquote(str(value or ""))
    numeric = re.search(r"(?<!\d)(20\d{2}|\d{2})[-_](0[1-9]|1[0-2])(?!\d)", raw)
    if numeric:
        year = numeric.group(1)
        return f"{year if len(year) == 4 else '20' + year}-{numeric.group(2)}", "parsed_month"
    text = re.sub(r"[_%-]+", " ", raw)
    year = re.search(r"\b(20(?:1\d|2\d))\b", text)
    month = next((number for name, number in _MONTHS.items() if re.search(rf"\b{name}\b", text, re.I)), None)
    if year and month:
        return f"{year.group(1)}-{month:02d}", "parsed_month"
    return "", "unresolved"


def report_month_from_source(title: str, source_url: str) -> tuple[str, str]:
    """Prefer the report label and filename; archive-directory dates are crawl metadata."""
    filename = unquote(urlsplit(source_url).path.rsplit("/", 1)[-1])
    for candidate in (title, filename):
        parsed = report_month(candidate)
        if parsed[0]:
            return parsed
    return "", "unresolved"


def discover_report_links(page_url: str, html: str) -> list[tuple[str, str]]:
    """Find source-labelled report downloads; generic PDFs are deliberately ignored."""
    soup = BeautifulSoup(html, "html.parser")
    found: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        href = urljoin(page_url, str(anchor["href"]).strip())
        label = re.sub(r"\s+", " ", anchor.get_text(" ", strip=True)).strip()
        signal = re.sub(r"[^a-z0-9]+", " ", f"{label} {urlsplit(href).path}", flags=re.I)
        looks_download = ".pdf" in href.lower() or "download" in href.lower()
        if looks_download and _REPORT_HINT.search(signal):
            found[href] = label[:240]
    return sorted(found.items())


def _fetch_listing(url: str, *, timeout: int, attempts: int) -> str:
    text, _size = fetch_text(url, headers=polite_headers(browser=True), timeout=timeout, attempts=attempts)
    return text


def discover_from_seed(
    seed_url: str,
    *,
    subpage_cap: int = 10,
    request_timeout: int = 60,
    request_attempts: int = 2,
) -> list[tuple[str, str]]:
    """Discover report downloads from the seed and at most one bounded archive hop."""
    html = _fetch_listing(seed_url, timeout=request_timeout, attempts=request_attempts)
    found = dict(discover_report_links(seed_url, html))
    soup = BeautifulSoup(html, "html.parser")
    host = urlsplit(seed_url).netloc
    subpages: list[str] = []
    for anchor in soup.find_all("a", href=True):
        href = urljoin(seed_url, str(anchor["href"]).strip())
        signal = f"{anchor.get_text(' ', strip=True)} {href}"
        if (
            urlsplit(href).netloc == host
            and ".pdf" not in href.lower()
            and href != seed_url
            and _ARCHIVE_HINT.search(signal)
            and href not in subpages
        ):
            subpages.append(href)
        if len(subpages) >= subpage_cap:
            break
    for href in subpages:
        try:
            found.update(
                discover_report_links(
                    href,
                    _fetch_listing(href, timeout=request_timeout, attempts=request_attempts),
                )
            )
        except Exception as exc:  # noqa: BLE001 - one archive page must not abort a council
            print(f"  subpage failed {href}: {type(exc).__name__}")
        time.sleep(0.3)
    return sorted(found.items())


def _load_manifest() -> list[dict]:
    if not MANIFEST.exists():
        return []
    return [json.loads(line) for line in MANIFEST.read_text(encoding="utf-8").splitlines() if line.strip()]


def _write_jsonl_atomic(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        "\n".join(json.dumps(row, ensure_ascii=False, sort_keys=True) for row in rows) + "\n",
        encoding="utf-8",
    )
    temporary.replace(path)


def _write_json_atomic(path: Path, payload: dict) -> None:
    """Write a small coverage artifact without leaving a partial census behind."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


def _census_payload(
    seed_rows: list[dict], outcomes: dict[str, dict], *, run_state: str, latest_per_council: int
) -> dict:
    """Account for every statutory council without pretending to measure calendar recall."""
    rows = [outcomes[row["local_authority"]] for row in sorted(seed_rows, key=lambda row: row["local_authority"])]
    return {
        "schema": "council-ce-report-source-census/1",
        "generated_utc": datetime.now(UTC).isoformat(),
        "run_state": run_state,
        "selection": {
            "kind": "latest_discoverable_reports_per_confirmed_council",
            "latest_per_council": latest_per_council,
        },
        "statutory_councils": len(seed_rows),
        "complete_council_accounting": len(rows) == len(seed_rows),
        "outcome_counts": dict(sorted(Counter(row["outcome"] for row in rows).items())),
        "councils": rows,
        "recall": "Unmeasured: this records bounded source discovery, not a meeting-calendar census.",
    }


def _extract_pdf(
    council: str, landing_url: str, source_url: str, title: str, *, request_timeout: int, request_attempts: int
) -> dict:
    fetched_at = datetime.now(UTC).isoformat()
    body = fetch_bytes(
        source_url,
        headers=polite_headers(browser=True),
        timeout=request_timeout,
        attempts=request_attempts,
        validate=lambda value: value.startswith(b"%PDF"),
    )
    month, date_status = report_month_from_source(title, source_url)
    base = {
        "council": council,
        "doc_type": "ce_report",
        "report_title": title or urlsplit(source_url).path.rsplit("/", 1)[-1],
        "report_month": month,
        "date_parse_status": date_status,
        "source_landing_url": landing_url,
        "source_url": source_url,
        "fetched_at": fetched_at,
    }
    if not body:
        return {
            **base,
            "document_id": "",
            "source_file_hash": "",
            "source_pages": 0,
            "extraction_status": "fetch_fail",
            "pages_path": "",
        }

    digest = hashlib.sha256(body).hexdigest()
    document_id = hashlib.sha256(f"{council}|{source_url}".encode()).hexdigest()[:24]
    try:
        document = fitz.open(stream=body, filetype="pdf")
        pages = [page.get_text().strip() for page in document]
    except Exception:  # noqa: BLE001 - retained as an explicit failed record
        return {
            **base,
            "document_id": document_id,
            "source_file_hash": digest,
            "source_pages": 0,
            "extraction_status": "open_fail",
            "pages_path": "",
        }

    native_chars = sum(len(text) for text in pages)
    if native_chars < 80 * max(1, len(pages)):
        return {
            **base,
            "document_id": document_id,
            "source_file_hash": digest,
            "source_pages": len(pages),
            "extraction_status": "needs_ocr",
            "pages_path": "",
        }

    classified_doc_type = classify_document(
        meeting=title,
        source_url=source_url,
        text="\n".join(pages[:2]),
    )
    if classified_doc_type != "ce_report":
        return {
            **base,
            "document_id": document_id,
            "source_file_hash": digest,
            "source_pages": len(pages),
            "extraction_status": f"not_ce_report_{classified_doc_type}",
            "classified_doc_type": classified_doc_type,
            "pages_path": "",
        }

    relative = (
        Path("ce_reports_corpus")
        / re.sub(r"[^a-z0-9]+", "_", council.lower()).strip("_")
        / f"{document_id}.pages.jsonl"
    )
    _write_jsonl_atomic(
        HERE / relative,
        [{"page": number, "text": text} for number, text in enumerate(pages, start=1)],
    )
    return {
        **base,
        "document_id": document_id,
        "source_file_hash": digest,
        "source_pages": len(pages),
        "extraction_status": "text",
        "classified_doc_type": classified_doc_type,
        "pages_path": relative.as_posix(),
    }


def _refresh_cached_metadata(record: dict, *, landing_url: str, source_url: str, title: str) -> dict:
    """Reapply current date/classification rules to cached page text without a network fetch."""
    month, date_status = report_month_from_source(title, source_url)
    updated = {
        **record,
        "report_title": title or str(record.get("report_title") or ""),
        "report_month": month,
        "date_parse_status": date_status,
        "source_landing_url": landing_url,
    }
    pages_path = str(record.get("pages_path") or "")
    path = HERE / pages_path if pages_path else None
    if path is None or not path.exists():
        return updated
    pages = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    classified = classify_document(
        meeting=updated["report_title"],
        source_url=source_url,
        text="\n".join(str(row.get("text") or "") for row in pages[:2]),
    )
    updated["classified_doc_type"] = classified
    updated["extraction_status"] = "text" if classified == "ce_report" else f"not_ce_report_{classified}"
    return updated


def main() -> int:
    parser = argparse.ArgumentParser(description="Harvest CE reports into their separate page-aware lane")
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--council", help="one council or comma-separated council names")
    scope.add_argument("--all-confirmed", action="store_true")
    scope.add_argument("--refresh-cached-only", action="store_true")
    parser.add_argument("--latest-per-council", type=int, default=2)
    parser.add_argument("--allow-reverify", action="store_true")
    parser.add_argument("--request-timeout", type=int, default=60)
    parser.add_argument("--request-attempts", type=int, default=2)
    parser.add_argument("--subpage-cap", type=int, default=10)
    parser.add_argument("--write-census", action=argparse.BooleanOptionalAction, default=None)
    args = parser.parse_args()

    if args.refresh_cached_only:
        rows = _load_manifest()
        refreshed = [
            _refresh_cached_metadata(
                row,
                landing_url=str(row.get("source_landing_url") or ""),
                source_url=str(row.get("source_url") or ""),
                title=str(row.get("report_title") or ""),
            )
            if row.get("pages_path")
            else row
            for row in rows
        ]
        _write_jsonl_atomic(MANIFEST, refreshed)
        print(f"refreshed cached metadata rows={len(refreshed)} -> {MANIFEST}")
        return 0

    if args.request_timeout < 1 or args.request_attempts < 1 or args.subpage_cap < 0:
        parser.error("--request-timeout and --request-attempts must be positive; --subpage-cap cannot be negative")

    all_seeds = list(csv.DictReader(SEEDS.open(encoding="utf-8")))
    seeds = all_seeds
    if args.council:
        requested = {value.strip() for value in args.council.split(",")}
        seeds = [row for row in seeds if row["local_authority"] in requested]
    else:
        seeds = [row for row in seeds if row["status"] == "confirmed"]
    if not args.allow_reverify:
        seeds = [row for row in seeds if row["status"] == "confirmed"]

    write_census = args.all_confirmed if args.write_census is None else args.write_census
    outcomes: dict[str, dict] = {}
    selected_names = {row["local_authority"] for row in seeds}
    for seed in all_seeds:
        council = seed["local_authority"]
        if council in selected_names:
            outcome = "pending"
        elif seed["status"] == "absent":
            outcome = "source_absent"
        elif seed["status"] == "engineering":
            outcome = "engineering_required"
        else:
            outcome = "not_selected"
        outcomes[council] = {
            "council": council,
            "registry_status": seed["status"],
            "selected": council in selected_names,
            "outcome": outcome,
        }

    def _save_census(run_state: str) -> None:
        if write_census:
            _write_json_atomic(
                CENSUS,
                _census_payload(
                    all_seeds,
                    outcomes,
                    run_state=run_state,
                    latest_per_council=max(1, args.latest_per_council),
                ),
            )

    _save_census("in_progress")

    rows = _load_manifest()
    by_url = {str(row.get("source_url") or ""): row for row in rows if row.get("source_url")}
    for seed in seeds:
        council = seed["local_authority"]
        landing_url = seed["seed_url"]
        if not landing_url:
            outcomes[council]["outcome"] = "no_harvestable_seed"
            _save_census("in_progress")
            print(f"[{council}] no harvestable seed ({seed['status']})")
            continue
        try:
            links = discover_from_seed(
                landing_url,
                subpage_cap=args.subpage_cap,
                request_timeout=args.request_timeout,
                request_attempts=args.request_attempts,
            )
        except Exception as exc:  # noqa: BLE001 - coverage failure is reported per council
            outcomes[council].update({"outcome": "listing_failed", "error_type": type(exc).__name__})
            _save_census("in_progress")
            print(f"[{council}] listing failed: {type(exc).__name__}: {exc}")
            continue
        outcomes[council]["discovered_links"] = len(links)
        links = sorted(
            links,
            key=lambda item: (report_month_from_source(item[1], item[0])[0], item[0]),
            reverse=True,
        )[: max(1, args.latest_per_council)]
        outcomes[council]["selected_links"] = len(links)
        outcomes[council]["outcome"] = "no_report_links" if not links else "links_selected"
        print(f"[{council}] {len(links)} selected report links")
        extraction_counts = Counter()
        for source_url, title in links:
            current = by_url.get(source_url)
            if current and current.get("extraction_status") == "text":
                current = _refresh_cached_metadata(
                    current,
                    landing_url=landing_url,
                    source_url=source_url,
                    title=title,
                )
                by_url[source_url] = current
                extraction_counts["cached_text"] += 1
                print(f"  cached {source_url}")
                continue
            record = _extract_pdf(
                council,
                landing_url,
                source_url,
                title,
                request_timeout=args.request_timeout,
                request_attempts=args.request_attempts,
            )
            by_url[source_url] = record
            extraction_counts[str(record["extraction_status"])] += 1
            print(
                f"  {record['extraction_status']} pages={record['source_pages']} "
                f"month={record['report_month'] or 'unresolved'} {source_url}"
            )
            time.sleep(0.4)
        if extraction_counts:
            outcomes[council]["extraction_status_counts"] = dict(sorted(extraction_counts.items()))
        _save_census("in_progress")
    _write_jsonl_atomic(MANIFEST, sorted(by_url.values(), key=lambda row: (row["council"], row["source_url"])))
    _save_census("complete")
    print(f"manifest rows={len(by_url)} -> {MANIFEST}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
