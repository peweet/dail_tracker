"""AHBRA — statutory assessment outcomes + publications (SANDBOX).

Two listings, one silver frame (``record_type`` distinguishes):

1. record_type='statutory_assessment'
   https://www.ahbregulator.ie/compliance/assessment-outcomes/
   HTML tables (one-plus per AHB): Name, AHB Registration Number, Period of
   Review, Date of Assessment Report, Overall Outcome, Status/Engagement,
   per-Standard outcomes, Next Steps. One row per assessment. Site caveat
   baked into ``caveat``: point-in-time outcomes, not a forward compliance
   guarantee (and never proof of wrongdoing).

2. record_type in {annual_report, sectoral_analysis, strategy_statement, ...}
   https://www.ahbregulator.ie/information-guidance/publications/
   PDF corpus (~14 docs) — metadata row per document; PDFs cached to
   bronze/ahbra/pdf/. Contents NOT parsed this pass.

No sanctions register exists as a separate listing today: statutory
non-compliance entries live as columns on the register XLSX itself
(see ahbra_register.py).

Licence note: footer "(c) AHBRA. All rights reserved." — no explicit re-use
licence. Flag for the licence gate.

Run (repo root):  .venv/Scripts/python pipeline_sandbox/new_sources/ahbra_notices.py
"""
from __future__ import annotations

import re
import sys
import time
from datetime import datetime
from pathlib import Path

import polars as pl
import requests
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent))
from _common import POLITE_DELAY_S, cache_raw, now_iso, sha256_bytes, write_silver  # noqa: E402

OUTCOMES_URL = "https://www.ahbregulator.ie/compliance/assessment-outcomes/"
PUBLICATIONS_URL = "https://www.ahbregulator.ie/information-guidance/publications/"
MAX_PDFS = 200  # unexpected-corpus guard: stop and report rather than hammer

# Browser-spoof headers (GOVIE_HEADERS pattern, see procurement_etenders_extract.py).
BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Referer": "https://www.ahbregulator.ie/",
    "Accept-Language": "en-IE,en;q=0.9",
}

CAVEAT = ("Point-in-time statutory assessment outcome; not a forward compliance "
          "guarantee and not proof of wrongdoing")

CATEGORY_MAP = {
    "Annual Reports": "annual_report",
    "Annual Sectoral Analysis": "sectoral_analysis",
    "Strategy Statements": "strategy_statement",
    "Other Publications": "other_publication",
}


def fetch_b(url: str, binary: bool = False, timeout: int = 60):
    time.sleep(POLITE_DELAY_S)
    r = requests.get(url, headers=BROWSER_HEADERS, timeout=timeout)
    r.raise_for_status()
    meta = {
        "source_url": r.url,
        "content_type": r.headers.get("content-type", ""),
        "source_last_modified": r.headers.get("last-modified"),
        "source_document_hash": sha256_bytes(r.content),
        "fetched_at": now_iso(),
        "bytes": len(r.content),
    }
    return (r.content if binary else r.text), meta


def norm_reg_no(s: str | None) -> str | None:
    """'AHB – 02751' (en-dash, stray spaces/NBSP) -> 'AHB-02751' to match the register."""
    if not s:
        return None
    s = s.replace(chr(0xA0), ' ').strip()
    return re.sub(r'\s*[\u2010-\u2015-]\s*', '-', s) or None


def iso_date(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"(\d{1,2})(st|nd|rd|th)\b", r"\1", s.strip())  # '26th March 2026'
    for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d %B %Y"):
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


BLANK = {
    "ahb_name": None, "ahb_registration_number": None, "period_of_review": None,
    "assessment_report_date": None, "assessment_report_date_raw": None,
    "overall_outcome": None, "status_engagement": None, "governance_standard": None,
    "financial_standard": None, "property_asset_standard": None,
    "tenancy_standard": None, "next_steps": None, "category": None,
    "language": None, "document_url": None, "bronze_path": None, "caveat": None,
}


def parse_assessments() -> list[dict]:
    html, meta = fetch_b(OUTCOMES_URL)
    cache_raw("ahbra", "assessment_outcomes.html", html.encode("utf-8"))
    s = BeautifulSoup(html, "html.parser")
    rows, skipped, continuation, collapsed = [], 0, 0, 0
    for table in s.find_all("table"):
        trs = table.find_all("tr")
        if not trs:
            continue
        header = [c.get_text(" ", strip=True) for c in trs[0].find_all(["th", "td"])]
        if not header or "Name" not in header[0] or len(header) < 6:
            # single-cell continuation blobs (Next Steps spillover) render as
            # their own tables — metadata-free, so skipped rather than guessed at
            skipped += 1
            continue
        idx = {h: i for i, h in enumerate(header)}

        def col(cells: list[str], label: str) -> str | None:
            i = idx.get(label)
            return cells[i] if i is not None and i < len(cells) and cells[i] else None

        for tr in trs[1:]:
            cells = [c.get_text(" ", strip=True) for c in tr.find_all(["th", "td"])]
            if not cells or not cells[0] or cells[0] == "Name":
                continue
            if len(cells) < 6:
                continuation += 1  # trailing spillover row inside the table — no fields
                continue
            if len(cells) == len(header) + 1:
                # source markup quirk: one cell duplicated (name or reg no) shifts
                # everything right — collapse the first adjacent duplicate pair
                for i in range(len(cells) - 1):
                    if cells[i] == cells[i + 1]:
                        del cells[i + 1]
                        collapsed += 1
                        break
            raw_date = col(cells, "Date of Assessment Report")
            rows.append({**BLANK,
                "record_type": "statutory_assessment",
                "title": f"Standards Assessment — {cells[0]} ({col(cells, 'Period of Review') or 'n/a'})",
                "ahb_name": cells[0],
                "ahb_registration_number": norm_reg_no(col(cells, "AHB Registration Number")),
                "period_of_review": col(cells, "Period of Review"),
                "assessment_report_date": iso_date(raw_date),
                "assessment_report_date_raw": raw_date,
                "overall_outcome": col(cells, "Overall Outcome"),
                "status_engagement": col(cells, "Status/Engagement"),
                "governance_standard": col(cells, "Governance Standard"),
                "financial_standard": col(cells, "Financial Standard"),
                "property_asset_standard": col(cells, "Property & Asset Management Standard"),
                "tenancy_standard": col(cells, "Tenancy Management Standard"),
                "next_steps": col(cells, "Next Steps"),
                "caveat": CAVEAT,
                "source_url": meta["source_url"],
                "source_document_hash": meta["source_document_hash"],
                "fetched_at": meta["fetched_at"],
                "source_published_date": iso_date(raw_date),
                "source_last_modified": meta["source_last_modified"],
                "extraction_method": "html_scrape",
                "confidence": "high",
                "privacy_tier": "public",
            })
    print(f"assessments: {len(rows)} rows ({skipped} non-assessment tables skipped, "
          f"{continuation} continuation rows skipped, {collapsed} split-cell rows repaired)")
    return rows


def parse_publications() -> list[dict]:
    html, page_meta = fetch_b(PUBLICATIONS_URL)
    cache_raw("ahbra", "publications.html", html.encode("utf-8"))
    s = BeautifulSoup(html, "html.parser")
    links, seen = [], set()
    for a in s.find_all("a", href=True):
        href = a["href"]
        if ".pdf" not in href.lower() or href in seen:
            continue
        seen.add(href)
        h = a.find_previous(["h2", "h3"])
        links.append((href, a.get_text(" ", strip=True), h.get_text(strip=True) if h else None))
    if len(links) > MAX_PDFS:
        raise RuntimeError(f"publications listing has {len(links)} PDFs (> {MAX_PDFS}) — stopping; report to owner")

    rows = []
    for href, text, heading in links:
        title = text or Path(href).name
        m = re.search(r"/uploads/(\d{4})/(\d{2})/", href)
        try:
            blob, meta = fetch_b(href, binary=True)
            bronze_path, _ = cache_raw("ahbra/pdf", Path(href).name, blob)
            bronze_str, doc_hash = str(bronze_path), meta["source_document_hash"]
            lastmod, fetched = meta["source_last_modified"], meta["fetched_at"]
            print(f"  pdf cached: {Path(href).name} ({meta['bytes']:,} B)")
        except Exception as e:  # noqa: BLE001 — keep the metadata row on a dead link
            print(f"  pdf FAILED {href}: {type(e).__name__}: {e}")
            bronze_str, doc_hash = None, None
            lastmod, fetched = None, now_iso()
        rows.append({**BLANK,
            "record_type": CATEGORY_MAP.get(heading or "",
                                            re.sub(r"[^a-z0-9]+", "_", (heading or "publication").lower()).strip("_")),
            "title": title,
            "category": heading,
            "language": "ga" if ("irish" in title.lower() or "tuarasc" in title.lower()
                                 or "raiteas" in title.lower() or "ráiteas" in title.lower()) else "en",
            "document_url": href,
            "bronze_path": bronze_str,
            "source_url": href,
            "source_document_hash": doc_hash,
            "fetched_at": fetched,
            "source_published_date": f"{m.group(1)}-{m.group(2)}" if m else None,  # month precision from upload path
            "source_last_modified": lastmod,
            "extraction_method": "html_scrape+pdf_cache",
            "confidence": "high",
            "privacy_tier": "public",
        })
    print(f"publications: {len(rows)} rows")
    return rows


def run() -> None:
    rows = parse_assessments() + parse_publications()
    df = pl.DataFrame(rows)
    out = write_silver("ahbra_notices", df)

    print(f"\nSILVER: {out}  rows={df.height}")
    print("  record_type counts:", dict(df.group_by("record_type").len().sort("len", descending=True).iter_rows()))
    asmt = df.filter(pl.col("record_type") == "statutory_assessment")
    if asmt.height:
        d = asmt.drop_nulls("assessment_report_date")["assessment_report_date"]
        print(f"  assessments: {asmt.height} rows, {asmt['ahb_name'].n_unique()} AHBs, "
              f"dates {d.min()} .. {d.max()} (unparsed dates: {asmt.height - d.len()})")
        print("  overall outcomes:", dict(asmt.group_by("overall_outcome").len()
                                          .sort("len", descending=True).iter_rows()))
        print(f"  reg-number null rate: {asmt['ahb_registration_number'].null_count()}/{asmt.height}")
    pubs = df.filter(pl.col("record_type") != "statutory_assessment")
    if pubs.height:
        pd_ = pubs.drop_nulls("source_published_date")["source_published_date"]
        print(f"  publications: {pubs.height} docs, cached={pubs['bronze_path'].drop_nulls().len()}, "
              f"upload-months {pd_.min()} .. {pd_.max()}")


if __name__ == "__main__":
    run()
