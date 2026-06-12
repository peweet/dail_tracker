"""Ministerial diaries (gov.ie / DETE) — sandbox ingestion: index + DETE entry parse.

Scoped 2026-06-12 (new-enrichment-sources round 2). Ministers' diaries are
published quarterly-in-arrears per department — the OTHER side of the lobbying
register's "we lobbied Minister X" returns (Minister Activity page dep,
IDEAS.md §12). There is NO central hub: each department runs its own
collection pages with its own filename conventions and its own scanning habits.

Coverage v1 (probe findings 2026-06-12):
  - DETE (enterprise.gov.ie): single listing page, ~150 per-minister-per-month
    PDFs, BORN-DIGITAL → entries parsed here. Two layout generations:
      2022-24:  "04 January 2023" / "Time 09:30 – 10:30" / "Subject Pre-Cabinet"
      2025-26:  "9 Feb" / "10:00 – 12:30" (or "All Day") / subject line(s)
  - DPER (gov.ie): per-year collections 2017-2026 + 3 minister-of-state
    collections, BUT every sampled PDF is an image-only SCAN (0-char text
    layer) → indexed as an OCR queue, NOT parsed. OCR must run OFF-BOX
    (feedback_paddleocr_crashes_local_box); this mirrors the
    sipo_candidate_expenses_sources.csv queue pattern.

Other departments (Health, DFA, Justice, ...) publish too — extend
DEPT_SOURCES once their listing slugs are probed.

HONESTY CAVEATS for any future surface: diaries are self-curated and
explicitly non-exhaustive ("may not reflect every engagement"); subjects are
free-text; a diary meeting is NOT a lobbying-register return and must never
be presented as one (feedback_no_inference_in_app — co-occurrence only).

Outputs -> data/sandbox/enrichment/
  ministerial_diaries_index.parquet / .csv    one row per published diary file
  ministerial_diary_entries.parquet / .csv    one row per parsed engagement (DETE)
PDF cache -> C:/tmp/min_diaries_pdfs/ (transient)

Run: .venv/Scripts/python.exe pipeline_sandbox/ministerial_diaries_extract.py
"""

from __future__ import annotations

import logging
import re
import time
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import fitz  # PyMuPDF
import polars as pl
import requests
from bs4 import BeautifulSoup

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (dail-tracker civic-data ingestion; contact p.glynn18@gmail.com)"}
OUT_DIR = Path("data/sandbox/enrichment")
PDF_CACHE = Path("C:/tmp/min_diaries_pdfs")
SLEEP_S = 0.3

_DPER = (
    "https://www.gov.ie/en/department-of-public-expenditure-infrastructure-"
    "public-service-reform-and-digitalisation/collections/"
)
DEPT_SOURCES: list[tuple[str, str]] = [
    ("DETE", "https://enterprise.gov.ie/en/who-we-are/ministers/ministers-diaries/"),
    *[("DPER", f"{_DPER}ministerial-diaries-{y}/") for y in range(2017, 2027)],
    ("DPER", f"{_DPER}minister-of-state-higgins-diaries/"),
    ("DPER", f"{_DPER}minister-of-state-smyths-diaries/"),
    ("DPER", f"{_DPER}minister-of-states-diaries/"),
]

MONTHS = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
]
_MONTH_NUM = {m: i + 1 for i, m in enumerate(MONTHS)}
_MONTH_NUM.update({m[:3]: i + 1 for i, m in enumerate(MONTHS)})

# filename → minister / period guesses
_FN_MINISTER_RE = re.compile(r"minister[-_ ]+(?:of[-_ ]state[-_ ]+)?([a-z]+)[-_ ]+diary", re.IGNORECASE)
_FN_PERIOD_RE = re.compile(
    r"(january|february|march|april|may|june|july|august|september|october|november|december)"
    r"[-_ ]*(\d{4})?",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"(20\d\d)")

# entry-parse line tokens
_WEEKDAY = r"(?:(?:Mon|Tues?|Wednes|Thurs?|Fri|Satur|Sun)(?:day)?,?\s+)?"
_DATE_FULL_RE = re.compile(
    rf"^{_WEEKDAY}(\d{{1,2}})(?:st|nd|rd|th)?\s+(January|February|March|April|May|June|July|"
    r"August|September|October|November|December)(?:\s+(\d{4}))?$",
    re.IGNORECASE,
)
_DATE_SHORT_RE = re.compile(
    rf"^{_WEEKDAY}(\d{{1,2}})(?:st|nd|rd|th)?\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.?$",
    re.IGNORECASE,
)
# bare time slot, or time slot with the subject inline on the same line
_TIME_RE = re.compile(r"^(?:Time\s+)?(\d{1,2}[:.]\d{2})\s*[–—-]\s*(\d{1,2}[:.]\d{2})\s*(\S.*)?$", re.IGNORECASE)
_ALL_DAY_RE = re.compile(r"^All\s*Day\s*(\S.*)?$", re.IGNORECASE)
_NOISE_RE = re.compile(r"^(Time|Subject|Details|Date|Minister .{0,60}(Diary|Calendar).*)$", re.IGNORECASE)


def discover_files() -> list[dict]:
    """Crawl every dept listing page for diary PDF links."""
    rows: list[dict] = []
    seen: set[str] = set()
    for dept, listing in DEPT_SOURCES:
        try:
            r = requests.get(listing, headers=HEADERS, timeout=60)
            r.raise_for_status()
        except Exception as e:
            log.warning("listing failed %s: %s", listing, e)
            continue
        soup = BeautifulSoup(r.text, "html.parser")
        n_before = len(rows)
        for a in soup.find_all("a", href=True):
            href: str = a["href"]
            if ".pdf" not in href.lower():
                continue
            url = urljoin(listing, href)
            low = url.lower()
            # diaries only — listing pages also link other publications
            if not ("diar" in low or "calendar" in low or _FN_PERIOD_RE.search(Path(low).name)):
                continue
            if url in seen:
                continue
            seen.add(url)
            fname = Path(url.split("?")[0]).name
            mm = _FN_MINISTER_RE.search(fname)
            pm = _FN_PERIOD_RE.search(fname)
            ym = _YEAR_RE.search(fname) or _YEAR_RE.search(listing)
            rows.append(
                {
                    "department": dept,
                    "listing_url": listing,
                    "file_url": url,
                    "file_name": fname,
                    "link_text": re.sub(r"\s+", " ", a.get_text(" ", strip=True))[:160] or None,
                    "minister_guess": (mm.group(1).title() if mm else None),
                    "period_month_guess": (_MONTH_NUM.get(pm.group(1).lower()) if pm else None),
                    "period_year_guess": (int(ym.group(1)) if ym else None),
                }
            )
        log.info("%s: %d diary PDFs on %s", dept, len(rows) - n_before, listing)
        time.sleep(SLEEP_S)
    return rows


def download(url: str, fname: str) -> Path | None:
    cache = PDF_CACHE / re.sub(r"[^A-Za-z0-9._-]", "_", fname)[-100:]
    if cache.exists():
        return cache
    try:
        r = requests.get(url, headers=HEADERS, timeout=90)
        r.raise_for_status()
        cache.write_bytes(r.content)
        time.sleep(SLEEP_S)
        return cache
    except Exception as e:
        log.warning("download failed %s: %s", url, e)
        return None


def parse_entries(text: str, default_year: int | None, default_month: int | None) -> list[dict]:
    """State machine over diary lines: date header → (time|All Day) → subject."""
    entries: list[dict] = []
    cur_date: date | None = None
    cur_time: str | None = None
    subject_parts: list[str] = []

    def flush() -> None:
        nonlocal subject_parts, cur_time
        if cur_date and cur_time and subject_parts:
            entries.append(
                {
                    "entry_date": cur_date,
                    "time_slot": cur_time,
                    "subject": re.sub(r"\s+", " ", " ".join(subject_parts)).strip(),
                }
            )
        subject_parts, cur_time = [], None

    for raw in text.splitlines():
        line = raw.strip().strip("​")
        if not line:
            continue
        if dm := (_DATE_FULL_RE.match(line) or _DATE_SHORT_RE.match(line)):
            flush()
            day = int(dm.group(1))
            month = _MONTH_NUM[dm.group(2).lower()[:3] if len(dm.group(2)) <= 4 else dm.group(2).lower()]
            year = int(dm.group(3)) if (dm.lastindex or 0) >= 3 and dm.group(3) else default_year
            if year is None:
                cur_date = None
                continue
            try:
                cur_date = date(year, month, day)
            except ValueError:
                cur_date = None
            continue
        if tm := _TIME_RE.match(line):
            flush()
            cur_time = f"{tm.group(1).replace('.', ':')}-{tm.group(2).replace('.', ':')}"
            if tm.group(3):  # subject inline on the same line
                subject_parts.append(tm.group(3).strip())
            continue
        if am := _ALL_DAY_RE.match(line):
            flush()
            cur_time = "all-day"
            if am.group(1):
                subject_parts.append(am.group(1).strip())
            continue
        if _NOISE_RE.match(line):
            continue
        if cur_time is not None:
            subject_parts.append(line)
    flush()
    return entries


def main() -> int:
    setup_standalone_logging("ministerial_diaries_extract")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PDF_CACHE.mkdir(parents=True, exist_ok=True)

    files = discover_files()
    if len(files) < 100:
        log.error("Only %d diary files discovered (expected ~270) — listing drift?", len(files))
        return 1

    all_entries: list[dict] = []
    for f in files:
        pdf = download(f["file_url"], f["file_name"])
        if pdf is None:
            f.update(
                {"n_pages": None, "has_text_layer": None, "n_entries_parsed": 0, "parse_status": "download_failed"}
            )
            continue
        try:
            doc = fitz.open(pdf)
            text = "".join(p.get_text() for p in doc)
            f["n_pages"] = len(doc)
        except Exception as e:
            log.warning("unreadable pdf %s: %s", pdf.name, e)
            f.update({"n_pages": None, "has_text_layer": False, "n_entries_parsed": 0, "parse_status": "unreadable"})
            continue
        has_text = len(text.strip()) > 100
        f["has_text_layer"] = has_text
        if not has_text:
            f.update({"n_entries_parsed": 0, "parse_status": "scanned_needs_offbox_ocr"})
            continue
        entries = parse_entries(text, f["period_year_guess"], f["period_month_guess"])
        f["n_entries_parsed"] = len(entries)
        f["parse_status"] = "parsed" if entries else "text_layout_unrecognised"
        for e in entries:
            e.update(
                {
                    "department": f["department"],
                    "minister": f["minister_guess"],
                    "source_pdf_url": f["file_url"],
                }
            )
        all_entries.extend(entries)

    idx = pl.DataFrame(files, infer_schema_length=None).with_columns(pl.lit(date.today()).alias("ingested_date"))
    save_parquet(idx, OUT_DIR / "ministerial_diaries_index.parquet")
    idx.write_csv(OUT_DIR / "ministerial_diaries_index.csv")
    log.info(
        "INDEX: %d files | text-layer=%d | scanned(OCR queue)=%d | parsed=%d | layout-unrecognised=%d",
        len(idx),
        (idx["has_text_layer"] == True).sum(),  # noqa: E712
        (idx["parse_status"] == "scanned_needs_offbox_ocr").sum(),
        (idx["parse_status"] == "parsed").sum(),
        (idx["parse_status"] == "text_layout_unrecognised").sum(),
    )

    if all_entries:
        ent = pl.DataFrame(all_entries, infer_schema_length=None).with_columns(
            pl.lit(date.today()).alias("ingested_date")
        )
        save_parquet(ent, OUT_DIR / "ministerial_diary_entries.parquet")
        ent.write_csv(OUT_DIR / "ministerial_diary_entries.csv")
        log.info(
            "ENTRIES: %d rows | %s -> %s | by minister: %s",
            len(ent),
            ent["entry_date"].min(),
            ent["entry_date"].max(),
            ent.group_by("minister").len().sort("len", descending=True).head(6).to_dicts(),
        )
    else:
        log.warning("No entries parsed at all — layout drift?")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
