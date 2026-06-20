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

Run: .venv/Scripts/python.exe extractors/ministerial_diaries_extract.py
"""

from __future__ import annotations

import logging
import re
import time
from collections import Counter
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import fitz  # PyMuPDF
import polars as pl
import requests
from bs4 import BeautifulSoup

from extractors._diary_minister import minister_from_filename
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

HEADERS = {"User-Agent": "Mozilla/5.0 (dail-tracker civic-data ingestion; contact p.glynn18@gmail.com)"}
OUT_DIR = Path("data/sandbox/enrichment")
PDF_CACHE = Path("C:/tmp/min_diaries_pdfs")
SLEEP_S = 0.5  # polite inter-request pace; bumped from 0.3 to ease off the gov.ie WAF

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
    # gov.ie collection pages that link the diary PDFs DIRECTLY (assets.gov.ie),
    # same flat-listing shape as DETE — verified 2026-06-16. Born-digital PDFs are
    # parsed; any image-only scans fall through to the OCR queue (no OCR on-box).
    ("HEALTH", "https://www.gov.ie/en/department-of-health/collections/department-of-health-ministers-diaries/"),
    (
        "JUSTICE",
        "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/ministerial-diaries/",
    ),
    (
        "HOUSING",
        "https://www.gov.ie/en/department-of-housing-local-government-and-heritage/collections/ministers-diaries/",
    ),
    ("TRANSPORT", "https://www.gov.ie/en/department-of-transport/organisation-information/ministers-diaries/"),
    (
        "DCCS",
        "https://www.gov.ie/en/department-of-culture-communications-and-sport/organisation-information/ministers-diaries/",
    ),
    # Added 2026-06-19 (coverage audit): departments confirmed to publish via gov.ie search but
    # previously absent from this list. Same flat-listing shape (collection/org-info page → diary
    # PDFs). FINANCE publishes PER-MINISTER collections (no single parent page) so each minister's
    # collection is listed separately — ADD the incumbent's collection when the portfolio rotates.
    # McGrath validated (19 PDFs, born-digital); DFHERIS validated (5 calendar PDFs).
    ("FINANCE", "https://www.gov.ie/en/department-of-finance/collections/minister-michael-mcgrath-tds-diary/"),
    ("FINANCE", "https://www.gov.ie/en/department-of-finance/collections/minister-jack-chambers-tds-diary/"),
    (
        "DFHERIS",
        "https://www.gov.ie/en/department-of-further-and-higher-education-research-innovation-and-science/organisation-information/ministers-diary/",
    ),
    # Added 2026-06-19 (round 2 — gov.ie search re-probed from a fresh session, crawler-confirmed
    # PDF counts, not just a 200 on the listing page):
    #   EDUCATION  — single collection page, 172 born-digital diary PDFs (current + former ministers).
    #   TAOISEACH  — the Taoiseach's OWN diary, published quarterly per-year (2022-2024 pages, 4 PDFs each).
    #   DECC       — the Environment/Climate/Communications lineage (Ryan/Bruton/O'Brien/Canney). The
    #                legacy /en/collection/89b20- URL is this dept's diary hub, NOT Social Protection
    #                (the filenames disambiguate it — do not relabel without re-checking the ministers).
    (
        "EDUCATION",
        "https://www.gov.ie/en/department-of-education/collections/department-of-education-ministers-diaries/",
    ),
    ("TAOISEACH", "https://www.gov.ie/en/department-of-the-taoiseach/collections/taoiseachs-diary-2024/"),
    ("TAOISEACH", "https://www.gov.ie/en/department-of-the-taoiseach/collections/taoiseachs-diary-2023/"),
    ("TAOISEACH", "https://www.gov.ie/en/department-of-the-taoiseach/collections/taoiseachs-diary-2022/"),
    ("DECC", "https://www.gov.ie/en/collection/89b20-ministers-diaries/"),
    # TODO (still unresolved after the 2026-06-19 re-probe): Social Protection (the generic
    # "ministers-diaries" search result resolved to DECC above, not DSP — DSP's own slug is still
    # unpinned), Foreign Affairs (only ministerial-briefs, no diary collection found), Agriculture,
    # Defence, Children (no dedicated diary collection surfaced — may not publish on gov.ie). A 404 /
    # "no diary collection" on a search ≠ proof of absence; re-probe each from a fresh session.
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

# filename → period guesses (minister now resolved via _diary_minister.minister_from_filename)
_FN_PERIOD_RE = re.compile(
    r"(january|february|march|april|may|june|july|august|september|october|november|december)"
    r"[-_ ]*(\d{4})?",
    re.IGNORECASE,
)
_YEAR_RE = re.compile(r"(20\d\d)")

# entry-parse line tokens
# Weekday prefix is optional; the 3-letter stem + ``[a-z]*`` tail accepts every spelling the
# published diaries use ("Mon"/"Monday", "Tue"/"Tues"/"Tuesday", "Wed", "Thu"/"Thur"/"Thurs",
# "Sat", "Sun"). The old list missed the bare 3-letter "Wed"/"Thu"/"Sat" forms used by the
# HEALTH/Higgins weekday-list layout, so those date headers never matched (parse_status
# text_layout_unrecognised). Day+month-name after it stays strict, so this cannot false-match.
_WEEKDAY = r"(?:(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)[a-z]*,?\s+)?"
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
# Self-contained one-line entry: "DD/MM/YYYY HH:MM <subject>" (Irish day-first). The early DETE
# 2016-18 layout (Breen/Halligan/Mitchell-O'Connor) and the odd Finance month export ship every
# engagement on a single line — the date->time->subject state machine had no token for it, so the
# files parsed to zero. Requires a non-blank subject after the time, so a bare print-timestamp
# ("14/04/2025 11:35") with nothing trailing falls through to _CAL_NOISE_RE instead of becoming a row.
_INLINE_DT_RE = re.compile(r"^(\d{1,2})/(\d{1,2})/(\d{4})\s+(\d{1,2})[:.](\d{2})\s+(\S.*)$")
# "Month YYYY" section header in a multi-year weekday-list (HEALTH "April 23 to Jan 25", Higgins):
# the short date lines that follow ("Sat 1 Apr") carry no year, so we hold the year from the most
# recent header. NOT an engagement itself. Checked before the date/noise branches so it wins over
# _CAL_NOISE_RE (which would otherwise silently drop it).
_MONTH_YEAR_HEADER_RE = re.compile(
    r"^(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+(20\d\d)$",
    re.IGNORECASE,
)
# Calendar-EXPORT layout noise (the DFIN/Finance "DFIN Diary" Outlook-export generation:
# repeating page header + two mini-calendars per page). Dropping these stops the per-page
# header from being glued onto the previous entry's subject. Tight patterns so they cannot
# match a real engagement line in the other (working) departments' layouts.
_CAL_NOISE_RE = re.compile(
    r"^(?:"
    r"(?-i:[A-Z]{2,6})\s+Diary"  # "DFIN Diary" department-code header (all-caps code, case-sensitive)
    r"|\d{1,2}/\d{1,2}/\d{4}\s+\d{1,2}:\d{2}"  # "14/04/2025 11:35" print timestamp
    r"|Mo\s*Tu\s*We\s*Th\s*Fr\s*Sa\s*Su"  # weekday grid header
    r"|(?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}"  # "April 2024" mini-cal title
    r"|[\d\s]{1,40}"  # bare mini-calendar number rows
    r")$",
    re.IGNORECASE,
)


def _infer_default_year(text: str) -> int | None:
    """Most-frequent plausible year in the document text — used when the filename carries no
    year (e.g. the Finance "April.pdf" calendar exports), so the date state machine isn't
    forced to drop every weekday header for lack of a year."""
    yrs = [int(y) for y in re.findall(r"\b(20\d\d)\b", text) if 2015 <= int(y) <= 2035]
    return Counter(yrs).most_common(1)[0][0] if yrs else None


def discover_files(only_depts: set[str] | None = None) -> list[dict]:
    """Crawl every dept listing page for diary PDF links.

    ``only_depts`` (upper-case department labels) restricts the crawl — used by
    the ``--depts`` smoke-test flag so a single source can be validated without
    crawling all of them.
    """
    rows: list[dict] = []
    seen: set[str] = set()
    sources = [(d, u) for d, u in DEPT_SOURCES if only_depts is None or d in only_depts]
    for dept, listing in sources:
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
            pm = _FN_PERIOD_RE.search(fname)
            ym = _YEAR_RE.search(fname) or _YEAR_RE.search(listing)
            rows.append(
                {
                    "department": dept,
                    "listing_url": listing,
                    "file_url": url,
                    "file_name": fname,
                    "link_text": re.sub(r"\s+", " ", a.get_text(" ", strip=True))[:160] or None,
                    # canonical surname from the filename (multi-token names, "…-Calendar"
                    # files and possessives handled in extractors._diary_minister); the gold
                    # promotion adds the dept+date fallback for name-less generic files.
                    "minister_guess": minister_from_filename(fname),
                    "period_month_guess": (_MONTH_NUM.get(pm.group(1).lower()) if pm else None),
                    "period_year_guess": (int(ym.group(1)) if ym else None),
                }
            )
        log.info("%s: %d diary PDFs on %s", dept, len(rows) - n_before, listing)
        time.sleep(SLEEP_S)
    return rows


# gov.ie / assets.gov.ie front a WAF that 405/429-throttles a BURST of rapid
# downloads (observed 2026-06-19: a full run 405'd ~160 new Education PDFs at
# SLEEP_S pacing). These are transient — the same URL serves fine after a short
# pause — so back off and retry rather than dropping the file to the OCR/retry
# queue. Only a real 404 (or exhausted retries) gives up.
_RETRY_STATUS = {403, 405, 429, 500, 502, 503}


def download(url: str, fname: str, *, retries: int = 4) -> Path | None:
    cache = PDF_CACHE / re.sub(r"[^A-Za-z0-9._-]", "_", fname)[-100:]
    if cache.exists():
        return cache
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=90)
            if r.status_code in _RETRY_STATUS and attempt < retries:
                wait = min(30, 2**attempt)  # 2s, 4s, 8s — let the WAF window reset
                log.info(
                    "download %s -> HTTP %d; backoff %ds (attempt %d/%d)", fname, r.status_code, wait, attempt, retries
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            cache.write_bytes(r.content)
            time.sleep(SLEEP_S)
            return cache
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(min(30, 2**attempt))
                continue
            log.warning("download failed %s: %s", url, e)
            return None
    return None


def parse_entries(text: str, default_year: int | None, default_month: int | None) -> list[dict]:
    """State machine over diary lines: date header → (time|All Day) → subject.

    ``ctx_year`` starts at ``default_year`` and is bumped by any "Month YYYY" section header,
    so a multi-year weekday-list (HEALTH "April 23 to Jan 25") dates its yearless short headers
    correctly instead of stamping the whole document with one inferred year.
    """
    entries: list[dict] = []
    cur_date: date | None = None
    cur_time: str | None = None
    subject_parts: list[str] = []
    ctx_year: int | None = default_year

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
        # one-line "DD/MM/YYYY HH:MM subject" entry (early DETE / Finance export)
        if im := _INLINE_DT_RE.match(line):
            flush()
            try:
                cur_date = date(int(im.group(3)), int(im.group(2)), int(im.group(1)))
            except ValueError:
                cur_date = None
            cur_time = f"{int(im.group(4)):02d}:{im.group(5)}"
            if cur_date:
                subject_parts.append(im.group(6).strip())
            continue
        # "Month YYYY" header → hold the running year for the yearless date lines below
        if my := _MONTH_YEAR_HEADER_RE.match(line):
            ctx_year = int(my.group(1))
            continue
        if dm := (_DATE_FULL_RE.match(line) or _DATE_SHORT_RE.match(line)):
            flush()
            day = int(dm.group(1))
            month = _MONTH_NUM[dm.group(2).lower()[:3] if len(dm.group(2)) <= 4 else dm.group(2).lower()]
            year = int(dm.group(3)) if (dm.lastindex or 0) >= 3 and dm.group(3) else ctx_year
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
        if _NOISE_RE.match(line) or _CAL_NOISE_RE.match(line):
            continue
        if cur_time is not None:
            subject_parts.append(line)
    flush()
    return entries


def main(only_depts: set[str] | None = None, max_files: int | None = None, min_files: int = 100) -> int:
    setup_standalone_logging("ministerial_diaries_extract")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    PDF_CACHE.mkdir(parents=True, exist_ok=True)

    # A restricted/capped run is a smoke test — write to a *_smoke sidecar so it
    # never clobbers the canonical full-corpus parquet.
    tag = "_smoke" if (only_depts or max_files is not None) else ""

    files = discover_files(only_depts=only_depts)
    if max_files is not None:
        files = files[:max_files]
    if len(files) < min_files:
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
        # fall back to the year inferred from the document body when the filename has none
        # (the Finance/DFIN calendar exports) — otherwise the date state machine drops every entry
        year_for_parse = f["period_year_guess"] or _infer_default_year(text)
        entries = parse_entries(text, year_for_parse, f["period_month_guess"])
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
    save_parquet(idx, OUT_DIR / f"ministerial_diaries_index{tag}.parquet")
    idx.write_csv(OUT_DIR / f"ministerial_diaries_index{tag}.csv")
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
        save_parquet(ent, OUT_DIR / f"ministerial_diary_entries{tag}.parquet")
        ent.write_csv(OUT_DIR / f"ministerial_diary_entries{tag}.csv")
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
    import argparse

    ap = argparse.ArgumentParser(description="Ministerial diaries ingestion (index + DETE/gov.ie entry parse).")
    ap.add_argument("--depts", help="comma-separated dept labels to restrict the crawl (e.g. DETE,HEALTH) — smoke test")
    ap.add_argument("--max-files", type=int, default=None, help="cap total diary files processed (smoke test)")
    ap.add_argument("--min-files", type=int, default=100, help="min discovered files before the drift guard trips")
    a = ap.parse_args()
    depts = {d.strip().upper() for d in a.depts.split(",")} if a.depts else None
    # a restricted/capped run is a smoke test — relax the full-corpus drift guard
    min_files = 1 if (depts or a.max_files is not None) else a.min_files
    raise SystemExit(main(only_depts=depts, max_files=a.max_files, min_files=min_files))
