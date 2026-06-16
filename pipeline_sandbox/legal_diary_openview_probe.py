"""EXPERIMENTAL (tracked code, gitignored sandbox data) — pull the Courts Service
Legal Diary jurisdictions that are NOT in the downloadable .docx.

WHY THIS EXISTS
  The production path (pdf_infra/legal_diary_poller.py -> extractors/legal_diary_extract.py)
  grabs the single /download .docx, which covers ONLY the Four Courts current court day
  (Supreme / Court of Appeal / Central Criminal / High). The Circuit Court — and years of
  history for the higher courts — are published behind a Domino *OpenView* index instead,
  one HTML detail document per sitting, and never touch that .docx. So 46 Circuit Court
  judges on our bench roster have an empty "Before the court", and the higher courts are
  capped at one day. This probe proves the OpenView source is fetchable, parseable, and
  survives the SAME privacy gate as the docx pipeline — before we commit to promotion.

SOURCE SHAPE (discovered live 2026-06-16)
  Index:  /legaldiary.nsf/<slug>?OpenView&Jurisdiction=<slug>&...  -> table of <tr
          class="clickable-row" data-url="/legaldiary.nsf/<slug>/<UNID>?OpenDocument">
          cells: Date · [Area/Venue ·] Type · [Sub-title ·] Updated
  Detail: <div class="ld-content"> … </div>, <br />-separated lines, TWO layouts:
    higher courts  "Before Mr. Justice Paul McDermott in Courtroom 06 at 10:15 (For Mention)"
                   then tab-separated  "<idx>\t<record#>\tDPP -v- A H"
    circuit court  "Before Judge Francis Comerford" / "At 10:30 Am" on separate lines,
                   case line  "<idx>\t<record#>\t<parties>\t : \t<solicitor> / <solicitor>"
  Case cells are TAB-delimited, so the party cell (the one with the v-separator) isolates
  cleanly and drops both the record reference AND the solicitor tail — the two quasi-
  identifiers the docx pipeline also strips.

PRIVACY (same model as extractors/legal_diary_extract.py)
  Re-uses that module's anonymiser verbatim: natural persons -> initials, organisations /
  State kept in clear, statutory in-camera categories dropped, and the residual-name gate
  REFUSES to write if any natural-person name survived. The "Circuit Court – Family"
  jurisdiction is in-camera in its entirety and is NOT fetched here at all.

SCOPE / SAFETY
  A PROBE, not the production backfill. The OpenView index holds thousands of rows per
  court (Supreme 1.5k, Appeal 2.9k, Central Criminal 1.6k, Circuit 0.7k); fetching every
  detail doc against a flaky Domino server (RemoteDisconnected) is a promotion-time job.
  Here we SAMPLE --limit detail docs per jurisdiction (newest first) to validate parsing
  and the privacy gate across both layouts. Polite: shared session, retry, small delay.

Output (gitignored):
  data/sandbox/parquet/legal_diary_openview_cases.parquet     (anonymised, gated)
  data/sandbox/parquet/legal_diary_openview_audit.parquet     (RAW — diagnostic only)
  data/_meta/legal_diary_openview_probe.json                  (coverage report)

Run:
  ./.venv/Scripts/python.exe pipeline_sandbox/legal_diary_openview_probe.py
  ./.venv/Scripts/python.exe pipeline_sandbox/legal_diary_openview_probe.py --limit 8 \
      --jurisdictions circuit-court,central-criminal-court
"""

from __future__ import annotations

import argparse
import contextlib
import html as _html
import logging
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import DATA_DIR  # noqa: E402
from extractors.legal_diary_extract import (  # noqa: E402  (re-use the docx anonymiser verbatim)
    _MONTHS,
    PARTY_RE,
    PrivacyInvariantError,
    category_of,
    parties,
    protected_reason,
    residual_name_tokens,
)
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

logger = logging.getLogger(__name__)

BASE = "https://legaldiary.courts.ie"
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
SANDBOX_PARQUET_DIR = DATA_DIR / "sandbox" / "parquet"
META_DIR = DATA_DIR / "_meta"
REPORT_PATH = META_DIR / "legal_diary_openview_probe.json"
SOURCE_NAME = "Courts Service Legal Diary (OpenView)"
TIMEOUT = 60
POLITE_DELAY = 0.3  # seconds between detail fetches — gentle on the Domino server

# OpenView jurisdictions that carry party-level case lists. High Court 500s on OpenView
# (already covered by the .docx) and the District Court publishes only a sittings schedule,
# so neither is here. "circuit-court" is Civil & Criminal; the Family jurisdiction is
# in-camera and deliberately excluded.
JURISDICTIONS = {
    "supreme-court": "Supreme Court",
    "court-of-appeal": "Court of Appeal",  # Criminal rows re-labelled per the row Type
    "central-criminal-court": "Central Criminal Court",
    "circuit-court": "Circuit Court",
}


# ─────────────────────────────────────────────────────────────── http
def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def _get(sess: requests.Session, url: str, attempts: int = 4) -> requests.Response | None:
    """GET with retry. The legaldiary Domino server intermittently drops a keep-alive
    mid-session (RemoteDisconnected); a retry on a fresh attempt clears it. Returns None
    if every attempt fails (a probe should skip a bad doc, not abort the run)."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            r = sess.get(url, timeout=TIMEOUT)
            r.raise_for_status()
            return r
        except requests.RequestException as exc:
            last = exc
            time.sleep(1.0 * (i + 1))
    logger.warning("GET failed after %d attempts: %s (%s)", attempts, url, last)
    return None


# ───────────────────────────────────────────────────────── index parse
def _open_view_url(slug: str) -> str:
    return (
        f"{BASE}/legaldiary.nsf/{slug}?OpenView&Jurisdiction={slug}&area=&type=&dateType=Date&dateFrom=&dateTo=&text="
    )


_INDEX_ROW_RE = re.compile(r'<tr class="clickable-row" data-url="([^"]+)">(.*?)</tr>', re.S)
_CELL_RE = re.compile(r"<td[^>]*data-text=\"([^\"]*)\"[^>]*>(.*?)</td>", re.S)


def _detext(s: str) -> str:
    return _html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s))).strip()


def parse_index(html: str, slug: str) -> list[dict]:
    """One dict per sitting row: the detail data-url plus the row's date / venue / type /
    sub-title. The cell ORDER differs by jurisdiction (circuit has Area/Venue + Sub-title),
    so we read by header position. data-text on the Date cell is a sortable YYYYMMDD."""
    # header order
    headers = [_detext(h).lower() for h in re.findall(r"<th[^>]*>(.*?)</th>", html, re.S)]
    rows: list[dict] = []
    for data_url, body in _INDEX_ROW_RE.findall(html):
        cells = _CELL_RE.findall(body)
        vals = [_detext(txt) for _dt, txt in cells]
        dts = [dt for dt, _txt in cells]
        rec: dict = {"data_url": data_url, "slug": slug}
        for h, v, dt in zip(headers, vals, dts, strict=False):
            if h == "date":
                rec["diary_date"] = _iso_from_yyyymmdd(dt) or _iso_from_text(v)
            elif h == "area/venue":
                rec["venue"] = v
            elif h == "type":
                rec["row_type"] = v
            elif h == "sub-title":
                rec["sub_title"] = v
        rows.append(rec)
    return rows


def _iso_from_yyyymmdd(dt: str) -> str | None:
    return f"{dt[:4]}-{dt[4:6]}-{dt[6:8]}" if re.fullmatch(r"\d{8}", dt or "") else None


def _iso_from_text(s: str) -> str | None:
    # "07 December 2026" / "17th November 2026"
    m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})", s or "")
    if not m:
        return None
    mon = _MONTHS.get(m.group(2).lower())
    return f"{int(m.group(3)):04d}-{mon:02d}-{int(m.group(1)):02d}" if mon else None


# ──────────────────────────────────────────────────────── detail parse
# packed "Before <judge> in <location> at <time> (status)" line. <location> is a
# Courtroom for the Dublin higher courts AND a town for the Circuit Court on circuit
# ("…in Galway at 10:30 (For Hearing)"), so it is matched loosely and anchored on " at ".
_PACKED_RE = re.compile(
    r"^Before\s+(?P<judge>.+?)\s+in\s+(?P<room>.+?)\s+at\s+(?P<time>\d[0-9:.\sapmAPM']*?)\s*(?:\((?P<status>[^)]+)\))?\s*$"
)
# bare "Before <judge>" line (Circuit Court civil — room/time follow on their own lines)
_JUDGE_RE = re.compile(
    r"^Before\s+(?P<judge>(?:Judge|Mr\.?\s+Justice|Ms\.?\s+Justice|Mrs\.?\s+Justice|"
    r"The President|The Chief Justice|Her Honour|His Honour)\b.+?)\s*$",
    re.I,
)
# a lone judge name — the Supreme Court / Court of Appeal list a PANEL, one judge per
# line with no "Before" prefix, so consecutive matches accumulate into one panel.
_JUSTICE_RE = re.compile(
    r"^(?:(?:Mr|Ms|Mrs)\.?\s+Justice\s+\S.*|The Chief Justice|The President(?:\s+of\s+the\s+Court\s+of\s+Appeal)?)\s*$",
    re.I,
)
# a new sitting / date masthead — resets any panel accumulated for the previous sitting
_RESET_RE = re.compile(r"^(?:AN CHÚIRT|\[THE |For (?:Mon|Tues|Wednes|Thurs|Fri|Satur|Sun)day|\(In the )", re.I)
_TIME_RE = re.compile(r"^At\s+(?P<time>.+?)\s*$", re.I)
_VENUE_RE = re.compile(r"^Sitting at\s+(?P<venue>.+?)\s*$", re.I)


def detail_lines(html: str) -> list[str]:
    m = re.search(r'<div class="ld-content">(.*?)</div>', html, re.S)
    if not m:
        return []
    body = re.sub(r"<br\s*/?>", "\n", m.group(1))
    out = []
    for raw in body.split("\n"):
        # keep tabs (case-cell delimiters); drop other tags
        txt = _html.unescape(re.sub(r"<[^>]+>", "", raw)).replace(" ", " ").rstrip()
        if txt.strip():
            out.append(txt)
    return out


def _case_title_from_line(line: str) -> str | None:
    """Pull the party cell out of a case line. Cells are tab-delimited; the party cell is
    the one carrying the v-separator — taking it alone drops the leading index, the record
    reference, and any trailing solicitor cells. Falls back to the whole line (record refs
    are stripped later by parties()->strip_refs) when there are no tabs."""
    cells = [c.strip() for c in line.split("\t") if c.strip()]
    for c in cells:
        if PARTY_RE.search(f" {c} "):
            return c
    if PARTY_RE.search(f" {line} ") or line.upper().lstrip().startswith("IN THE MATTER"):
        return line.strip()
    return None


def parse_detail(lines: list[str], row: dict, court: str, source_url: str) -> list[dict]:
    """State machine over one sitting's lines -> raw case dicts (pre-anonymisation).
    Handles both detail layouts; the judge/room/time carry forward until the next header."""
    judge = room = time_s = status = None
    venue = row.get("venue")
    list_type = row.get("sub_title") or row.get("row_type")
    panel: list[str] = []  # accumulating Supreme/Court-of-Appeal panel judges
    saw_case = False  # a justice line after a case line starts a fresh panel
    cases: list[dict] = []
    for ln in lines:
        if _RESET_RE.match(ln):
            judge = room = time_s = status = None
            panel, saw_case = [], False
            continue
        mv = _VENUE_RE.match(ln)
        if mv:
            venue = mv.group("venue")
            continue
        mp = _PACKED_RE.match(ln)
        if mp:
            judge = mp.group("judge").strip()
            room = re.sub(r"^Courtroom\s+", "", mp.group("room").strip(), flags=re.I)
            time_s, status = mp.group("time").strip(), (mp.group("status") or "").strip() or None
            panel, saw_case = [], False
            continue
        mj = _JUDGE_RE.match(ln)
        if mj:
            judge, room, time_s, status = mj.group("judge").strip(), None, None, None
            panel, saw_case = [], False
            continue
        if _JUSTICE_RE.match(ln):
            if saw_case:  # previous panel's matters are done — this begins a new panel
                panel, saw_case = [], False
            panel.append(ln.strip())
            judge = " & ".join(panel)
            continue
        mt = _TIME_RE.match(ln)
        if mt and judge and not time_s:
            time_s = mt.group("time").strip()
            continue
        title = _case_title_from_line(ln)
        if title:
            saw_case = True
            prot = protected_reason(list_type or "", title)
            cases.append(
                {
                    "slug": row["slug"],
                    "court": court,
                    "venue": venue,
                    "diary_date": row.get("diary_date"),
                    "judge": judge,
                    "courtroom": room,
                    "time": time_s,
                    "status": status,
                    "list_type": list_type,
                    "raw_case": title,
                    "category": category_of(list_type or "", title),
                    "protected": bool(prot),
                    "protected_reason": prot,
                    "source_url": source_url,
                }
            )
    return cases


def _court_for_row(slug: str, row: dict) -> str:
    base = JURISDICTIONS[slug]
    if slug == "court-of-appeal" and (row.get("row_type") or "").lower() == "criminal":
        return "Court of Appeal (Criminal)"
    return base


# ───────────────────────────────────────────────────────── orchestration
def run(args) -> int:
    for d in (SANDBOX_PARQUET_DIR, META_DIR):
        d.mkdir(parents=True, exist_ok=True)
    slugs = [s.strip() for s in (args.jurisdictions or ",".join(JURISDICTIONS)).split(",") if s.strip()]
    bad = [s for s in slugs if s not in JURISDICTIONS]
    if bad:
        logger.error("Unknown jurisdiction(s): %s. Known: %s", bad, list(JURISDICTIONS))
        return 2

    sess = _session()
    all_cases: list[dict] = []
    per_court: dict[str, dict] = {}
    for slug in slugs:
        iv = _get(sess, _open_view_url(slug))
        if iv is None:
            logger.error("Index unreachable for %s — skipping.", slug)
            continue
        index_rows = parse_index(iv.text, slug)
        # newest first (the probe wants current listings, not 2026 far-future scheduling)
        index_rows.sort(key=lambda r: r.get("diary_date") or "", reverse=True)
        sample = index_rows[: args.limit]
        logger.info("%s: %d sittings in index, sampling %d.", slug, len(index_rows), len(sample))
        n_cases = n_docs = 0
        for row in sample:
            detail_url = urljoin(BASE, row["data_url"])
            dr = _get(sess, detail_url)
            time.sleep(POLITE_DELAY)
            if dr is None:
                continue
            n_docs += 1
            court = _court_for_row(slug, row)
            cs = parse_detail(detail_lines(dr.text), row, court, detail_url)
            all_cases += cs
            n_cases += len(cs)
        per_court[slug] = {"index_rows": len(index_rows), "docs_fetched": n_docs, "case_lines": n_cases}

    if not all_cases:
        logger.error("Parsed 0 case lines across %s — source drift or all fetches failed.", slugs)
        return 1

    # infer_schema_length=None: protected_reason is null for most rows and only the rare
    # in-camera hit carries a string, so a short inference window mis-types the column.
    audit = pl.DataFrame(all_cases, infer_schema_length=None)
    save_parquet(audit, SANDBOX_PARQUET_DIR / "legal_diary_openview_audit.parquet")

    # ---- anonymise + privacy gate (identical contract to the docx pipeline) ----
    _pd = pl.Struct(
        [
            pl.Field("case_anonymised", pl.Utf8),
            pl.Field("plaintiff", pl.Utf8),
            pl.Field("defendant", pl.Utf8),
            pl.Field("plaintiff_kind", pl.Utf8),
        ]
    )
    cases = (
        audit.filter(~pl.col("protected"))
        .with_columns(pl.col("raw_case").map_elements(parties, return_dtype=_pd).alias("_p"))
        .unnest("_p")
        .filter(pl.col("case_anonymised").str.len_chars() > 2)
        .select(
            [
                "slug",
                "court",
                "venue",
                "diary_date",
                "judge",
                "courtroom",
                "time",
                "status",
                "list_type",
                "category",
                "case_anonymised",
                "plaintiff",
                "defendant",
                "plaintiff_kind",
                "source_url",
            ]
        )
        .sort(["court", "diary_date", "judge"], nulls_last=True)
    )

    _name_cols = ["case_anonymised", "plaintiff", "defendant"]
    offenders = [
        (f"{col}={r[col]!r}", toks)
        for r in cases.select(_name_cols).iter_rows(named=True)
        for col in _name_cols
        if (toks := residual_name_tokens(r[col] or ""))
    ]
    if offenders:
        sample = " | ".join(f"{c}->{t}" for c, t in offenders[:8])
        raise PrivacyInvariantError(
            f"{len(offenders)} OpenView case cells retain a natural-person name after "
            f"anonymisation; refusing to write. e.g. {sample}"
        )

    save_parquet(cases, SANDBOX_PARQUET_DIR / "legal_diary_openview_cases.parquet")

    n_protected = int(audit.filter(pl.col("protected")).height)
    report = {
        "source_name": SOURCE_NAME,
        "jurisdictions": per_court,
        "case_lines_parsed": int(audit.height),
        "cases_dropped_protected": n_protected,
        "cases_kept_anonymised": int(cases.height),
        "by_court_kept": {r[0]: r[1] for r in cases.group_by("court").len().sort("len", descending=True).iter_rows()},
        "judges_seen": int(cases.select(pl.col("judge").n_unique()).item()),
        "plaintiff_kinds": {
            r[0]: r[1] for r in cases.group_by("plaintiff_kind").len().sort("len", descending=True).iter_rows()
        },
        "privacy_gate": "PASSED — no natural-person name survived anonymisation",
    }
    import json

    REPORT_PATH.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(
        "PROBE OK: %d case lines -> %d anonymised (%d in-camera dropped); gate PASSED. Report -> %s",
        audit.height,
        cases.height,
        n_protected,
        REPORT_PATH,
    )
    logger.info("Kept by court: %s", report["by_court_kept"])
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Probe the Legal Diary OpenView jurisdictions (sandbox).")
    ap.add_argument("--limit", type=int, default=12, help="detail docs sampled per jurisdiction (newest first)")
    ap.add_argument("--jurisdictions", help=f"comma list; default all of {list(JURISDICTIONS)}")
    args = ap.parse_args()
    setup_standalone_logging("legal_diary_openview_probe")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
