"""legal_diary_openview_extract.py — Legal Diary OpenView HTML archive -> judiciary gold.

The promotion of pipeline_sandbox/legal_diary_openview_probe.py. Reads the raw HTML
detail documents archived by pdf_infra/legal_diary_openview_poller.py (one per sitting,
under data/bronze/legal_diary_openview/<slug>/<UNID>.html) and produces the SAME
privacy-tiered, anonymised gold facts as the .docx pipeline — for the Circuit Court (the
.docx omits it entirely) and the higher courts' full history.

REUSE: the anonymiser and the privacy gate are imported VERBATIM from
extractors/legal_diary_extract.py — there is ONE privacy implementation, shared by both
the .docx and OpenView paths (parties / protected_reason / category_of /
residual_name_tokens / PrivacyInvariantError). Only the source PARSER differs (HTML
OpenView layouts vs the .docx state machine).

OUTPUTS (gold, committed — anonymised / no-party-data only):
  data/gold/parquet/judicial_legal_diary_openview_cases.parquet      (Tier C, ANONYMISED)
  data/gold/parquet/judicial_legal_diary_openview_schedule.parquet   (Tier A, officials only)
META:
  data/_meta/judicial_legal_diary_openview_coverage.json
SANDBOX (RAW, never promoted):
  data/sandbox/parquet/judicial_legal_diary_openview_audit.parquet

SCOPE (doc/LEGAL_DIARY_OPENVIEW_BUILD_PLAN.md): canonical for Supreme / Court of Appeal /
Central Criminal / Circuit. The High Court stays on the .docx path (OpenView 500s) and is
NOT produced here, so the two gold sets are disjoint by court — no de-duplication needed.

Run:
  ./.venv/Scripts/python.exe extractors/legal_diary_openview_extract.py
"""

from __future__ import annotations

import argparse
import contextlib
import html as _html
import json
import logging
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import BRONZE_DIR, DATA_DIR, GOLD_PARQUET_DIR  # noqa: E402
from extractors.legal_diary_extract import (  # noqa: E402  (the ONE shared anonymiser + gate)
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

PARSER_VERSION = "1.0.0"
SOURCE_NAME = "Courts Service Legal Diary (OpenView)"
BASE = "https://legaldiary.courts.ie"
ARCHIVE_DIR = BRONZE_DIR / "legal_diary_openview"
SANDBOX_PARQUET_DIR = DATA_DIR / "sandbox" / "parquet"
META_DIR = DATA_DIR / "_meta"
COVERAGE_PATH = META_DIR / "judicial_legal_diary_openview_coverage.json"

JURISDICTIONS = {
    "supreme-court": "Supreme Court",
    "court-of-appeal": "Court of Appeal",  # Criminal re-labelled per the Category/Type cell
    "central-criminal-court": "Central Criminal Court",
    "circuit-court": "Circuit Court",
}


# ───────────────────────────────────────────────────── detail metadata
_CELL_RE = re.compile(r'<span class="cell-title">(.*?)</span>(.*?)</div>', re.S)


def _detext(s: str) -> str:
    return _html.unescape(re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", s))).strip()


def parse_meta(html: str) -> dict:
    """The alfresco-properties cells: {cell-title -> value}. Carries Date, Updated,
    Category (Civil/Criminal), Type (sub-title), and — for the Circuit Court — a cell
    whose title is the court name and whose value is the venue town."""
    return {_detext(t): _detext(v) for t, v in _CELL_RE.findall(html)}


def _iso_date(s: str) -> str | None:
    # "7th December 2026" -> "2026-12-07"
    m = re.search(r"(\d{1,2})(?:st|nd|rd|th)?\s+([A-Za-z]+)\s+(\d{4})", s or "")
    if not m:
        return None
    mon = _MONTHS.get(m.group(2).lower())
    return f"{int(m.group(3)):04d}-{mon:02d}-{int(m.group(1)):02d}" if mon else None


def court_and_meta(slug: str, meta: dict) -> tuple[str, dict]:
    """Resolve the court label + the row context (date / venue / list_type) from the
    detail's metadata cells. Court of Appeal splits Civil vs Criminal on its category."""
    category = meta.get("Category", "")
    sub_type = meta.get("Type", "")
    court = JURISDICTIONS[slug]
    if slug == "court-of-appeal" and "criminal" in f"{category} {sub_type}".lower():
        court = "Court of Appeal (Criminal)"
    venue = meta.get(JURISDICTIONS[slug])  # circuit puts the venue under a court-named cell
    return court, {
        "diary_date": _iso_date(meta.get("Date", "")),
        "venue": venue,
        "list_type": sub_type or category or None,
    }


# ───────────────────────────────────────────────────── detail body parse
# packed "Before <judge> in <location> at <time> (status)"; <location> is a Courtroom for
# the Dublin courts and a town for the Circuit Court on circuit, so matched loosely.
_PACKED_RE = re.compile(
    r"^Before\s+(?P<judge>.+?)\s+in\s+(?P<room>.+?)\s+at\s+(?P<time>\d[0-9:.\sapmAPM']*?)\s*(?:\((?P<status>[^)]+)\))?\s*$"
)
_JUDGE_RE = re.compile(
    r"^Before\s+(?P<judge>(?:Judge|Mr\.?\s+Justice|Ms\.?\s+Justice|Mrs\.?\s+Justice|"
    r"The President|The Chief Justice|Her Honour|His Honour)\b.+?)\s*$",
    re.I,
)
# Supreme / Court of Appeal list a PANEL — one judge per line, no "Before" prefix.
_JUSTICE_RE = re.compile(
    r"^(?:(?:Mr|Ms|Mrs)\.?\s+Justice\s+\S.*|The Chief Justice|The President(?:\s+of\s+the\s+Court\s+of\s+Appeal)?)\s*$",
    re.I,
)
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
        txt = _html.unescape(re.sub(r"<[^>]+>", "", raw)).replace(" ", " ").rstrip()
        if txt.strip():
            out.append(txt)
    return out


def _case_title(line: str) -> str | None:
    """The party cell from a tab-delimited case line — the cell carrying the v-separator.
    Taking it alone drops the leading index, the record reference, AND any solicitor cells.
    Falls back to the whole line (record refs stripped later by parties()->strip_refs)."""
    cells = [c.strip() for c in line.split("\t") if c.strip()]
    for c in cells:
        if PARTY_RE.search(f" {c} "):
            return c
    if PARTY_RE.search(f" {line} ") or line.upper().lstrip().startswith("IN THE MATTER"):
        return line.strip()
    return None


def parse_detail(lines: list[str], court: str, ctx: dict, source_url: str) -> list[dict]:
    """State machine over one sitting's body -> raw case dicts (pre-anonymisation). Handles
    the packed, bare-judge and panel layouts; judge/room/time carry until the next header.
    Panel judges (Supreme / Court of Appeal) are joined with ' & '."""
    judge = room = time_s = status = None
    venue = ctx.get("venue")
    list_type = ctx.get("list_type")
    panel: list[str] = []
    saw_case = False
    cases: list[dict] = []
    for ln in lines:
        if _RESET_RE.match(ln):
            judge = room = time_s = status = None
            panel, saw_case = [], False
            continue
        if (mv := _VENUE_RE.match(ln)) and not venue:
            venue = mv.group("venue")
            continue
        if mp := _PACKED_RE.match(ln):
            judge = mp.group("judge").strip()
            room = re.sub(r"^Courtroom\s+", "", mp.group("room").strip(), flags=re.I)
            time_s, status = mp.group("time").strip(), (mp.group("status") or "").strip() or None
            panel, saw_case = [], False
            continue
        if mj := _JUDGE_RE.match(ln):
            judge, room, time_s, status = mj.group("judge").strip(), None, None, None
            panel, saw_case = [], False
            continue
        if _JUSTICE_RE.match(ln):
            if saw_case:
                panel, saw_case = [], False
            panel.append(ln.strip())
            judge = " & ".join(panel)
            continue
        if (mt := _TIME_RE.match(ln)) and judge and not time_s:
            time_s = mt.group("time").strip()
            continue
        title = _case_title(ln)
        if title:
            saw_case = True
            prot = protected_reason(list_type or "", title)
            cases.append(
                {
                    "court": court,
                    "venue": venue,
                    "diary_date": ctx.get("diary_date"),
                    "judge": judge,
                    "courtroom": room,
                    "time": time_s,
                    "status": status,
                    "list_type": list_type,
                    "panel_size": (judge.count(" & ") + 1) if judge else 0,
                    "raw_case": title,
                    "category": category_of(list_type or "", title),
                    "protected": bool(prot),
                    "protected_reason": prot,
                    "source_url": source_url,
                }
            )
    return cases


# ───────────────────────────────────────────────────────── orchestration
def _iter_archive(slugs: list[str]):
    for slug in slugs:
        d = ARCHIVE_DIR / slug
        if not d.exists():
            continue
        for f in sorted(d.glob("*.html")):
            yield slug, f


def run(args) -> int:
    for d in (GOLD_PARQUET_DIR, SANDBOX_PARQUET_DIR, META_DIR):
        d.mkdir(parents=True, exist_ok=True)
    slugs = [s.strip() for s in (args.jurisdictions or ",".join(JURISDICTIONS)).split(",") if s.strip()]

    all_cases: list[dict] = []
    n_docs = 0
    for slug, path in _iter_archive(slugs):
        html = path.read_text(encoding="utf-8", errors="ignore")
        court, ctx = court_and_meta(slug, parse_meta(html))
        source_url = f"{BASE}/legaldiary.nsf/{slug}/{path.stem}?OpenDocument"
        cs = parse_detail(detail_lines(html), court, ctx, source_url)
        all_cases += cs
        n_docs += 1

    if not all_cases:
        logger.error(
            "Parsed 0 case lines from %d archived doc(s) under %s. Run the poller first, "
            "or the archive HTML drifted. NOT writing gold (would clobber).",
            n_docs,
            ARCHIVE_DIR,
        )
        return 2

    audit = pl.DataFrame(all_cases, infer_schema_length=None)
    save_parquet(audit, SANDBOX_PARQUET_DIR / "judicial_legal_diary_openview_audit.parquet")

    # ---- Tier C: anonymised cases (GOLD) ----
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
        .with_columns(pl.lit(SOURCE_NAME).alias("source"))
        .select(
            [
                "court",
                "venue",
                "diary_date",
                "judge",
                "courtroom",
                "time",
                "status",
                "list_type",
                "panel_size",
                "category",
                "case_anonymised",
                "plaintiff",
                "defendant",
                "plaintiff_kind",
                "source",
                "source_url",
            ]
        )
        .sort(["court", "diary_date", "judge"], nulls_last=True)
    )

    # ---- PRIVACY GATE (runtime, -O-proof; identical contract to the .docx pipeline) ----
    forbidden = {"raw_case", "party", "parties", "solicitor", "solicitors"} & set(cases.columns)
    if forbidden:
        raise PrivacyInvariantError(f"raw-name column(s) leaked into gold cases: {sorted(forbidden)}")
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
            f"{len(offenders)} OpenView gold case cells retain a natural-person name after "
            f"anonymisation; refusing to write gold. e.g. {sample}"
        )
    save_parquet(cases, GOLD_PARQUET_DIR / "judicial_legal_diary_openview_cases.parquet")

    # ---- Tier A: schedule (officials only) — one row per sitting with an item count ----
    schedule = (
        cases.group_by(["court", "venue", "diary_date", "judge", "courtroom", "time", "list_type", "panel_size"])
        .agg(pl.len().alias("n_items"))
        .sort(["diary_date", "court", "venue", "judge"], nulls_last=True)
    )
    save_parquet(schedule, GOLD_PARQUET_DIR / "judicial_legal_diary_openview_schedule.parquet")

    n_protected = int(audit.filter(pl.col("protected")).height)
    coverage = {
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "source_name": SOURCE_NAME,
        "docs_parsed": n_docs,
        "case_lines_parsed": int(audit.height),
        "cases_dropped_protected": n_protected,
        "cases_kept_anonymised": int(cases.height),
        "sittings": int(schedule.height),
        "by_court": {r[0]: r[1] for r in cases.group_by("court").len().sort("len", descending=True).iter_rows()},
        "judges_seen": int(cases.select(pl.col("judge").n_unique()).item()),
        "date_range": [
            cases.select(pl.col("diary_date").min()).item(),
            cases.select(pl.col("diary_date").max()).item(),
        ],
        "privacy_gate": "PASSED — no natural-person name survived anonymisation",
    }
    COVERAGE_PATH.write_text(json.dumps(coverage, indent=2, ensure_ascii=False), encoding="utf-8")
    logger.info(
        "GOLD written: %d docs -> %d anonymised cases (%d in-camera dropped), %d sittings; gate PASSED.",
        n_docs,
        cases.height,
        n_protected,
        schedule.height,
    )
    logger.info("By court: %s · Coverage -> %s", coverage["by_court"], COVERAGE_PATH)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse the Legal Diary OpenView HTML archive -> judiciary gold.")
    ap.add_argument("--jurisdictions", help=f"comma list; default all of {list(JURISDICTIONS)}")
    args = ap.parse_args()
    setup_standalone_logging("legal_diary_openview_extract")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
