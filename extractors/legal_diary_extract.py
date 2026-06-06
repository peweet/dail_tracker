"""legal_diary_extract.py — Courts Service daily Legal Diary -> judiciary gold facts.

Graduated from a sandbox parse spike into this re-runnable extractor.
Parses ONE day's diary .docx (zipfile -> XML state machine: Court -> Courtroom ->
Judge -> Time -> List -> case lines) into three privacy-tiered outputs.

PRIVACY MODEL (agreed 2026-06-05 — see memory project_judiciary_feature_validation):
  Tier A  schedule : judge sitting-sessions (court x courtroom x judge x list x time).
                     Names ONLY public officials in their public function -> safe in clear.
  Tier B  counts   : per-session item counts -> aggregate density, no party data.
  Tier C  cases    : individual cases, made publishable by:
      1. DROPPING statutory in-camera categories entirely (minors / family / wards /
         special care / childcare / asylum) — never parsed into the kept set, never linked;
      2. anonymising EVERY natural person to initials, keeping orgs / State bodies in clear;
      3. stripping all case references + solicitor annotations (quasi-identifiers);
      4. attaching a provenance link + source_sha256 so anyone can verify the primary record.

OUTPUTS
  GOLD (committed runtime set — anonymised / no-party-data only):
    data/gold/parquet/judicial_legal_diary_schedule.parquet   (Tier A)
    data/gold/parquet/judicial_legal_diary_counts.parquet     (Tier B)
    data/gold/parquet/judicial_legal_diary_cases.parquet      (Tier C, ANONYMISED)
  META:
    data/_meta/judicial_legal_diary_coverage.json
  SANDBOX (diagnostic only — keeps RAW un-anonymised text; NEVER promoted to gold):
    data/sandbox/parquet/judicial_legal_diary_audit.parquet

Each gold row carries diary_date so successive days ACCUMULATE (the source exposes the
current day only — history comes from the poller's archive, one file per day).

Run:
  ./.venv/Scripts/python.exe extractors/legal_diary_extract.py                 # latest archived docx
  ./.venv/Scripts/python.exe extractors/legal_diary_extract.py --file C:/tmp/diary.docx
  ./.venv/Scripts/python.exe extractors/legal_diary_extract.py --all-archived  # rebuild every archived day
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import html
import json
import logging
import re
import sys
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from config import BRONZE_DIR, DATA_DIR, GOLD_PARQUET_DIR  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402

logger = logging.getLogger(__name__)

PARSER_VERSION = "1.1.0"  # 1.1.0: plaintiff/defendant split + plaintiff_kind columns
SOURCE_NAME = "Courts Service Legal Diary"
SOURCE_URL = "https://legaldiary.courts.ie/"

ARCHIVE_DIR = BRONZE_DIR / "legal_diary"
SANDBOX_PARQUET_DIR = DATA_DIR / "sandbox" / "parquet"
META_DIR = DATA_DIR / "_meta"
COVERAGE_PATH = META_DIR / "judicial_legal_diary_coverage.json"

PARQUET_KW = {"compression": "zstd", "compression_level": 3, "statistics": True}


# ============================================================ docx -> lines
def read_docx_lines(path: Path) -> list[str]:
    xml = zipfile.ZipFile(path).read("word/document.xml").decode("utf-8", "ignore")
    out = []
    for para in re.split(r"</w:p>", xml):
        txt = "".join(re.findall(r"<w:t[^>]*>([^<]*)</w:t>", para))
        txt = html.unescape(txt).replace("’", "'").strip()
        if txt:
            out.append(txt)
    return out


_MONTHS = {m: i for i, m in enumerate(
    ["january", "february", "march", "april", "may", "june", "july", "august",
     "september", "october", "november", "december"], start=1)}
_DATE_RE = re.compile(
    r"(?:MON|TUES|WEDNES|THURS|FRI|SATUR|SUN)DAY\s+THE\s+(\d{1,2})(?:ST|ND|RD|TH)\s+"
    r"DAY\s+OF\s+([A-Z]+)\s+(\d{4})", re.I)


def diary_date_from_lines(lines: list[str]) -> str | None:
    """Parse 'THURSDAY THE 4TH DAY OF JUNE 2026' -> '2026-06-04'."""
    for ln in lines:
        m = _DATE_RE.search(ln)
        if m:
            day, month, year = int(m.group(1)), m.group(2).lower(), int(m.group(3))
            if month in _MONTHS:
                return f"{year:04d}-{_MONTHS[month]:02d}-{day:02d}"
    return None


# ============================================================ structural classifiers
COURTS = [
    ("SUPREME COURT", "Supreme Court"),
    ("COURT OF APPEAL (CRIMINAL)", "Court of Appeal (Criminal)"),
    ("COURT OF APPEAL", "Court of Appeal"),
    ("CENTRAL CRIMINAL COURT", "Central Criminal Court"),
    ("HIGH COURT", "High Court"),
    ("CIRCUIT COURT", "Circuit Court"),
    ("DISTRICT COURT", "District Court"),
]
JUDGE_RE = re.compile(r"^(MR|MS|MRS)\s+JUSTICE\s+[A-Z]", re.I)
PRES_RE = re.compile(r"^(THE PRESIDENT|THE CHIEF JUSTICE|HER HONOUR|HIS HONOUR|JUDGE)\b", re.I)
ROOM_RE = re.compile(r"^(IN COURT\b|COURT\s+\d)", re.I)
TIME_RE = re.compile(r"^AT\b.*(O'CLOCK|AM|PM|\d[:.]\d)", re.I)
STATUS_RE = re.compile(r"^(FOR MENTION|FOR HEARING|AT HEARING|FOR RULING|FOR JUDGMENT|"
                       r"FOR DIRECTIONS|FOR CALL ?OVER|NOT IN CUSTODY|IN CUSTODY|TO FIX)", re.I)
LIST_RE = re.compile(r"FAMILY|CHANCERY|COMMERCIAL|CRIMINAL|PERSONAL INJ|JUDICIAL REVIEW|PROBATE|"
                     r"BANKRUPT|EXAMIN|APPEAL|MENTION|WARDS|EDUCATION|ASYLUM|INSOLVENCY|"
                     r"COMPETITION|PLANNING|ADMIRALTY")
PARTY_RE = re.compile(r"\b-?\s*v\s*-?\b", re.I)


# ============================================================ privacy classifiers
# statutory in-camera / vulnerable -> DROPPED entirely (not anonymised, not linked)
PROTECTED_KEYS = [
    "minor", "child and family", "tusla", "care order", "wards of court", "ward of court",
    "special care", "special education", "family law", "in camera", "custody", "guardian",
    "adoption", "childcare", "asylum", "immigration", "citizenship",
]
# tokens marking a party as an ORGANISATION / STATE body -> kept in clear
ORG_KEYS = ["limited", " ltd", "d.a.c", " dac", " plc", "company", "bank", "insurance",
            "minister", "ireland", "attorney general", "commissioner", "council", "authority",
            "agency", "board", "revenue", " hse", "an garda", "designated activity",
            "university", "college", "credit union", "society", "fund", "holdings",
            "dpp", "director of public prosecutions", "people at the suit"]
PROSECUTOR_KEYS = ["dpp", "director of public prosecutions", "people at the suit"]
STATE_KEYS = ["minister", "attorney general", "ireland", "commissioner", "council",
              "authority", "revenue", " hse", "an garda", "state"]

# Plaintiff-side classifier — the "who is bringing the case" accountability signal.
# Deliberately NOT the greedy category_of sets: company markers are tested BEFORE
# geographic / institution words so a commercial litigant ("Bank of Ireland", "Mars
# Capital Finance Ireland DAC") classifies as an organisation, not mis-tagged a State
# body on the bare word "ireland".
PLAINTIFF_ORG_MARKERS = [
    " ltd", "limited", " dac", "d.a.c", " plc", "designated activity", "company",
    "bank", "finance", "insurance", "holdings", " fund", "capital", "mortgage",
    "credit union", "university", "college", "society", "ventures", "investments",
    "properties", " homes", "real estate", "airlines", "services", " group",
]
PLAINTIFF_STATE_MARKERS = [
    "minister", "attorney general", "commissioner", "revenue", " hse", "an garda",
    "the state", "county council", "city council", "local authority", " authority",
    " agency", " board", "office of", "mental health review",
]


def plaintiff_kind(raw_side: str) -> str:
    """Classify the FIRST (applicant / plaintiff / prosecutor) party, from the RAW
    segment so org / State names are intact. Priority prosecutor > organisation >
    State body > individual; company markers beat geographic words (see above)."""
    s = raw_side.lower()
    if any(k in s for k in PROSECUTOR_KEYS):
        return "state-prosecutor"
    if any(k in s for k in PLAINTIFF_ORG_MARKERS):
        return "organisation"
    if any(k in s for k in PLAINTIFF_STATE_MARKERS):
        return "state-body"
    return "individual"


def protected_reason(list_type: str, case: str) -> str | None:
    blob = f"{list_type} || {case}".lower()
    return next((k for k in PROTECTED_KEYS if k in blob), None)


def category_of(list_type: str, case: str) -> str:
    blob = f"{list_type} || {case}".lower()
    if any(k in blob for k in PROSECUTOR_KEYS):
        return "criminal"
    if any(k in blob for k in STATE_KEYS):
        return "public-law"
    if any(k in blob for k in ORG_KEYS):
        return "commercial"
    return "civil"


# ============================================================ anonymisation
def strip_refs(t: str) -> str:
    # High Court record refs: generalise H.JR / H.P to the whole H.<list>.<year>.<seq>
    # family (H.COS commercial, H.CA, …); the trailing seq can glue to the next word
    # (H.COS.2025.0000177SOLFRENO), so eat optional trailing digits too.
    t = re.sub(r"\bH\.?[A-Z]{1,4}\.?\s*\d{4}\.?\d*", " ", t, flags=re.I)
    t = re.sub(r"\([^)]*\)", " ", t)                         # solicitor / duration parens
    t = re.sub(r":[A-Z]{2,}:[A-Z0-9:]+", " ", t)             # :LCA:OLCA:2026:000144
    t = re.sub(r"\b\d*\s*CCDP\d+/\d+", " ", t, flags=re.I)
    t = re.sub(r"\b\d*\s*CJA/\d+", " ", t, flags=re.I)
    t = re.sub(r"\bPI\s*\d+", " ", t, flags=re.I)
    t = re.sub(r"\b\d{4}\s+\d+\s+[A-Z]\b", " ", t)           # 2022 3507 P
    t = re.sub(r"\b\d+\s*/\s*\d+\b", " ", t)                 # 260/22, 174/25
    t = re.sub(r"^\s*\d+\.\s+", " ", t)                      # leading list index "20. "
    t = re.sub(r"^\s*\d{4}\s+\d+\s+", " ", t)                # leading "2026 134 " ref
    t = re.sub(r"\d{3,}", " ", t)                            # long digit run / glued ref
    t = re.sub(r"^\s*\d{1,3}\s+(?=[A-Za-z])", " ", t)        # short leading index "82 Filbeck"
    t = re.sub(r"^[A-Z]{1,4}\d[\w./:]*", " ", t)             # leading glued alnum ref
    # leading court list-section codes, space-delimited only (SP / COS / COM / CHY) —
    # glued variants ("SPPROMONTORIA" vs "SPROMONTORIA") carry inconsistent prefix
    # lengths, so stripping them blind truncates the name; left intact instead.
    t = re.sub(r"^\s*(?:SP|COS|COM|CHY)\s+(?=[A-Z])", " ", t)
    t = re.sub(r"^\s*-?\s*v\s*-\s*", " ", t, flags=re.I)     # dangling leading "v-" separator
    return re.sub(r"\s+", " ", t).strip(" -:.,")


_SKIP = {"the", "and", "of", "for", "mr", "mrs", "ms", "dr", "an", "na", "orse",
         "through", "nf", "trading", "as", "formerly", "also", "known", "minor", "a"}
# trailing "& Ors" / "and Others" / "& Anor" / "and Another" -> collapsed to "& Ors"
_TAIL = re.compile(r"(?:&|\band)\s*(ors|anor|another|others)\b", re.I)
# EVERY "v" / "-v-" party separator (NOT maxsplit=1 — consolidated listings chain them)
_VSPLIT = re.compile(r"\s*-?\s*\bv\b\.?\s*-?\s*", re.I)
# and-conjunction between co-parties on one side (so a public body riding alongside
# named individuals cannot keep them in clear); also catches stray "&" inside a side
_ANDSPLIT = re.compile(r"\s*(?:\band\b|&)\s*", re.I)


def _initials(side: str) -> str:
    tail = " & Ors" if _TAIL.search(side) else ""
    core = _TAIL.sub("", side)
    core = re.split(r"\b(through|orse|trading as|t/a|formerly|also known as|aka)\b",
                    core, flags=re.I)[0]
    toks = [w for w in re.findall(r"[A-Za-z']+", core) if w.lower() not in _SKIP]
    ini = ".".join(w[0].upper() for w in toks[:4])
    return (ini + "." if ini else "X") + tail


def _is_org(side: str) -> bool:
    return any(k in side.lower() for k in ORG_KEYS)


def _anonymise_party(side: str) -> str:
    """Anonymise ONE party side. Split into and-chunks so a public body riding
    alongside named individuals (e.g. '<Name> and <Name> and X County Council')
    cannot keep the individuals in clear: each non-org chunk is reduced to initials,
    org / State chunks are kept verbatim. A trailing '& Ors' / 'and others' is
    preserved. Over-anonymising an org name (no keyword) is harmless; the only
    unsafe direction is a natural person kept in clear, which this prevents."""
    side = side.strip()
    if not side:
        return ""
    tail = " & Ors" if _TAIL.search(side) else ""
    core = _TAIL.sub("", side)
    rendered = [c.strip() if _is_org(c.strip()) else _initials(c.strip())
                for c in _ANDSPLIT.split(core) if c.strip()]
    joined = " and ".join(r for r in rendered if r)
    return (joined + tail) if joined else ("X" + tail)


def parties(raw: str) -> dict[str, str]:
    """Reduce a raw case line to publishable, anonymised parties. Splits on EVERY 'v'
    separator (consolidated matters chain them) and anonymises each segment side by
    side. Returns case_anonymised (all segments joined), plaintiff (first segment),
    defendant (the rest), and plaintiff_kind (classified from the RAW first segment).
    plaintiff / defendant are the SAME anonymised material as case_anonymised, so the
    privacy gate over case_anonymised covers them. Natural persons -> initials;
    organisations / State bodies kept in clear."""
    t = strip_refs(raw)
    raw_segs = [s for s in _VSPLIT.split(t) if s.strip()] if t else []
    anon_segs = [_anonymise_party(s) for s in raw_segs]
    return {
        "case_anonymised": " v ".join(anon_segs),
        "plaintiff": anon_segs[0] if anon_segs else "",
        "defendant": " v ".join(anon_segs[1:]) if len(anon_segs) > 1 else "",
        "plaintiff_kind": plaintiff_kind(raw_segs[0]) if raw_segs else "",
    }


def anonymise(raw: str) -> str:
    """Backward-compatible scalar form: just the joined anonymised case title."""
    return parties(raw)["case_anonymised"]


class PrivacyInvariantError(RuntimeError):
    """Raised when the anonymised gold cases set would expose a natural person.
    A hard, ``-O``-proof gate (never an ``assert``): the writer refuses to emit gold."""


_NAME_TOKEN = re.compile(r"[A-Za-z]{2,}")


def residual_name_tokens(case_anonymised: str) -> list[str]:
    """Natural-person name tokens that survived anonymisation. Every non-org party
    chunk must be pure initials (single letters); any 2+-letter word in such a chunk
    is a leaked name. Org / State chunks (legitimately kept in clear) are exempt.
    This is the CONTENT privacy invariant the old column-name assert never checked."""
    leaks: list[str] = []
    for seg in re.split(r"\s+v\s+", case_anonymised):
        core = _TAIL.sub("", seg)
        for chunk in _ANDSPLIT.split(core):
            chunk = chunk.strip()
            if not chunk or _is_org(chunk):
                continue
            leaks.extend(_NAME_TOKEN.findall(chunk))
    return leaks


# ============================================================ parse one day
def parse_day(lines: list[str], diary_date: str):
    court = room = judge = list_type = time_s = status = None
    schedule: dict = {}
    cases: list = []

    def key():
        return (court, room, judge, list_type, time_s)

    for ln in lines:
        hit = next((full for kw, full in COURTS
                    if ln.isupper() and ln.upper().strip().endswith(kw)), None)
        if hit:
            court, judge, list_type, time_s, status = hit, None, None, None, None
            continue
        if ROOM_RE.match(ln) and ln.isupper():
            room, judge = ln.title(), None
            continue
        if JUDGE_RE.match(ln) or (PRES_RE.match(ln) and ln.isupper()):
            judge = re.sub(r"\s+", " ", ln.title()).strip()
            list_type = time_s = status = None
            continue
        if TIME_RE.match(ln):
            time_s = ln.title()
            continue
        if STATUS_RE.match(ln):
            status = ln.title()
            continue
        if ln.isupper() and ("LIST" in ln or LIST_RE.search(ln)):
            list_type = ln.title()
            if judge:
                schedule.setdefault(key(), {
                    "diary_date": diary_date, "court": court, "courtroom": room,
                    "judge": judge, "list_type": list_type, "time": time_s, "n_items": 0})
            continue
        if PARTY_RE.search(f" {ln} ") or ln.upper().startswith("IN THE MATTER"):
            prot = protected_reason(list_type or "", ln)
            cases.append({
                "diary_date": diary_date, "court": court, "courtroom": room, "judge": judge,
                "list_type": list_type, "time": time_s, "status": status, "raw_case": ln,
                "category": category_of(list_type or "", ln),
                "protected": bool(prot), "protected_reason": prot})
            if judge:
                s = schedule.setdefault(key(), {
                    "diary_date": diary_date, "court": court, "courtroom": room,
                    "judge": judge, "list_type": list_type, "time": time_s, "n_items": 0})
                s["n_items"] += 1
    return list(schedule.values()), cases


# ============================================================ orchestration
def _resolve_inputs(args) -> list[Path]:
    """Default = EVERY archived day (gold is a forward-accumulating fact rebuilt
    from the full archive, since the source exposes only the current day). --file
    parses a single docx (one-off / first run before any archive exists)."""
    if args.file:
        return [Path(args.file)]
    archived = sorted(ARCHIVE_DIR.glob("*.docx")) if ARCHIVE_DIR.exists() else []
    if archived:
        return archived
    fallback = Path("C:/tmp/diary.docx")
    if fallback.exists():
        logger.info("No archive present; using cached %s", fallback)
        return [fallback]
    return []


def run(args) -> int:
    for d in (GOLD_PARQUET_DIR, SANDBOX_PARQUET_DIR, META_DIR):
        d.mkdir(parents=True, exist_ok=True)

    inputs = _resolve_inputs(args)
    if not inputs:
        logger.error("No diary .docx to parse (no --file, no archive, no cache). Aborting.")
        return 1

    all_sched, all_cases, days = [], [], []
    for path in inputs:
        if not path.exists():
            logger.error("File not found: %s", path)
            return 1
        sha = hashlib.sha256(path.read_bytes()).hexdigest()
        lines = read_docx_lines(path)
        ddate = args.date or diary_date_from_lines(lines)
        if not ddate:
            logger.error("Could not determine diary date for %s (no header match, no --date).", path)
            return 2
        sched, cases = parse_day(lines, ddate)
        for c in cases:
            c["source_sha256"] = sha[:16]
        all_sched += sched
        all_cases += cases
        days.append({"file": path.name, "diary_date": ddate, "sha256": sha[:16],
                     "sessions": len(sched), "cases": len(cases)})
        logger.info("Parsed %s (%s): %d sessions, %d case lines", path.name, ddate,
                    len(sched), len(cases))

    if not all_cases and not all_sched:
        logger.error("Parsed 0 sessions and 0 cases across %d file(s) — likely HTML/format "
                     "drift. NOT writing gold (would clobber prior days).", len(inputs))
        return 2

    # ---- Tier A: schedule (officials only) ----
    sched_df = (pl.DataFrame(all_sched).filter(pl.col("judge").is_not_null()).unique()
                .sort(["diary_date", "court", "courtroom", "judge"]))
    sched_df.write_parquet(GOLD_PARQUET_DIR / "judicial_legal_diary_schedule.parquet", **PARQUET_KW)

    # ---- Tier B: counts ----
    counts_df = (sched_df.select(["diary_date", "court", "judge", "list_type", "n_items"])
                 .filter(pl.col("n_items") > 0).sort(["diary_date", "n_items"], descending=[False, True]))
    counts_df.write_parquet(GOLD_PARQUET_DIR / "judicial_legal_diary_counts.parquet", **PARQUET_KW)

    # ---- audit (RAW, sandbox only) ----
    audit_df = pl.DataFrame(all_cases)
    audit_df.write_parquet(SANDBOX_PARQUET_DIR / "judicial_legal_diary_audit.parquet", **PARQUET_KW)

    # ---- Tier C: anonymised cases (GOLD) ----
    # parties() returns the joined title PLUS the split plaintiff / defendant / kind,
    # all from the same anonymised segments — unnest into columns.
    _parties_dtype = pl.Struct([
        pl.Field("case_anonymised", pl.Utf8), pl.Field("plaintiff", pl.Utf8),
        pl.Field("defendant", pl.Utf8), pl.Field("plaintiff_kind", pl.Utf8)])
    cases_df = (audit_df.filter(~pl.col("protected"))
                .with_columns(pl.col("raw_case").map_elements(parties, return_dtype=_parties_dtype)
                              .alias("_parties"))
                .unnest("_parties")
                .filter(pl.col("case_anonymised").str.len_chars() > 2)
                .with_columns([pl.lit(SOURCE_NAME).alias("source"),
                               pl.lit(SOURCE_URL).alias("source_url")])
                .select(["diary_date", "court", "judge", "list_type", "status", "category",
                         "case_anonymised", "plaintiff", "defendant", "plaintiff_kind",
                         "source", "source_url", "source_sha256"])
                .sort(["diary_date", "court", "judge"]))

    # ---- PRIVACY GATE (runtime, -O-proof; runs BEFORE any gold is written) ----
    # 1. structural: no raw-name column may reach the published set.
    forbidden = {"raw_case", "party", "parties", "solicitor", "solicitors"} & set(cases_df.columns)
    if forbidden:
        raise PrivacyInvariantError(f"raw-name column(s) leaked into gold cases: {sorted(forbidden)}")
    # 2. content: no anonymised text column may retain a natural-person name. Checks
    #    case_anonymised AND the derived plaintiff / defendant split (the split is built
    #    from the same anonymised segments, but we gate each column explicitly so a
    #    future change to the split can never quietly leak). See anonymise() bugs fixed
    #    2026-06-05 (multi-`v` split + whole-side org classification).
    _name_cols = ["case_anonymised", "plaintiff", "defendant"]
    offenders = [(f"{col}={r[col]!r}", toks)
                 for r in cases_df.select(_name_cols).iter_rows(named=True)
                 for col in _name_cols
                 if (toks := residual_name_tokens(r[col] or ""))]
    if offenders:
        sample = " | ".join(f"{c}->{t}" for c, t in offenders[:5])
        raise PrivacyInvariantError(
            f"{len(offenders)} gold case text-cells retain natural-person names "
            f"after anonymisation; refusing to write gold. e.g. {sample}")

    cases_df.write_parquet(GOLD_PARQUET_DIR / "judicial_legal_diary_cases.parquet", **PARQUET_KW)

    n_protected = audit_df.filter(pl.col("protected")).height
    drop_reasons = {r[0]: r[1] for r in (audit_df.filter(pl.col("protected"))
                    .group_by("protected_reason").len().sort("len", descending=True).iter_rows())}
    plaintiff_kinds = {r[0]: r[1] for r in (cases_df.group_by("plaintiff_kind").len()
                       .sort("len", descending=True).iter_rows())}
    coverage = {
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(),
        "source_name": SOURCE_NAME,
        "source_url": SOURCE_URL,
        "days": days,
        "n_days": len(days),
        "tier_a_sessions": sched_df.height,
        "tier_b_counts": counts_df.height,
        "case_lines_parsed": audit_df.height,
        "cases_dropped_protected": n_protected,
        "cases_kept_anonymised": cases_df.height,
        "drop_reasons": drop_reasons,
        "plaintiff_kinds": plaintiff_kinds,
        "privacy_note": ("Tier C drops statutory in-camera categories entirely and reduces every "
                         "natural person to initials; organisations/State bodies kept in clear; "
                         "case refs stripped; provenance link + source_sha256 attached."),
    }
    COVERAGE_PATH.write_text(json.dumps(coverage, indent=2, ensure_ascii=False), encoding="utf-8")

    logger.info("GOLD written: %d sessions, %d counts, %d anonymised cases (%d dropped in-camera).",
                sched_df.height, counts_df.height, cases_df.height, n_protected)
    logger.info("Coverage -> %s", COVERAGE_PATH)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Parse Courts Service Legal Diary -> judiciary gold facts.")
    ap.add_argument("--file", help="parse a single .docx (default: rebuild from EVERY archived day)")
    ap.add_argument("--date", help="override diary date (YYYY-MM-DD) if the header can't be parsed")
    args = ap.parse_args()
    setup_standalone_logging("legal_diary_extract")
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
