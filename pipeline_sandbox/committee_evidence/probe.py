"""Committee-evidence feasibility probe (Phase 0 — sandbox only, no promotion).

WHY THIS EXISTS
---------------
A question about the CCMA's *Housing, Building and Land Use Committee* surfaced a
gap: the CCMA (the 31 council chief executives' association) is invisible in our
data — not on the lobbying register, not a payee — yet it shapes housing policy by
giving evidence to the **Oireachtas Joint Committee on Housing, Local Government and
Heritage**. Every body that appears before an Oireachtas committee leaves the same
public, born-digital trail. The potential feature is a "who gives evidence to
Oireachtas committees, and do they take public money" layer (sibling to the
ministerial-diaries / Who-Ministers-Meet feature).

Before building any of that stack this probe answers ONE question:
    of the organisations that give evidence to PAC + the Housing committee,
    what fraction cross-reference to data we already hold?
A high match-rate => a real feature. A low one => mostly attendance lists.

WHAT IT DOES (and deliberately does NOT)
----------------------------------------
- Enumerates committee MEETINGS via the official API (/v1/debates?chamber_type=committee)
  — fully supported, authoritative. There is NO committee-submissions API endpoint and
  the data.oireachtas.ie S3 directory listing is denied, so submission PDFs are NOT
  enumerable; we therefore take the witness-ORG signal from the TRANSCRIPT itself
  (section headings + the Cathaoirleach's opening welcome), which IS API-enumerable.
- Cross-references each witness org against our gold parquet via the SHARED house
  name-norm key (shared.name_norm.name_norm_expr) — exact-normalised match only, to
  keep false positives down.
- Writes a coverage report + a sandbox parquet. NO gold writes, NO views, NO pipeline
  registration. Everything lives under pipeline_sandbox/committee_evidence/.

CORRECTNESS (guard against committee conflation)
------------------------------------------------
Two *different* bodies must never be equated: the CCMA's internal committee is NOT a
Dáil committee. We only ever record "org X gave evidence to Oireachtas committee Y on
date Z". Committee identity is taken from the API (house.committeeCode / showAs) and
reconciled against the AKN-XML FRBR path — never inferred. Org cross-references are
emitted as human-eyeballable example rows, never just an aggregate %.
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

# Repo root on sys.path so `python pipeline_sandbox/committee_evidence/probe.py` finds services/ shared/.
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import polars as pl  # noqa: E402

from services.http_engine import fetch_json, fetch_text  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402
from shared.name_norm import name_norm_expr  # noqa: E402

logger = logging.getLogger("committee_evidence_probe")

API = "https://api.oireachtas.ie/v1"
OUT_DIR = Path(__file__).resolve().parent

# The two probe committees, keyed by the API's authoritative committeeCode.
COMMITTEES = {
    "pac": "committee_of_public_accounts",
    "housing": "joint_committee_on_housing_local_government_and_heritage",
}

# Honorifics that mark a NON-member witness in a transcript <from> label. TDs/Senators
# read "Deputy X" / "Senator X"; ministers "Minister for ..."; the chair "An Cathaoirleach".
_WITNESS_HONORIFIC = re.compile(r"^\s*(Mr\.?|Ms\.?|Mrs\.?|Dr\.?|Professor|Prof\.?|Cllr\.?)\s", re.I)
_MEMBER_PREFIX = re.compile(r"^\s*(Deputy|Senator|An Cathaoirleach|An Leas-Chathaoirleach|Minister)\b", re.I)

# Heading suffixes that describe the FORMAT of the session, not the witness body. Strip
# them so "Strategic Priorities of the Housing Agency: Engagement with Chairperson" and
# "Housing Activation Office: Discussion" reduce toward the org/topic.
_HEADING_FORMAT_TAIL = re.compile(
    r":\s*(Discussion|Engagement.*|Statements|Motion.*|Committee Stage.*|"
    r"Pre-Legislative Scrutiny.*|Examination.*|Briefing.*|Resumed.*|Meeting.*)$",
    re.I,
)

# Leading topic phrasing that wraps the actual body name ("Operations of Tailte Éireann"
# -> "Tailte Éireann", "Strategic Priorities of the Housing Agency" -> "Housing Agency").
_HEADING_TOPIC_PREFIX = re.compile(
    r"^(?:Strategic Priorities of|Operations of|Delivery of|Overview of|Update on|"
    r"Examination of|Engagement with|Discussion on|Briefing on|Report on the Accounts of|"
    r"Situation in|Meeting the [A-Za-z ]+ of)\s+(?:the\s+)?",
    re.I,
)
# Chapter / accounting framing stripped from PAC headings.
_HEADING_CHAPTER_PREFIX = re.compile(r"^Chapter\s+\d+\s*[-–—:]\s*", re.I)
_HEADING_ACCOUNTING_TAIL = re.compile(
    r"\s*[-–—:]\s*(?:Financial Statements|Appropriation Accounts|Annual Report)\b.*$", re.I)
# Headings that are pure topics/accounting artefacts with no body to extract — dropped.
_HEADING_DROP = re.compile(
    r"^(?:Appropriation Accounts|Financial Statements|Report on the Accounts|"
    r"Vote Accounting|Superannuation|Public Expenditure,|Chapter\s+\d+\b.*(?:Fund|Revenues|Neutrality|"
    r"Management|Clearing|Receipts))",
    re.I,
)

# A heading/welcome phrase that names an actual public body (vs a pure topic). Used only to
# FLAG unmatched rows as "real body we don't hold" vs likely extraction noise — not to match.
_PUBLIC_BODY_HINT = re.compile(
    r"\b(Department|Agency|Authority|Commission|Council|Office|Board|Bord|Údarás|Udaras|"
    r"Coimisiún|Coimisiun|Éireann|Eireann|Ireland|Service|Executive|Tusla|HSE|Garda)\b", re.I)

# Opening-welcome phrasings the Cathaoirleach uses to introduce witnesses' bodies.
# Captures the org phrase that follows. Heuristic — corroboration, not the primary signal.
_WELCOME_PATTERNS = [
    re.compile(r"representatives? (?:of|from) (?:the )?([A-Z][^.:;,]{4,80})", ),
    re.compile(r"officials? (?:of|from) (?:the )?([A-Z][^.:;,]{4,80})"),
    re.compile(r"witnesses? from (?:the )?([A-Z][^.:;,]{4,80})"),
    re.compile(r"I welcome[^.]*?from (?:the )?([A-Z][^.:;,]{4,80})"),
]

_TAG = re.compile(r"<[^>]+>")
_FROM = re.compile(r"<from[^>]*>(.*?)</from>", re.S)
_HEADING = re.compile(r"<heading[^>]*>(.*?)</heading>", re.S)
_SPEECH = re.compile(r"<speech[^>]*>(.*?)</speech>", re.S)
_FRBRTHIS = re.compile(r'<FRBRthis[^>]*value="([^"]+)"')


def _plain(s: str) -> str:
    return re.sub(r"\s+", " ", _TAG.sub("", s)).strip()


# --------------------------------------------------------------------------- enumerate


def enumerate_meetings(code: str, max_meetings: int, since: str) -> list[dict]:
    """All committee meeting records for one committeeCode, newest first, via /v1/debates.

    Returns the authoritative API record (committee identity + AKN-XML uri). Stops at
    max_meetings or when records predate `since`. No silent truncation: logs the cap hit.
    """
    out: list[dict] = []
    skip = 0
    page = 500
    while len(out) < max_meetings:
        url = f"{API}/debates?chamber_type=committee&limit={page}&skip={skip}"
        data, _ = fetch_json(url)
        rows = data.get("results", [])
        if not rows:
            break
        for r in rows:
            rec = r.get("debateRecord", {})
            house = rec.get("house", {})
            if house.get("committeeCode") != code:
                continue
            date = rec.get("date", "")
            if date < since:
                continue
            if not rec.get("formats", {}).get("xml"):
                continue
            out.append(rec)
            if len(out) >= max_meetings:
                logger.warning("hit --max-meetings cap (%d) for %s; older meetings NOT scanned", max_meetings, code)
                return out
        # crude early-stop: the API returns newest-first; once a full page is all older
        # than `since`, no point paging further.
        page_dates = [r.get("debateRecord", {}).get("date", "") for r in rows]
        if page_dates and max(page_dates) < since:
            break
        skip += page
    return out


# ----------------------------------------------------------------------------- extract


def extract_witness_orgs(rec: dict) -> dict:
    """Pull witness-org candidates + a committee-identity reconciliation from one meeting.

    Org signal, in priority order:
      1. section <heading> text (format-tail stripped) — the meeting topic/body
      2. the opening welcome speech ("representatives of <ORG>") — corroboration
    Also lists non-member witness PERSON labels (Mr./Ms/Dr. ...) for the human reviewer.
    """
    house = rec.get("house", {})
    xml, _ = fetch_text(rec["formats"]["xml"]["uri"])

    # Committee-identity reconciliation: API code vs AKN FRBR path. Flag, never guess.
    frbr = _FRBRTHIS.search(xml)
    frbr_path = frbr.group(1) if frbr else ""
    path_code = ""
    m = re.search(r"/debateRecord/([^/]+)/", frbr_path)
    if m:
        path_code = m.group(1)
    code_match = (path_code == house.get("committeeCode"))

    headings = [_plain(h) for h in _HEADING.findall(xml)]
    org_candidates: list[tuple[str, str]] = []  # (org_text, source)
    for h in headings:
        if not h or h.lower().startswith("business of") or _HEADING_DROP.search(h):
            continue
        org = _HEADING_FORMAT_TAIL.sub("", h).strip()
        # PAC headings like "Vote 45 - Further and Higher Education ..." -> take the dept side
        vm = re.match(r"Vote\s+\d+\s*[-–—]\s*(.+)", org)
        if vm:
            org = vm.group(1).strip()
        # "Financial Statements 2024: National Training Fund" -> take the body after colon
        fm = re.match(r"(?:Financial Statements|Appropriation Accounts)[^:]*:\s*(.+)", org, re.I)
        if fm:
            org = fm.group(1).strip()
        # "<Body> – Financial Statements 2024" -> take the body; "Chapter 4 - ..." prefix gone
        org = _HEADING_CHAPTER_PREFIX.sub("", org)
        org = _HEADING_ACCOUNTING_TAIL.sub("", org).strip()
        # "Operations of Tailte Éireann" / "Strategic Priorities of the Housing Agency" -> body
        org = _HEADING_TOPIC_PREFIX.sub("", org).strip()
        if 3 < len(org) < 90:
            org_candidates.append((org, "heading"))

    # opening welcome (first 2 speeches) for corroborating org mentions
    speeches = _SPEECH.findall(xml)
    welcome = _plain(" ".join(speeches[:2])) if speeches else ""
    for pat in _WELCOME_PATTERNS:
        for hit in pat.findall(welcome):
            org = re.sub(r"\s+(?:Mr|Ms|Mrs|Dr)\b.*$", "", hit).strip()  # cut where the name list starts
            if 3 < len(org) < 90:
                org_candidates.append((org, "welcome"))

    # witness persons (non-member speakers) — context for the reviewer, not matched
    froms = [_plain(f) for f in _FROM.findall(xml)]
    witnesses = sorted({f for f in froms if _WITNESS_HONORIFIC.match(f) and not _MEMBER_PREFIX.match(f)})

    # dedupe org candidates, prefer heading source
    seen: dict[str, str] = {}
    for org, src in org_candidates:
        key = org.lower()
        if key not in seen or (seen[key] == "welcome" and src == "heading"):
            seen[key] = src
    orgs = [{"witness_org": o, "org_source": s} for o, s in ((c, seen[c.lower()]) for c in {x[0] for x in org_candidates})]

    return {
        "committee_code": house.get("committeeCode"),
        "committee_name": house.get("showAs"),
        "house_no": house.get("houseNo"),
        "date": rec.get("date"),
        "source_xml": rec["formats"]["xml"]["uri"],
        "frbr_path_code": path_code,
        "committee_code_reconciled": code_match,
        "headings": headings,
        "witness_persons": witnesses,
        "orgs": orgs,
    }


# --------------------------------------------------------------------------- cross-ref


def _norm_set(df: pl.DataFrame, col: str) -> set[str]:
    """Distinct normalised names from a frame column (already-normalised cols pass through)."""
    if col.endswith(("_normalised", "_norm")):
        return set(df.select(pl.col(col).alias("k")).drop_nulls()["k"].to_list())
    return set(df.select(name_norm_expr(col).alias("k")).drop_nulls()["k"].to_list())


# Datasets small + authoritatively long-named enough to allow a token-subset ("likely")
# match in addition to exact: a department/council's official name often differs in wording
# from how a witness body is introduced ("Department of Housing" vs the publisher's
# "Department of Housing, Local Government and Heritage"). Large commercial sets
# (payee/awards/lobbying) stay EXACT-only — token-subset there is both costly and FP-prone.
_TOKEN_TIER_SETS = {"payments_publisher", "councils"}


def load_crossref() -> dict:
    """Build normalised name-sets for each dataset a witness org could match."""
    gold = _REPO_ROOT / "data" / "gold" / "parquet"
    meta = _REPO_ROOT / "data" / "_meta"
    ds: dict[str, set[str]] = {}

    pay = pl.read_parquet(gold / "procurement_payments_fact.parquet")
    ds["payments_payee"] = _norm_set(pay, "supplier_normalised")
    ds["payments_publisher"] = _norm_set(pay, "publisher_name")  # PAC: dept = the publisher/payer

    aw = pl.read_parquet(gold / "procurement_awards.parquet")
    ds["procurement_awards"] = _norm_set(aw, "supplier_norm")

    lob = pl.read_parquet(gold / "top_lobbyist_organisations.parquet")
    ds["lobbying_register"] = _norm_set(lob, "lobbyist_name")

    ce = pl.read_csv(meta / "la_chief_executives.csv")
    ds["councils"] = _norm_set(ce, "council_name")
    return ds


def _token_subset(org_key: str, names: set[str]) -> bool:
    """True if org_key and some dataset name are token-subset-related, with guards against
    trivial matches: the smaller side must have >=2 tokens AND a distinctive token (>=5 chars)."""
    ot = set(org_key.split())
    if len(ot) < 2:
        return False
    for n in names:
        nt = set(n.split())
        if len(nt) < 2:
            continue
        small = ot if len(ot) <= len(nt) else nt
        if (ot <= nt or nt <= ot) and any(len(t) >= 5 for t in small):
            return True
    return False


def crossref_org(org: str, ds: dict) -> list[tuple[str, str]]:
    """(dataset, tier) matches for this org. tier='exact' for normalised-equality (any set);
    tier='likely' for token-subset (small authoritative sets only). Exact preferred per set."""
    key = pl.DataFrame({"n": [org]}).select(name_norm_expr("n"))["n"][0]
    if not key or len(key) < 5:  # too-short keys are extraction noise
        return []
    out: list[tuple[str, str]] = []
    for name, names in ds.items():
        if key in names:
            out.append((name, "exact"))
        elif name in _TOKEN_TIER_SETS and _token_subset(key, names):
            out.append((name, "likely"))
    return out


# ------------------------------------------------------------------------------- main


def run(committees: list[str], max_meetings: int, since: str) -> None:
    ds = load_crossref()
    logger.info("cross-ref sets: %s", {k: len(v) for k, v in ds.items()})

    rows: list[dict] = []
    recon_flags = 0
    for short in committees:
        code = COMMITTEES[short]
        meetings = enumerate_meetings(code, max_meetings, since)
        logger.info("%s (%s): %d meetings since %s", short, code, len(meetings), since)
        for rec in meetings:
            ev = extract_witness_orgs(rec)
            if not ev["committee_code_reconciled"]:
                recon_flags += 1
                logger.warning("committee code MISMATCH api=%s frbr=%s date=%s",
                               ev["committee_code"], ev["frbr_path_code"], ev["date"])
            for o in ev["orgs"]:
                matches = crossref_org(o["witness_org"], ds)
                exact = [d for d, t in matches if t == "exact"]
                likely = [d for d, t in matches if t == "likely"]
                rows.append({
                    "committee": short,
                    "committee_name": ev["committee_name"],
                    "date": ev["date"],
                    "witness_org": o["witness_org"],
                    "org_source": o["org_source"],
                    "matched_exact": ",".join(exact),
                    "matched_likely": ",".join(likely),
                    "n_matched": len(matches),
                    "is_public_body": bool(_PUBLIC_BODY_HINT.search(o["witness_org"])),
                    "source_xml": ev["source_xml"],
                })

    if not rows:
        logger.error("no witness orgs extracted — check enumeration / extraction")
        return
    df = pl.DataFrame(rows)
    save_parquet(df, OUT_DIR / "committee_witness_crossref.parquet")
    _write_coverage(df, recon_flags, since)


def _write_coverage(df: pl.DataFrame, recon_flags: int, since: str) -> None:
    lines: list[str] = []
    lines.append("# Committee-Evidence Probe — Coverage Report\n")
    lines.append(f"_Sandbox feasibility probe (Phase 0). Meetings since {since}. "
                 "Witness-org signal = transcript headings + opening welcome. "
                 "Match = exact normalised name (shared `name_norm_expr`) in our gold data._\n")
    lines.append(f"\n**Committee-identity reconciliation mismatches (API vs AKN path):** {recon_flags} "
                 "(should be 0 — any >0 is a conflation risk to inspect).\n")

    # headline per committee: exact, +likely, and the "real public body" ceiling
    lines.append("\n## Headline: witness orgs that cross-reference to our data\n")
    lines.append("_`exact` = normalised-name equality. `+likely` adds token-subset matches on the "
                 "small authoritative sets (departments/councils). `public-body ceiling` = share of "
                 "witness orgs that are clearly real state bodies (matched or not) — the realistic "
                 "upper bound once an org-identity alias map lands in Phase 1._\n")
    lines.append("| Committee | Witness orgs | Exact | Exact rate | +Likely | +Likely rate | Public-body ceiling |")
    lines.append("|---|---|---|---|---|---|---|")
    for c in df["committee"].unique().to_list():
        sub = df.filter(pl.col("committee") == c)
        distinct = sub.select(pl.col("witness_org").str.to_lowercase()).unique().height
        exact = sub.filter(pl.col("matched_exact") != "").select(pl.col("witness_org").str.to_lowercase()).unique().height
        anym = sub.filter(pl.col("n_matched") > 0).select(pl.col("witness_org").str.to_lowercase()).unique().height
        body = sub.filter(pl.col("is_public_body")).select(pl.col("witness_org").str.to_lowercase()).unique().height
        er = f"{(exact / distinct * 100):.0f}%" if distinct else "—"
        ar = f"{(anym / distinct * 100):.0f}%" if distinct else "—"
        cr = f"{(body / distinct * 100):.0f}%" if distinct else "—"
        lines.append(f"| {c} | {distinct} | {exact} | {er} | {anym} | {ar} | {cr} |")

    # per-dataset breakdown (exact + likely combined, tier shown)
    lines.append("\n## Which datasets the matches land in\n")
    rec = []
    for r in df.filter(pl.col("n_matched") > 0).iter_rows(named=True):
        for d in filter(None, r["matched_exact"].split(",")):
            rec.append((d, "exact"))
        for d in filter(None, r["matched_likely"].split(",")):
            rec.append((d, "likely"))
    if rec:
        bd = pl.DataFrame(rec, schema=["dataset", "tier"], orient="row").group_by(["dataset", "tier"]).agg(
            pl.len().alias("n")).sort("n", descending=True)
        lines.append("| Dataset | Tier | Witness-org matches |")
        lines.append("|---|---|---|")
        for r in bd.iter_rows(named=True):
            lines.append(f"| {r['dataset']} | {r['tier']} | {r['n']} |")

    # worked examples — human must eyeball these
    lines.append("\n## Worked examples (HUMAN-VERIFY before trusting any number)\n")
    ex = df.filter(pl.col("n_matched") > 0).sort(["committee", "date"], descending=[False, True]).head(45)
    lines.append("| Committee | Date | Witness org | Source | Exact | Likely |")
    lines.append("|---|---|---|---|---|---|")
    for r in ex.iter_rows(named=True):
        lines.append(f"| {r['committee']} | {r['date']} | {r['witness_org']} | {r['org_source']} | "
                     f"{r['matched_exact']} | {r['matched_likely']} |")

    # unmatched public bodies — these are the Phase-1 alias-map work-list (real bodies we hold but miss)
    lines.append("\n## Unmatched but clearly a public body (Phase-1 alias-map work-list)\n")
    unb = df.filter((pl.col("n_matched") == 0) & pl.col("is_public_body")).select(
        ["committee", "date", "witness_org", "org_source"]).unique().head(40)
    lines.append("| Committee | Date | Witness org | Source |")
    lines.append("|---|---|---|---|")
    for r in unb.iter_rows(named=True):
        lines.append(f"| {r['committee']} | {r['date']} | {r['witness_org']} | {r['org_source']} |")

    # unmatched & not obviously a body — likely extraction noise / pure topics to filter
    lines.append("\n## Unmatched & not obviously a body (likely topic/extraction noise)\n")
    unn = df.filter((pl.col("n_matched") == 0) & ~pl.col("is_public_body")).select(
        ["committee", "date", "witness_org", "org_source"]).unique().head(30)
    lines.append("| Committee | Date | Witness org | Source |")
    lines.append("|---|---|---|---|")
    for r in unn.iter_rows(named=True):
        lines.append(f"| {r['committee']} | {r['date']} | {r['witness_org']} | {r['org_source']} |")

    (OUT_DIR / "COVERAGE.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    logger.info("wrote %s", OUT_DIR / "COVERAGE.md")


def main() -> None:
    ap = argparse.ArgumentParser(description="Committee-evidence feasibility probe (sandbox, no promotion).")
    ap.add_argument("--committees", default="pac,housing", help="comma list of: " + ",".join(COMMITTEES))
    ap.add_argument("--max-meetings", type=int, default=25, help="cap meetings scanned per committee (smoke-safe)")
    ap.add_argument("--since", default="2024-01-01", help="ignore meetings before this ISO date")
    args = ap.parse_args()
    setup_standalone_logging("committee_evidence_probe")
    committees = [c.strip() for c in args.committees.split(",") if c.strip() in COMMITTEES]
    run(committees, args.max_meetings, args.since)


if __name__ == "__main__":
    main()
