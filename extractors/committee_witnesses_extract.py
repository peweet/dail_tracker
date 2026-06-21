"""Oireachtas committee-evidence witnesses → silver (Phase 1).

Extracts WHICH ORGANISATIONS gave evidence to which Oireachtas committee, on what
date — the ingestion backbone of the "Who gives evidence" feature (see
doc/committee-evidence plan; sandbox probe in pipeline_sandbox/committee_evidence/).

WHY THE TRANSCRIPT, NOT SUBMISSIONS
-----------------------------------
There is no committee-submissions API endpoint and the data.oireachtas.ie S3 directory
listing is denied, so submission PDFs are NOT enumerable. Committee MEETINGS are fully
enumerable via /v1/debates?chamber_type=committee, and the witness-ORG signal is in the
AKN-XML transcript itself: section <heading>s name the body/topic, and the Cathaoirleach's
opening welcome names the witnesses ("representatives of <ORG>"). Orgs are NOT tagged
(TLCOrganization absent), so we parse them from heading + welcome text. Witnesses are the
non-member speakers (Mr./Ms/Dr.; TDs read "Deputy"/"Senator").

CORRECTNESS (committee conflation guard)
----------------------------------------
A witness's *own* committee (e.g. the CCMA's internal "Housing, Building and Land Use
Committee") is NOT an Oireachtas committee. We record only "org X gave evidence to
Oireachtas committee Y on date Z". Committee identity is taken from the API
(house.committeeCode / showAs) and reconciled against the AKN FRBR path per meeting; rows
whose codes disagree are dropped, never guessed.

This is EXTRACTION ONLY (→ silver). Cross-referencing witness orgs to payments/lobbying
(Phase 2) and gold promotion (Phase 3) live in separate steps.

Run (smoke first — never a blind background pull):
  python extractors/committee_witnesses_extract.py --max-meetings 25 --since 2024-01-01
"""

from __future__ import annotations

import argparse
import logging
import re

import polars as pl

from config import SILVER_DIR
from services.http_engine import fetch_json, fetch_text
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

API = "https://api.oireachtas.ie/v1"
OUT_DIR = SILVER_DIR / "committee_evidence"

# Target committees, keyed by the API's authoritative committeeCode. Phase 1 scope = the
# two highest-value committees; widen by adding committeeCodes after this lands.
COMMITTEES = {
    "pac": "committee_of_public_accounts",
    "housing": "joint_committee_on_housing_local_government_and_heritage",
}

# A non-member witness <from> label (TDs read "Deputy X" / "Senator X"; chair "An
# Cathaoirleach"; ministers "Minister for ...").
_WITNESS_HONORIFIC = re.compile(r"^\s*(Mr\.?|Ms\.?|Mrs\.?|Dr\.?|Professor|Prof\.?|Cllr\.?)\s", re.I)
_MEMBER_PREFIX = re.compile(r"^\s*(Deputy|Senator|An Cathaoirleach|An Leas-Chathaoirleach|Minister)\b", re.I)

# Session-FORMAT tail (not the body): "...: Discussion", "...: Engagement with Chairperson".
_HEADING_FORMAT_TAIL = re.compile(
    r":\s*(Discussion|Engagement.*|Statements|Motion.*|Committee Stage.*|"
    r"Pre-Legislative Scrutiny.*|Examination.*|Briefing.*|Resumed.*|Meeting.*)$",
    re.I,
)
# Leading topic phrasing wrapping the body name ("Operations of Tailte Éireann" -> body).
_HEADING_TOPIC_PREFIX = re.compile(
    r"^(?:Strategic Priorities of|Operations of|Delivery of|Overview of|Update on|"
    r"Examination of|Engagement with|Discussion on|Briefing on|Report on the Accounts of|"
    r"Situation in|Meeting the [A-Za-z ]+ of)\s+(?:the\s+)?",
    re.I,
)
_HEADING_CHAPTER_PREFIX = re.compile(r"^Chapter\s+\d+\s*[-–—:]\s*", re.I)
_HEADING_ACCOUNTING_TAIL = re.compile(
    r"\s*[-–—:]\s*(?:Financial Statements|Appropriation Accounts|Annual Report)\b.*$", re.I)
# Headings that are pure topics/accounting artefacts (no body to extract) — dropped.
_HEADING_DROP = re.compile(
    r"^(?:Appropriation Accounts|Financial Statements|Report on the Accounts|"
    r"Report of the Comptroller|Vote Accounting|Superannuation|Public Expenditure,|"
    r"General Scheme|Proposed Changes|Business of|Chapter\s+\d+\b.*(?:Fund|Revenues|Neutrality|"
    r"Management|Clearing|Receipts|Standards|Purposes|Investment))",
    re.I,
)

# Cathaoirleach welcome phrasings introducing witness bodies (corroboration signal).
_WELCOME_PATTERNS = [
    re.compile(r"representatives? (?:of|from) (?:the )?([A-Z][^.:;,]{4,80})"),
    re.compile(r"officials? (?:of|from) (?:the )?([A-Z][^.:;,]{4,80})"),
    re.compile(r"witnesses? from (?:the )?([A-Z][^.:;,]{4,80})"),
    re.compile(r"I welcome[^.]*?from (?:the )?([A-Z][^.:;,]{4,80})"),
]
# Tightening: a welcome capture often runs into the name list / time / clause that follows.
# Cut at the first of these boundaries.
_WELCOME_TAIL_CUT = re.compile(
    r"\s+(?:Mr|Ms|Mrs|Dr|Professor)\b.*$|"      # name list begins
    r"\s+(?:at|from)\s+\d.*$|"                    # "... at 10", "... from 10"
    r"\s+(?:immediately|on completion|were before|to make|to give|to discuss|who).*$",  # trailing clause
    re.I,
)

# Bare generic captures (over-trimmed welcome text) that name no specific body — dropped.
_GENERIC_ORG = {"department", "departments", "office", "agency", "committee", "board",
                "commission", "council", "minister", "officials", "representatives"}

_TAG = re.compile(r"<[^>]+>")
_FROM = re.compile(r"<from[^>]*>(.*?)</from>", re.S)
_HEADING = re.compile(r"<heading[^>]*>(.*?)</heading>", re.S)
_SPEECH = re.compile(r"<speech[^>]*>(.*?)</speech>", re.S)
_FRBRTHIS = re.compile(r'<FRBRthis[^>]*value="([^"]+)"')


def _plain(s: str) -> str:
    return re.sub(r"\s+", " ", _TAG.sub("", s)).strip()


# --------------------------------------------------------------------------- enumerate


def enumerate_meetings(code: str, max_meetings: int, since: str) -> list[dict]:
    """Committee meeting records for one committeeCode (newest first) via /v1/debates.

    No silent truncation: logs when the --max-meetings cap is hit so a partial scan is
    never mistaken for full coverage.
    """
    out: list[dict] = []
    skip, page = 0, 500
    while len(out) < max_meetings:
        data, _ = fetch_json(f"{API}/debates?chamber_type=committee&limit={page}&skip={skip}")
        rows = data.get("results", [])
        if not rows:
            break
        for r in rows:
            rec = r.get("debateRecord", {})
            if rec.get("house", {}).get("committeeCode") != code:
                continue
            if rec.get("date", "") < since or not rec.get("formats", {}).get("xml"):
                continue
            out.append(rec)
            if len(out) >= max_meetings:
                logger.warning("hit --max-meetings cap (%d) for %s; older meetings NOT scanned",
                               max_meetings, code)
                return out
        page_dates = [r.get("debateRecord", {}).get("date", "") for r in rows]
        if page_dates and max(page_dates) < since:
            break
        skip += page
    return out


# ----------------------------------------------------------------------------- extract


def _clean_heading_org(h: str) -> str | None:
    """A witness body name from one section heading, or None if it's a pure topic."""
    if not h or h.lower().startswith("business of") or _HEADING_DROP.search(h):
        return None
    org = _HEADING_FORMAT_TAIL.sub("", h).strip()
    vm = re.match(r"Vote\s+\d+\s*[-–—]\s*(.+)", org)          # "Vote 45 - Dept ..." -> dept
    if vm:
        org = vm.group(1).strip()
    fm = re.match(r"(?:Financial Statements|Appropriation Accounts)[^:]*:\s*(.+)", org, re.I)
    if fm:                                                     # "...: National Training Fund" -> body
        org = fm.group(1).strip()
    org = _HEADING_CHAPTER_PREFIX.sub("", org)
    org = _HEADING_ACCOUNTING_TAIL.sub("", org).strip()       # "Body – Financial Statements 2024" -> Body
    org = _HEADING_TOPIC_PREFIX.sub("", org).strip()          # "Operations of X" -> X
    return org if 3 < len(org) < 90 else None


def extract_meeting(rec: dict) -> dict:
    """Witness orgs + persons + committee-identity reconciliation for one meeting."""
    house = rec.get("house", {})
    xml, _ = fetch_text(rec["formats"]["xml"]["uri"])

    # committee-identity reconciliation: API code vs AKN FRBR path
    frbr = _FRBRTHIS.search(xml)
    m = re.search(r"/debateRecord/([^/]+)/", frbr.group(1)) if frbr else None
    path_code = m.group(1) if m else ""
    reconciled = path_code == house.get("committeeCode")

    # org candidates: heading (primary) + welcome (corroboration). First writer of a
    # lower-cased key wins, so headings (looped first) take precedence over welcome.
    candidates: dict[str, tuple[str, str]] = {}  # key -> (org, source)
    for h in (_plain(x) for x in _HEADING.findall(xml)):
        org = _clean_heading_org(h)
        if org:
            candidates.setdefault(org.lower(), (org, "heading"))
    speeches = _SPEECH.findall(xml)
    welcome = _plain(" ".join(speeches[:2])) if speeches else ""
    for pat in _WELCOME_PATTERNS:
        for hit in pat.findall(welcome):
            org = _WELCOME_TAIL_CUT.sub("", hit).strip()
            if 3 < len(org) < 90 and org.lower() not in _GENERIC_ORG:
                candidates.setdefault(org.lower(), (org, "welcome"))

    froms = [_plain(f) for f in _FROM.findall(xml)]
    persons = sorted({f for f in froms if _WITNESS_HONORIFIC.match(f) and not _MEMBER_PREFIX.match(f)})

    orgs = [{"witness_org": org, "org_source": src} for org, src in candidates.values()]
    return {
        "committee_code": house.get("committeeCode"),
        "committee_name": house.get("showAs"),
        "house_no": house.get("houseNo"),
        "date": rec.get("date"),
        "source_xml": rec["formats"]["xml"]["uri"],
        "frbr_path_code": path_code,
        "reconciled": reconciled,
        "orgs": orgs,
        "persons": persons,
    }


# ------------------------------------------------------------------------------- main


def run(committees: list[str], max_meetings: int, since: str) -> None:
    org_rows: list[dict] = []
    person_rows: list[dict] = []
    dropped = 0
    for short in committees:
        code = COMMITTEES[short]
        meetings = enumerate_meetings(code, max_meetings, since)
        logger.info("%s (%s): %d meetings since %s", short, code, len(meetings), since)
        for rec in meetings:
            ev = extract_meeting(rec)
            if not ev["reconciled"]:
                dropped += 1
                logger.warning("DROP unreconciled meeting api=%s frbr=%s date=%s",
                               ev["committee_code"], ev["frbr_path_code"], ev["date"])
                continue
            base = {k: ev[k] for k in ("committee_code", "committee_name", "house_no", "date", "source_xml")}
            for o in ev["orgs"]:
                org_rows.append({**base, **o})
            for p in ev["persons"]:
                person_rows.append({**base, "witness_person": p})

    if dropped:
        logger.warning("dropped %d meeting(s) on committee-code reconciliation mismatch", dropped)
    if not org_rows:
        logger.error("no witness orgs extracted — check enumeration / extraction")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    orgs_df = pl.DataFrame(org_rows).unique(["committee_code", "date", "witness_org"]).sort(
        ["committee_code", "date", "witness_org"])
    persons_df = pl.DataFrame(person_rows).unique(["committee_code", "date", "witness_person"]).sort(
        ["committee_code", "date", "witness_person"])
    save_parquet(orgs_df, OUT_DIR / "committee_witnesses.parquet")
    save_parquet(persons_df, OUT_DIR / "committee_witness_persons.parquet")
    logger.info("wrote %d witness-org rows, %d witness-person rows -> %s",
                orgs_df.height, persons_df.height, OUT_DIR)


def main() -> None:
    ap = argparse.ArgumentParser(description="Committee-evidence witness extractor (-> silver).")
    ap.add_argument("--committees", default="pac,housing", help="comma list of: " + ",".join(COMMITTEES))
    ap.add_argument("--max-meetings", type=int, default=60, help="cap meetings per committee")
    ap.add_argument("--since", default="2024-01-01", help="ignore meetings before this ISO date")
    args = ap.parse_args()
    setup_standalone_logging("committee_witnesses")
    committees = [c.strip() for c in args.committees.split(",") if c.strip() in COMMITTEES]
    run(committees, args.max_meetings, args.since)


if __name__ == "__main__":
    main()
