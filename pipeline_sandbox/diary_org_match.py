"""Diary subject × lobbying-register org gazetteer match (Build Plan Phase 5.2).

Promotes the read-only probe (``probe_enrichment_matching.py`` EXP 1) to a
sandbox producer of the ``diary_org_mentions`` table (Build Plan §2.3). One row
per (diary entry × matched organisation). **Explosion-prone by design** — same
trap as ``procurement_lobbying_overlap``: NEVER count entries through this table
without ``COUNT(DISTINCT entry_id)``.

Gazetteer = TIER ① only for v1: lobbying-register org names (lobbyist_name +
client_name). This is the highest-value tier — a hit means *both sides of the
record exist* (a lobbyist named the org AND a minister's diary names it).
Tiers ②CRO ③stateboards ④alias are deferred (CRO is huge; see the build plan).

Matching is conservative — normalise then **exact-contains / token only, no
edit-distance** — with the two guards the round-1 probe proved necessary:
  * SURNAME guard: a single-token org whose token IS the entry's minister
    surname is dropped ("Minister Chambers" ≠ Chambers Ireland, "Harris" ≠
    Harris Group).
  * PLACENAME guard: a single-token org that is an Irish county/town is dropped
    (Shannon, Cork, …) — these collapse out of suffix-stripped names and match
    travel/venue lines.

Two confidence tiers (Build Plan §7.2): HIGH = a ≥2-token normalised org name
found contiguously in the subject (display tier); MEDIUM = a single distinctive
token (len≥6, non-stopword) co-occurring with an engagement cue (export/research
tier only). Recall is deliberately poor — say so downstream: matches are
indicative, not exhaustive.

PRECISION (Build Plan §7.2.4 display gate) — MEASURED 2026-06-13 on a full
census of the HIGH tier (382 mentions), adjudicated under explicit rules:
**96.3% precision (14 FP), clearing the 90% gate.** Residual FP classes, all
small: generic industry events ("Wind Energy Expo" vs Wind Energy Ireland, ×4),
a near-name collision (British Chamber vs British-Irish Chamber, ×2), and
historical persons not in the current-Dáil member file (Costello/Quinn/Coleman/
O'Connor/O'Regan, ×5). MEDIUM tier is research-only and unmeasured. Surface the
measured precision in provenance; frame matches as co-occurrence ("organisation
named in the diary"), never "the minister met X".

Known follow-ups to lift precision further: a HISTORICAL member-name list (the
current file is 176 current TDs only) kills the residual persons; a curated
event-phrase stoplist would catch the generic-industry tail.

Output -> data/sandbox/enrichment/diary_org_mentions.parquet (+ entry_id stamped
back onto ministerial_diary_entries.parquet so the two tables join).

Run: .venv/Scripts/python.exe pipeline_sandbox/diary_org_match.py
"""

from __future__ import annotations

import hashlib
import logging
import re
import unicodedata
from collections import defaultdict
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ENR = Path("data/sandbox/enrichment")
ENTRIES = ENR / "ministerial_diary_entries.parquet"
MENTIONS = ENR / "diary_org_mentions.parquet"
LOB = Path("data/silver/lobbying/parquet")

_SUFFIX_RE = re.compile(
    r"\b(limited|ltd|plc|p\.l\.c|dac|d\.a\.c|clg|uc|ulc|icav|teoranta|teo|cga|"
    r"company|co|holdings|group|ireland|irish|the)\b\.?",
)
_PUNCT_RE = re.compile(r"[^a-z0-9 ]")

# tokens too generic to anchor a single-token match (from the probe, frozen)
STOP = {
    "ireland", "irish", "national", "association", "federation", "society", "institute",
    "council", "group", "alliance", "forum", "network", "centre", "center", "office",
    "board", "union", "college", "university", "services", "service", "department",
    "minister", "meeting", "launch", "visit", "interview", "company", "limited",
    "irelands", "new", "event", "opening", "members", "enterprise", "chambers",
    "chamber", "local", "capital", "business", "holdings", "management", "partners",
    "technology", "energy", "media", "communications", "development",
}

# Irish counties + towns that collapse out of org names and collide with
# travel/venue lines. Single-token medium matches equal to one of these are
# dropped (the "Shannon" trap).
PLACENAMES = {
    "carlow", "cavan", "clare", "cork", "donegal", "dublin", "galway", "kerry",
    "kildare", "kilkenny", "laois", "leitrim", "limerick", "longford", "louth",
    "mayo", "meath", "monaghan", "offaly", "roscommon", "sligo", "tipperary",
    "waterford", "westmeath", "wexford", "wicklow", "shannon", "athlone",
    "dundalk", "drogheda", "navan", "bray", "tralee", "ennis", "naas", "mullingar",
    "letterkenny", "clonmel", "portlaoise",
}

CUE_RE = re.compile(
    r"\b(meeting|meet|mtg|call|phonecall|visit|launch|opening|reception|roundtable|"
    r"briefing|dinner|lunch|breakfast|address|speech|event|presentation|signing)\b"
)
# A person-title immediately before a matched span means the span is a PERSON,
# not the org ("Minister Harris" ≠ Harris Group, "Prof Eamonn Murphy" the person
# ≠ any firm). Deliberately EXCLUDES org-ambiguous titles (CEO/Chair/President/
# Director/Secretary) because those legitimately precede an org name
# ("CEO Wind Energy Ireland", "President of IBEC") — including them would drop
# true positives.
_PERSON_TITLE = (
    r"minister|min|taoiseach|tanaiste|deputy|dep|senator|sen|mr|ms|mrs|dr|prof|"
    r"professor|cllr|ambassador|amb|sir|lord"
)

# Curated demotion (HIGH → MEDIUM): org names whose distinctive token is a
# stripped geographic suffix, leaving a generic industry/topic phrase that the
# diary also uses as plain prose. These collide too often to sit in the display
# tier; they remain in the research/export tier. (NOT "wind energy" — Wind
# Energy Ireland is named directly often enough to stay high.)
GENERIC_TOPIC = {"renewable energy", "mental health", "european movement"}


def norm(s: str) -> str:
    """Normalise an org name / subject for matching (probe-identical)."""
    s = unicodedata.normalize("NFD", s.lower())
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    s = s.replace("&", " and ")
    s = _PUNCT_RE.sub(" ", s)
    s = _SUFFIX_RE.sub(" ", s)
    return re.sub(r"\s+", " ", s).strip()


def anchor_tier(n: str) -> str | None:
    """Tier a normalised org name, or None if it can't anchor a match.

    HIGH needs ≥2 tokens AND ≥9 non-space chars — the length floor rejects
    over-stripped generic stubs like "bank of" (from "Bank of Ireland Group
    plc") that otherwise collide with "Central Bank of Ireland".
    """
    toks = n.split()
    if len(toks) >= 2 and len(n.replace(" ", "")) >= 9:
        return "high"
    if len(toks) == 1 and len(toks[0]) >= 6 and toks[0] not in STOP:
        return "medium"
    return None


def entry_id(department: str | None, entry_date, time_slot: str | None, subject: str | None) -> str:
    """Stable content-hash surrogate key for a diary entry (re-parse safe)."""
    raw = f"{department}|{entry_date}|{time_slot}|{subject}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def build_gazetteer(
    lobbyists: list[str],
    clients: list[str],
    person_names: set[str] | None = None,
) -> dict[str, tuple[str, str, str]]:
    """norm -> (display_name, source, tier). Lobbyists win ties over clients.

    ``person_names`` (normalised TD/Senator full names) are excluded — a
    politician's name in the lobbying register is not an organisation, and it
    collides with diary subjects that simply mention that person.
    """
    persons = person_names or set()
    gaz: dict[str, tuple[str, str, str]] = {}

    def add(name: str, source: str) -> None:
        n = norm(name)
        if len(n) < 4 or n in gaz or n in persons:
            return
        t = anchor_tier(n)
        if not t:
            return
        if t == "high" and n in GENERIC_TOPIC:  # curated demotion
            t = "medium"
        gaz[n] = (name, source, t)

    for name in lobbyists:
        add(name, "lobbyist")
    for name in clients:
        if len(name) <= 120:  # client field sometimes holds a paragraph
            add(name, "client")
    return gaz


def build_token_index(gaz: dict[str, tuple[str, str, str]]) -> dict[str, set[str]]:
    idx: dict[str, set[str]] = defaultdict(set)
    for n in gaz:
        for t in n.split():
            if len(t) >= 4 and t not in STOP:
                idx[t].add(n)
    return idx


def match_subject(
    subject: str | None,
    minister: str | None,
    gaz: dict[str, tuple[str, str, str]],
    token_index: dict[str, set[str]],
) -> list[dict]:
    """Return org-mention dicts for one subject (guards applied)."""
    if not subject:
        return []
    subj_n = " " + norm(subject) + " "
    has_cue = bool(CUE_RE.search(subject.lower()))
    minister_norm = norm(minister) if minister else ""
    cands: set[str] = set()
    for tok in subj_n.split():
        cands |= token_index.get(tok, set())
    out: list[dict] = []
    for cand in cands:
        display, source, tier = gaz[cand]
        single = " " not in cand
        if tier == "medium" and not has_cue:
            continue
        if f" {cand} " not in subj_n:
            continue
        if single and cand == minister_norm:  # surname guard (entry's own minister)
            continue
        if single and cand in PLACENAMES:  # placename guard
            continue
        if re.search(rf"\b(?:{_PERSON_TITLE})\s+{re.escape(cand)}\b", subj_n):  # person-title guard (all tiers)
            continue
        out.append(
            {
                "matched_org_name": display,
                "match_source": "lobbying_register",
                "match_method": "exact_norm" if tier == "high" else "token_set",
                "match_confidence": tier,
                "gazetteer_key": cand,
                "gaz_origin": source,
            }
        )
    return out


def load_member_names() -> set[str]:
    """Normalised TD/Senator full names — excluded from the org gazetteer so a
    politician named in a diary subject is not mistaken for a lobbying org."""
    csv = Path("data/silver/flattened_members.csv")
    if not csv.exists():
        log.warning("flattened_members.csv missing (%s) — no member-name exclusion", csv)
        return set()
    names = pl.read_csv(csv, infer_schema_length=0).get_column("full_name").drop_nulls().to_list()
    return {n for raw in names if len(n := norm(raw)) >= 4}


def main() -> int:
    setup_standalone_logging("diary_org_match")
    if not ENTRIES.exists():
        log.error("entries parquet missing: %s", ENTRIES)
        return 1

    e = pl.read_parquet(ENTRIES)
    # stamp a stable entry_id so mentions join back (idempotent)
    e = e.with_columns(
        pl.struct(["department", "entry_date", "time_slot", "subject"])
        .map_elements(
            lambda s: entry_id(s["department"], s["entry_date"], s["time_slot"], s["subject"]),
            return_dtype=pl.String,
        )
        .alias("entry_id")
    )
    save_parquet(e, ENTRIES)
    e.write_csv(ENTRIES.with_suffix(".csv"))

    lobbyists = (
        pl.scan_parquet(LOB / "returns_master.parquet")
        .select("lobbyist_name")
        .unique()
        .collect()["lobbyist_name"]
        .drop_nulls()
        .to_list()
    )
    clients = (
        pl.scan_parquet(LOB / "client_company_returns_detail.parquet")
        .select("client_name")
        .unique()
        .collect()["client_name"]
        .drop_nulls()
        .to_list()
    )
    persons = load_member_names()
    gaz = build_gazetteer(lobbyists, clients, person_names=persons)
    token_index = build_token_index(gaz)
    log.info("gazetteer: %d orgs (%d tokens) from %d lobbyists + %d clients (%d member names excluded)",
             len(gaz), len(token_index), len(lobbyists), len(clients), len(persons))

    rows: list[dict] = []
    for r in e.iter_rows(named=True):
        for m in match_subject(r["subject"], r["minister"], gaz, token_index):
            rows.append({"entry_id": r["entry_id"], "entry_date": r["entry_date"],
                         "minister": r["minister"], "subject": r["subject"], **m})

    if not rows:
        log.warning("no org mentions matched — check gazetteer / normalisation")
        return 1
    m = pl.DataFrame(rows)
    save_parquet(m, MENTIONS)
    m.write_csv(MENTIONS.with_suffix(".csv"))

    n_entries = m["entry_id"].n_unique()
    log.info("MENTIONS: %d rows | %d DISTINCT entries (%.1f%% of %d)",
             len(m), n_entries, 100 * n_entries / len(e), len(e))
    log.info("  tier split: %s", m.group_by("match_confidence").len().sort("len", descending=True).to_dicts())
    log.info("  top matched orgs (DISTINCT entries):")
    top = (m.group_by("matched_org_name")
           .agg(pl.col("entry_id").n_unique().alias("n_entries"))
           .sort("n_entries", descending=True).head(12))
    for r in top.iter_rows(named=True):
        log.info("    %4d  %s", r["n_entries"], r["matched_org_name"][:50])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
