"""Diary subject × lobbying-register org gazetteer match (Build Plan Phase 5.2).

Promotes the read-only probe (``probe_enrichment_matching.py`` EXP 1) to a
sandbox producer of the ``diary_org_mentions`` table (Build Plan §2.3). One row
per (diary entry × matched organisation). **Explosion-prone by design** — same
trap as ``procurement_lobbying_overlap``: NEVER count entries through this table
without ``COUNT(DISTINCT entry_id)``.

Gazetteer tiers:
  ① lobbying-register org names (lobbyist_name + client_name) — the highest-value
     tier: a hit means *both sides of the record exist* (a lobbyist named the org
     AND a minister's diary names it); gaz_origin lobbyist/client.
  ③ STATEBOARDS (added 2026-06-19) — state-body names (Marine Institute, Low Pay
     Commission, ...). The diaries name agencies that never lobby, so these are
     diary-only orgs (gaz_origin='stateboard') that widen recall beyond the register.
  ⑤ CURATED ACRONYMS (added 2026-06-19, the "another angle") — see ACRONYMS. The
     diaries speak in acronyms (IBEC/IDA/IFA) the full-name register can't match;
     auto-generating them is precision-poison, so this is a hand-curated map matched
     case-sensitively whole-word against the raw subject (gaz_origin='acronym').
  ⑥ CURATED FULL-NAME ORGS (added 2026-06-19) — see CURATED_ORGS. Well-known orgs the
     register never carries AND norm() defeats ("Enterprise Ireland"→stopword "enterprise",
     432 misses; "Bank of Ireland"→stub). Matched as whole phrases on an accent-folded raw
     subject (gaz_origin='curated_org'), with substring guards (Microsoft Teams≠Microsoft,
     Central Bank of Ireland≠Bank of Ireland).
Tiers ②CRO ④alias remain deferred (CRO is huge — false-positive heavy; see the plan).
MEASURED 2026-06-19: adding ③+⑤ lifted mentions 1524 → 2687 (distinct diary entries
matched 3.3% → 5.8%); the acronym tier alone added 991 high-precision hits.

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

UPDATE 2026-06-19 (after the EDUCATION/DECC source extension grew the corpus to
~739 files): a new dominant FP class surfaced — the DCCS/Housing diaries append a
trailing attendee/adviser tag in ``<subject> - <official name>`` form, and those
official names sit in the lobbying register's ``person_primarily_responsible``
field, so e.g. 'Dónall Geoghegan' matched 178 internal Pre-Cab/Cab-Debrief lines.
FIX: ``load_responsible_persons`` now excludes those individuals from the gazetteer
(see its docstring), GUARDED by ORG_TOKENS + the client set so real orgs mistyped
into the person field survive. Measured effect: −11 person keys / −206 mention rows
(178 Geoghegan + 15 David Kelly + a small tail), zero real-org loss. Re-census the
HIGH-tier precision once the EDUCATION backfill completes.

Output -> data/sandbox/enrichment/diary_org_mentions.parquet (+ entry_id stamped
back onto ministerial_diary_entries.parquet so the two tables join).

Run: .venv/Scripts/python.exe extractors/diary_org_match.py
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
    "ireland",
    "irish",
    "national",
    "association",
    "federation",
    "society",
    "institute",
    "council",
    "group",
    "alliance",
    "forum",
    "network",
    "centre",
    "center",
    "office",
    "board",
    "union",
    "college",
    "university",
    "services",
    "service",
    "department",
    "minister",
    "meeting",
    "launch",
    "visit",
    "interview",
    "company",
    "limited",
    "irelands",
    "new",
    "event",
    "opening",
    "members",
    "enterprise",
    "chambers",
    "chamber",
    "local",
    "capital",
    "business",
    "holdings",
    "management",
    "partners",
    "technology",
    "energy",
    "media",
    "communications",
    "development",
    # generic English words that collapse out of org names and collide with diary prose
    # ("VISION HOLDINGS LIMITED" -> "vision", which matched "Sound and Vision Launch",
    # "EirGrid Vision 2030", "Our Vision Our Voice" etc. — all false positives).
    "vision",
}

# Irish counties + towns that collapse out of org names and collide with
# travel/venue lines. Single-token medium matches equal to one of these are
# dropped (the "Shannon" trap).
PLACENAMES = {
    "carlow",
    "cavan",
    "clare",
    "cork",
    "donegal",
    "dublin",
    "galway",
    "kerry",
    "kildare",
    "kilkenny",
    "laois",
    "leitrim",
    "limerick",
    "longford",
    "louth",
    "mayo",
    "meath",
    "monaghan",
    "offaly",
    "roscommon",
    "sligo",
    "tipperary",
    "waterford",
    "westmeath",
    "wexford",
    "wicklow",
    "shannon",
    "athlone",
    "dundalk",
    "drogheda",
    "navan",
    "bray",
    "tralee",
    "ennis",
    "naas",
    "mullingar",
    "letterkenny",
    "clonmel",
    "portlaoise",
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

# Tokens that betray an ORGANISATION even when its name slipped into the lobbying
# register's free-text person field — used to RESCUE real orgs from the person
# exclusion below (people mistype "Limerick Chamber" / "Macra na Feirme" into the
# person_primarily_responsible field, but those carry an org-indicator token).
ORG_TOKENS = {
    "chamber",
    "society",
    "association",
    "federation",
    "coalition",
    "council",
    "institute",
    "union",
    "company",
    "bank",
    "port",
    "bus",
    "rail",
    "airport",
    "school",
    "college",
    "university",
    "centre",
    "center",
    "network",
    "alliance",
    "forum",
    "trust",
    "foundation",
    "club",
    "agency",
    "authority",
    "board",
    "office",
    "partnership",
    "services",
    "solutions",
    "media",
    "energy",
    "technology",
    "systems",
    "group",
    "holdings",
    "consulting",
    "consultants",
    "feirme",
    "mhuire",
    "gaeilge",
    "eireann",
    "teoranta",
    "dhl",
    "four",
    "ltd",
    "plc",
    "clg",
}


# Curated acronym → canonical-org map (the "another angle" of 2026-06-19). The
# diaries speak in ACRONYMS while the lobbying register stores FULL names, so a
# full-name gazetteer alone misses ~11% of external meetings (IBEC/IDA/IFA/...).
# AUTO-GENERATING acronyms from the register is precision-poison (first-match-wins
# over 13k orgs hands "IDA"->Irish Dental Assoc, "SFA"->a school, "CEO"->Corporate
# Europe Observatory). So this is hand-curated to the dominant Irish public-affairs
# meaning, >=3 letters (no 2-letter EU/EI/FG collisions), no role/word initialisms
# (CEO/AGM/FOI/PMB). Matched CASE-SENSITIVELY as a whole word against the RAW subject
# (so "IDA" hits, "Ida" the name does not). Canonical names normalise onto the
# register/sector vocabulary so the downstream lobbying join + sector tag still fire.
ACRONYMS: dict[str, str] = {
    # business / employer & sector representative bodies
    "IBEC": "Irish Business and Employers Confederation",
    "ISME": "Irish Small and Medium Enterprises Association",
    "SFA": "Small Firms Association",
    "IFA": "Irish Farmers Association",
    "ICMSA": "Irish Creamery Milk Suppliers Association",
    "ICOS": "Irish Co-operative Organisation Society",
    "CIF": "Construction Industry Federation",
    "RGDATA": "RGDATA",
    "VFI": "Vintners Federation of Ireland",
    "LVA": "Licensed Vintners Association",
    "RAI": "Restaurants Association of Ireland",
    "DIGI": "Drinks Industry Group of Ireland",
    "RIAI": "Royal Institute of the Architects of Ireland",
    "SCSI": "Society of Chartered Surveyors Ireland",
    # state agencies / bodies
    "IDA": "IDA Ireland",
    "NSAI": "National Standards Authority of Ireland",
    "SEAI": "Sustainable Energy Authority of Ireland",
    "NTMA": "National Treasury Management Agency",
    "HSE": "Health Service Executive",
    "HEA": "Higher Education Authority",
    "QQI": "Quality and Qualifications Ireland",
    "EPA": "Environmental Protection Agency",
    "CRU": "Commission for Regulation of Utilities",
    "ESB": "Electricity Supply Board",
    "TII": "Transport Infrastructure Ireland",
    "NTA": "National Transport Authority",
    "LDA": "Land Development Agency",
    "IAA": "Irish Aviation Authority",
    "HIQA": "Health Information and Quality Authority",
    "SBCI": "Strategic Banking Corporation of Ireland",
    "IHREC": "Irish Human Rights and Equality Commission",
    # unions
    "ICTU": "Irish Congress of Trade Unions",
    "SIPTU": "SIPTU",
    "INTO": "Irish National Teachers Organisation",
    "ASTI": "Association of Secondary Teachers Ireland",
    "INMO": "Irish Nurses and Midwives Organisation",
    "IMO": "Irish Medical Organisation",
    # sport / civil society
    "GAA": "Gaelic Athletic Association",
    "FAI": "Football Association of Ireland",
    "IRFU": "Irish Rugby Football Union",
    "NWCI": "National Women's Council of Ireland",
    "SVP": "Society of Saint Vincent de Paul",
    "ICCL": "Irish Council for Civil Liberties",
    "NCSE": "National Council for Special Education",
    "CCPC": "Competition and Consumer Protection Commission",
    "AIB": "AIB",
    "DAA": "DAA",
    # multinationals / professional-services brand initialisms (safe whole-word)
    "AWS": "Amazon Web Services",
    "MSD": "MSD Ireland",
    "PWC": "PwC",
    "IBM": "IBM",
    "KPMG": "KPMG",
}
# Single case-sensitive whole-word alternation; longest-first so "ICMSA" wins over a
# shorter prefix. \b is unsafe (matches at digit/underscore joins), so use explicit
# ASCII-letter look-arounds.
_ACR_RE = re.compile(r"(?<![A-Za-z])(" + "|".join(sorted(ACRONYMS, key=len, reverse=True)) + r")(?![A-Za-z])")

# Curated full-name org map (Tier ⑥, added 2026-06-19). Well-known orgs/companies that
# recur in the diaries but the lobbying register never matches — either they don't lobby
# under that name, OR norm() defeats them: "Enterprise Ireland"→"enterprise" (a STOP word,
# 432 misses!), "Bank of Ireland"→"bank of" (rejected stub). So these are matched on a
# light ACCENT-FOLDED subject (no suffix strip) as whole phrases — bypassing both traps.
# Hand-vetted from a coverage scan of the unmatched entries; keys are ascii-lower phrases,
# values the canonical display. Single-token keys only where distinctive (no "meta"/"three").
CURATED_ORGS: dict[str, str] = {
    "microsoft": "Microsoft",
    "enterprise ireland": "Enterprise Ireland",
    "an post": "An Post",
    "bank of ireland": "Bank of Ireland",
    "dublin chamber": "Dublin Chamber",
    "tesco": "Tesco",
    "aldi": "Aldi",
    "lidl": "Lidl",
    "apple": "Apple",
    "eli lilly": "Eli Lilly",
    "irving oil": "Irving Oil",
    "combilift": "Combilift",
    "salesforce": "Salesforce",
    "linkedin": "LinkedIn",
    "nestle": "Nestlé",
    "boston scientific": "Boston Scientific",
    # pharma / medtech multinationals that recur but the register misses (added 2026-06-19).
    # J&J keyed on the FULL form only — bare "johnson" collides with people (Paul Johnson) +
    # venues (Tom Johnson House). "Analog" deliberately NOT added: its hits are products /
    # buildings ("Analog Active Learning Device", "Analog Building UL"), not Analog Devices.
    "astrazeneca": "AstraZeneca",
    "takeda": "Takeda",
    "regeneron": "Regeneron",
    "johnson and johnson": "Johnson & Johnson",
    # Added 2026-06-21 from a vetted scan of the OCR-expanded `other` bucket (doc/
    # DIARY_GAZETTEER_CANDIDATES.md). Each verified against real diary subjects before adding;
    # COLLISION-PRONE candidates DROPPED on inspection: "roche" (Stephen Roche / Rochestown Park
    # Hotel), "baxter" (Peter Baxter, a person), "bayer" (OCR noise), bare "kerry" (the county /
    # Radio Kerry — use the "kerry group" phrase instead). "cisco" is safe: the boundary guard
    # blocks "san francisco". Keys are distinctive (no common person/place collision) or phrases.
    "merck": "Merck",  # met Taoiseach Martin (expansion) + Calleary; distinct from MSD group entry
    "amgen": "Amgen",
    "cisco": "Cisco",
    "oracle": "Oracle",
    "coca cola": "Coca-Cola",
    "kerry group": "Kerry Group",
    "teva": "Teva Pharmaceuticals",
    "becton": "Becton Dickinson",
    "alexion": "Alexion",
    "thermo fisher": "Thermo Fisher Scientific",
    "anthropic": "Anthropic",
}
_CURATED_RE = re.compile(
    r"(?<![a-z0-9])(" + "|".join(sorted(CURATED_ORGS, key=len, reverse=True)) + r")(?![a-z0-9])"
)


def _fold_subject(s: str) -> str:
    """Lowercase + NFKD→ASCII + keep [a-z0-9 ] — accent-fold WITHOUT stripping suffixes, so
    "Nestlé"→"nestle" and "Enterprise Ireland" stays intact for curated-phrase matching."""
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode().lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9 ]", " ", s)).strip()


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
    stateboards: list[str] | None = None,
) -> dict[str, tuple[str, str, str]]:
    """norm -> (display_name, source, tier). Lobbyists win ties over clients.

    ``person_names`` (normalised TD/Senator full names) are excluded — a
    politician's name in the lobbying register is not an organisation, and it
    collides with diary subjects that simply mention that person.

    ``stateboards`` (state-body names) widen the gazetteer beyond the lobbying
    register (Tier ③): the diaries name agencies/commissions that never file a
    lobbying return, so these add diary-only orgs (gaz_origin='stateboard'). They
    are added LAST so an org already present from the register keeps its origin.
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
    for name in stateboards or []:
        add(name, "stateboard")
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
                "match_source": "lobbying_register" if source in ("lobbyist", "client") else source,
                "match_method": "exact_norm" if tier == "high" else "token_set",
                "match_confidence": tier,
                "gazetteer_key": cand,
                "gaz_origin": source,
            }
        )

    # Curated-acronym tier: case-sensitive whole-word hits in the RAW subject. These
    # recover the acronym-heavy diaries the full-name gazetteer can't reach. Skip any
    # whose canonical name already matched above (avoid double-counting IBEC etc.).
    already = {norm(o["matched_org_name"]) for o in out}
    for m in _ACR_RE.finditer(subject):
        display = ACRONYMS[m.group(1)]
        if norm(display) in already:
            continue
        already.add(norm(display))
        out.append(
            {
                "matched_org_name": display,
                "match_source": "curated_acronym",
                "match_method": "acronym",
                "match_confidence": "high",
                "gazetteer_key": m.group(1),
                "gaz_origin": "acronym",
            }
        )

    # Curated full-name tier: whole-phrase hits in the accent-folded subject. Recovers the
    # well-known orgs the register + norm() miss (Microsoft/Enterprise Ireland/An Post/...).
    # Two substring guards first: "Microsoft Teams" rides venue lines ("(Microsoft Teams
    # Meeting)") and is NOT Microsoft the company (~220 false hits); and "Central Bank of
    # Ireland" (the regulator) must not match the curated "Bank of Ireland" — gluing "central
    # bank" into one token removes the word boundary before "bank".
    folded = (
        _fold_subject(subject)
        .replace("microsoft teams", "teams")
        .replace("ms teams", "teams")
        .replace("central bank", "centralbank")
    )
    for m in _CURATED_RE.finditer(folded):
        display = CURATED_ORGS[m.group(1)]
        if norm(display) in already:
            continue
        already.add(norm(display))
        out.append(
            {
                "matched_org_name": display,
                "match_source": "curated_org",
                "match_method": "curated",
                "match_confidence": "high",
                "gazetteer_key": m.group(1),
                "gaz_origin": "curated_org",
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


def load_responsible_persons(client_keys: set[str]) -> set[str]:
    """Normalised INDIVIDUAL names from returns_master.person_primarily_responsible.

    These are the people who actually did the lobbying — not organisations. Their
    names collide with diary subjects that append an attendee/adviser tag in a
    trailing ``<subject> - <official>`` form (the DCCS/Housing diaries do this):
    e.g. 178 'Dónall Geoghegan' hits on internal Pre-Cab/Cab-Debrief lines. Excluding
    them is the same principle as the TD-name exclusion (a person's name is not an org).

    GUARDED so a real org mistyped into the person field survives: a name is treated
    as a person ONLY if it is two tokens, carries no ORG_TOKENS indicator, and is not
    itself a known client org (Limerick Chamber, Macra na Feirme, One in Four, ...).
    """
    p = LOB / "returns_master.parquet"
    if not p.exists():
        return set()
    raw = (
        pl.scan_parquet(p)
        .select("person_primarily_responsible")
        .unique()
        .collect()["person_primarily_responsible"]
        .drop_nulls()
        .to_list()
    )
    out: set[str] = set()
    for value in raw:
        n = norm(re.split(r"[(,/]", value)[0])  # drop trailing "(title)" / ", role" noise
        if is_personal_name(n, client_keys):
            out.add(n)
    return out


def is_personal_name(name_norm: str, client_keys: set[str]) -> bool:
    """True if a normalised name (drawn from the person field) reads as an INDIVIDUAL.

    Person ⇔ exactly two tokens, carries no ORG_TOKENS indicator, and is not itself a
    known client org. The org-token + client guards are what rescue real organisations
    that were mistyped into the person field (Limerick Chamber, Macra na Feirme, ...)."""
    toks = name_norm.split()
    return (
        len(toks) == 2 and 4 <= len(name_norm) <= 40 and name_norm not in client_keys and not (set(toks) & ORG_TOKENS)
    )


def reclassify_other_as_external(e: pl.DataFrame, matched_entry_ids: set[str]) -> tuple[pl.DataFrame, int]:
    """Promote 'other'-class entries that NAME a gazetteer org to 'external_meeting'.

    The keyword classifier (diary_entry_classify) needs a trigger verb, so a terse entry
    whose subject is JUST the org name ("Nestlé Ireland", "SFA Council", "Irish Hotels
    Federation") falls to 'other'. But if this matcher found a real org in it, it IS an
    external engagement — promote it so a front-end filter on external meetings includes it.

    One-directional (only 'other' is ever touched) and idempotent. This is an org-EVIDENCE
    refinement layered on top of the keyword pass — the matcher must run after the classifier.
    """
    if "entry_class" not in e.columns:
        return e, 0
    mask = (pl.col("entry_class") == "other") & pl.col("entry_id").is_in(list(matched_entry_ids))
    n = int(e.filter(mask).height)
    out = e.with_columns(
        pl.when(mask).then(pl.lit("external_meeting")).otherwise(pl.col("entry_class")).alias("entry_class")
    )
    return out, n


def load_stateboards() -> list[str]:
    """State-body names (Tier ③ gazetteer widening). The diaries name agencies and
    commissions that never lobby — Marine Institute, Low Pay Commission, Land
    Development Agency — so adding them recovers genuine engagements the lobbying
    register can't. Uses both the short `body` and the `body_full` label."""
    p = Path("data/gold/parquet/stateboards_roster.parquet")
    if not p.exists():
        return []
    df = pl.scan_parquet(p).select("body", "body_full").collect()
    names = set(df["body"].drop_nulls().to_list()) | set(df["body_full"].drop_nulls().to_list())
    return sorted(names)


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
    # Person exclusions: TD/Senator names + the individuals who did the lobbying
    # (person_primarily_responsible), the latter guarded so real orgs survive.
    client_keys = {norm(c) for c in clients if len(c) <= 120}
    members = load_member_names()
    responsible = load_responsible_persons(client_keys)
    persons = members | responsible
    stateboards = load_stateboards()  # Tier ③ widening beyond the lobbying register
    gaz = build_gazetteer(lobbyists, clients, person_names=persons, stateboards=stateboards)
    token_index = build_token_index(gaz)
    log.info(
        "gazetteer: %d orgs (%d tokens) from %d lobbyists + %d clients + %d stateboards + %d acronyms "
        "+ %d curated orgs (%d member + %d lobbying-person names excluded)",
        len(gaz),
        len(token_index),
        len(lobbyists),
        len(clients),
        len(stateboards),
        len(ACRONYMS),
        len(CURATED_ORGS),
        len(members),
        len(responsible),
    )

    # Match on a URL-STRIPPED subject so a pasted "https://meet.google.com" / "goo.gl/maps"
    # venue link never false-matches the company (Google/Maps in a URL is not a Google meeting).
    # The RAW subject is still stored for provenance; the page strips URLs again for display.
    url_re = re.compile(r"https?://\S+|www\.\S+")
    rows: list[dict] = []
    for r in e.iter_rows(named=True):
        subj_for_match = url_re.sub(" ", r["subject"]) if r["subject"] else r["subject"]
        for m in match_subject(subj_for_match, r["minister"], gaz, token_index):
            rows.append(
                {
                    "entry_id": r["entry_id"],
                    "entry_date": r["entry_date"],
                    "minister": r["minister"],
                    "subject": r["subject"],
                    **m,
                }
            )

    if not rows:
        log.warning("no org mentions matched — check gazetteer / normalisation")
        return 1
    m = pl.DataFrame(rows)
    save_parquet(m, MENTIONS)
    m.write_csv(MENTIONS.with_suffix(".csv"))

    # Org-evidence reclassification: promote 'other' entries that named a real org to
    # external_meeting and re-persist entries (entry_id already stamped above).
    e, n_promoted = reclassify_other_as_external(e, {r["entry_id"] for r in rows})
    if n_promoted:
        save_parquet(e, ENTRIES)
        e.write_csv(ENTRIES.with_suffix(".csv"))
        log.info("reclassified %d 'other' entries -> external_meeting (named a gazetteer org)", n_promoted)

    n_entries = m["entry_id"].n_unique()
    log.info(
        "MENTIONS: %d rows | %d DISTINCT entries (%.1f%% of %d)", len(m), n_entries, 100 * n_entries / len(e), len(e)
    )
    log.info("  tier split: %s", m.group_by("match_confidence").len().sort("len", descending=True).to_dicts())
    log.info("  top matched orgs (DISTINCT entries):")
    top = (
        m.group_by("matched_org_name")
        .agg(pl.col("entry_id").n_unique().alias("n_entries"))
        .sort("n_entries", descending=True)
        .head(12)
    )
    for r in top.iter_rows(named=True):
        log.info("    %4d  %s", r["n_entries"], r["matched_org_name"][:50])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
