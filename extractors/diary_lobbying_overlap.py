"""Diary × lobbying-register OVERLAP — the strict, display-grade join (sandbox).

Takes the noisy ``diary_org_mentions`` co-occurrence table (extractors/diary_org_match.py)
and distils it to the *defensible* signal: a lobbying-registered organisation that a
minister actually MET, with a corroboration flag for "this org also lobbied this same
minister". This is the table a front-end "Ministers met these registered interests"
feature should sit on — NOT the raw mentions, which are too noisy to ship.

Strictness (each filter removes a measured noise class — see project_ministerial_diaries):
  1. KEEP both tiers,      — the ``match_confidence`` column (high|medium) is carried through
     carry confidence        so the front-end can lead with HIGH (≥2-token name found verbatim,
                             96.3% measured precision) and mark MEDIUM as lower-confidence.
                             DO NOT drop MEDIUM: a single distinctive token + an engagement cue
                             is exactly how every single-word GLOBAL BRAND lands here — "Google",
                             "Insurance" (Insurance Ireland), "Vodafone", "Pfizer" all normalise
                             to one token, so a HIGH-only filter silently deletes the marquee
                             names. (Generic single tokens are already gated upstream by the
                             STOP list + length floor + engagement-cue requirement.)
  2. drop entry_class      — travel + media. "Return flight from Brussels Aer Lingus" and
     {travel, media}        "Virgin Media Tonight Show" are the org name appearing in a
                             flight/photocall line, not a meeting. This is the single
                             biggest contaminant (Aer Lingus is ~all travel).
  3. PERSON_DENYLIST       — individual lobbyists / advisers / journalists whose 2-token
                             personal name passed the org gazetteer (e.g. Dónall Geoghegan,
                             a ministerial adviser, 177×). STOPGAP until the historical-
                             member-name list lands (diary_org_match.py docstring follow-up);
                             current-Dáil TDs are already excluded upstream.

Corroboration ("lobbied AND met the same minister") joins each surviving meeting to
data/silver/lobbying/parquet/lobby_break_down_by_politician.parquet (the named-target
table: one row per return × politician × activity) on normalised org-name AND minister
SURNAME. Surname keys are crude (the diary minister is a filename guess like "Ryans") so a
True here is indicative, not proof — and a common surname can collide. NEVER read this as
influence/causation: it evidences ACCESS (lobbied + met), there is no outcome variable.

Outputs -> data/sandbox/enrichment/
  diary_lobbying_overlap.parquet        one row per (meeting × matched org); the detail grain
  diary_lobbying_overlap_ranked.parquet one row per org: who met ministers most + corroboration

Run: .venv/Scripts/python.exe extractors/diary_lobbying_overlap.py
"""

from __future__ import annotations

import logging
import re
import unicodedata
from pathlib import Path

import polars as pl

from extractors._diary_minister import resolve_minister  # canonical minister from filename/dept+date
from extractors.diary_org_match import ACRONYMS, norm  # identical org/subject normaliser + curated acronym map
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ENR = Path("data/sandbox/enrichment")
ENTRIES = ENR / "ministerial_diary_entries.parquet"
MENTIONS = ENR / "diary_org_mentions.parquet"
POL = Path("data/silver/lobbying/parquet/lobby_break_down_by_politician.parquet")
OUT_DETAIL = ENR / "diary_lobbying_overlap.parquet"
OUT_RANKED = ENR / "diary_lobbying_overlap_ranked.parquet"

# diary entry classes that are NOT a meeting with the named org (the org name rides a
# travel/photocall line). Everything else (external_meeting, govt_business, other,
# internal_dept, constituency, oireachtas) can legitimately host a real engagement.
EXCLUDE_CLASSES = {"travel", "media"}

# Government bodies that leak into the gazetteer via the client_name field — a department
# meeting a minister is not an outside interest lobbying. Drop by name prefix/keyword.
_GOV_BODY_RE = re.compile(
    r"^(department of|office of|houses of the oireachtas|an roinn)\b|\bgovernment\b",
    re.IGNORECASE,
)

# Vetted individual-name false positives (persons, not orgs) that cleared the org
# gazetteer's current-TD exclusion. Match on the verbatim display name. Stopgap — a
# historical-member + adviser name list would generalise this.
PERSON_DENYLIST = {
    "Dónall Geoghegan",
    "David Kelly",
    "Harry McGee",
    "Dave Fallon",
    "Patrick Costello",
    "Tony O'Brien",
    "Brian Carroll",
    "Tara Farrell",
    "Brendan Griffin",
    "John O'Neill",
    "Marc Coleman",
    "Seamus Quinn",
    "Pat Fitzpatrick",
    "Conor Kelly",
    "John Lynch",
    "Niall O'Connor",
    "bryan lynam",
}

# Heuristic sector tag, applied to a FOLDED org name (NFKD-ASCII + "&"→"and" + lowercase, via
# _fold below — so "Uisce Éireann", "Bus Éireann", "Banking & Payments Federation" match plain
# ASCII patterns). First pattern wins → ORDER MATTERS: specific industries first, broad civic
# buckets (sport/arts/education/professional/business-reps/charity) last so e.g. "Special
# Olympics"→sport not charity, "King's Inns"→professional not charity. NOT authoritative — a
# keyword map for slicing the overlap, not classification of record. Unmatched → "other".
SECTOR_PATTERNS: list[tuple[str, re.Pattern[str]]] = [
    # STATE bodies / non-commercial semi-states / regulators / agencies — placed FIRST so a state
    # agency is never mis-tagged into an industry (e.g. SEAI≠energy company, NSAI≠standards firm).
    # A minister meeting these is government business, not an outside interest lobbying — the
    # front-end should treat this bucket separately from private-interest sectors. (Commercial
    # semi-states that trade like firms — ESB, Bord na Móna, Uisce Éireann, Coillte — deliberately
    # stay in their industry sector; the line here is non-commercial agency/regulator vs trading utility.)
    (
        "state-semi-state",
        re.compile(
            r"(ida ireland|enterprise ireland|health service executive|\bhse\b|transport infrastructure|sustainable energy authority|\bseai\b|eirgrid|commission for regulation|standards authority|\bnsai\b|tailte|strategic banking|low pay commission|human rights and equality|legal aid board|treasury management|\bntma\b|housing finance agency|inland fisheries|legal services regulat|health information and quality|\bhiqa\b|parole board|coimisiun pleanala|western development|injuries resolution|injuries board|digital hub|valuation tribunal|residential tenancies|marine institute|private security authority|road safety authority|oversight and audit|\bgaisce\b|failte ireland|"
            # non-commercial agencies/regulators that FIRST-MATCH-WINS otherwise mis-sorts into a
            # civic bucket (EPA→'environmental'→charity-ngo; HEA→'education'; NTA→'national transport')
            # — these are state bodies, so is_state_body must flag them. (Commercial semi-states that
            # TRADE — ESB/Dublin Port/LDA — deliberately stay in their industry sector per the rule above.)
            r"environmental protection|higher education authority|national transport authority)"
        ),
    ),
    (
        "us-tech",
        re.compile(
            r"(google|meta platform|facebook|whatsapp|instagram|\bapple\b|amazon|microsoft|\bintel\b|\bibm\b|salesforce|linkedin|nvidia|oracle|cisco|hewlett|workday|huawei|tiktok|\bdell\b|stripe|paypal|mastercard|\bvisa\b|airbnb|taoglas|general electric|openai|accenture|schneider electric|clunetech|deliveroo|\bsap\b|adobe|activision|coinbase|\bindeed\b|explorium|firebird|too good to go)"
        ),
    ),
    (
        "data-centres-digital-infra",
        re.compile(
            r"(data centre|data center|echelon|equinix|digital infrastructure|cloud infrastructure|hyperscale|colocation)"
        ),
    ),
    (
        "medical-devices",
        re.compile(
            r"(medtech|medtronic|boston scientific|stryker|\babbott\b|becton|cook medical|edwards lifesci|edwards life sci|teleflex|integer|depuy|dexcom|veonet|bausch|\blomb\b|solventum|johnson and johnson)"
        ),
    ),
    (
        "pharma",
        re.compile(
            r"(pharma|\bipha\b|pfizer|\bmsd\b|novartis|abbvie|eli lilly|\broche\b|gilead|takeda|astrazeneca|regeneron|astellas|jazz pharma|pharmacy union|sanofi|biomarin|janssen|zoetis|novo nordisk|horizon therapeutics|therapeutics)"
        ),
    ),
    ("insurance", re.compile(r"(insurance|aviva|zurich|allianz|\baxa\b|\bfbd\b|irish life|\bvhi\b|laya)")),
    (
        "banking-finance",
        re.compile(
            r"(bank of ireland|\baib\b|allied irish|permanent tsb|ptsb|ulster bank|banking and payments|bpfi|credit union|finance ireland|\bdavy\b|goodbody|revolut|financial services union|euronext|stock exchange|payments europe)"
        ),
    ),
    (
        "funds-leasing",
        re.compile(
            r"(irish funds|hedge|blackrock|private equity|alternative invest|avolon|aercap|gecas|aircraft leas|\bkkr\b|carlyle|amundi)"
        ),
    ),
    ("gambling-betting", re.compile(r"(entain|flutter|bookmaker|paddy power|\bbetting\b|gambling)")),
    (
        "energy-utilities",
        re.compile(
            r"(wind energy|\besb\b|bord gais|bord na mona|uisce eireann|gas networks|\bsse\b|energia|renewable|hydrogen|offshore wind|nephin energy|electricity|\bcoillte\b|statkraft|pinergy|solar energy|heatgrid|energy co-?operative|supernode|solid fuel|flotation energy|form energy|futurenergy|greenlink|interconnector|bioenergy|district energy|liquid gas|new fortress|ocean winds|pardus energy|tipperary energy|community power|demand response|glen dimplex)"
        ),
    ),
    (
        "telecoms-media",
        re.compile(
            r"(vodafone|\beir\b|three ireland|virgin media|sky ireland|comreg|broadcast|newsbrands|\brte\b|bauer media|verizon|netflix|lens media)"
        ),
    ),
    (
        "transport-aviation",
        re.compile(
            r"(aer lingus|ryanair|\bdaa\b|dublin airport|shannon airport|cork airport|dublin port|port of cork|shannon foynes|bus eireann|dublin bus|iarnrod|\bcie\b|national transport|irish ferries|dublin aerospace|aviation|airlines for america|aer rianta|coach tourism|harbour company|\biata\b|international air transport|ireland west airport|\bknock\b|lufthansa|manna drone|mobility partnership|taxis for ireland|tier mobility|zipp mobility|motor industry|\bsimi\b|nissan)"
        ),
    ),
    (
        "mining-industrial",
        re.compile(
            r"(tara mines|aughinish|alumina|boliden|cement|\bquarr|smurfit|kingspan|ecocem|plant contractors|\bplastics\b)"
        ),
    ),
    (
        "property-construction",
        re.compile(
            r"(construction industry|\bcif\b|\bproperty\b|cairn|glenveagh|land development|housing alliance|estate agents|auctioneers|breffni group|kinsealy|cluid|social housing|approved housing|green building|hammerson|concrete|construction defects|oaklee|local development)"
        ),
    ),
    # CLINICAL / PROVIDER / professional health bodies only — patient & disease ADVOCACY
    # charities (cancer, heart, diabetes, parkinson's…) live in charity-ngo for consistency.
    (
        "healthcare-health-bodies",
        re.compile(
            r"(nursing homes|\bhospital\b|rotunda|royal college|college of physicians|college of ophthalm|nurses and midwives|social care|care alliance|equine centre|\binmo\b|irish medical organisation|\bimo\b|childrens health|private hospitals)"
        ),
    ),
    (
        "hospitality-tourism-events",
        re.compile(
            r"(vintner|\blva\b|\bvfi\b|drinks ireland|drinks industry|diageo|heineken|guinness|whiskey|distiller|sazerac|alcohol beverage|publican|restaurants association|hotels federation|\bhotel\b|self catering|hospitality|event industry|venue operators|visitor experiences|amusement trades|travel agents|center parcs|staycity|westbury|westport house|caravan and camping|capital entertainment|entertainment and leisure)"
        ),
    ),
    (
        "farming-agri",
        re.compile(
            r"(farmers|\bifa\b|icmsa|macra|\bicsa\b|teagasc|bord bia|ornua|kerry group|glanbia|tirlan|dairygold|lakeland|dairy industry|meat industry|\bmii\b|keelings|devenish|horticultur|\bagri|forest|grain and feed|national stud|atlantic dawn|marine harvest|seafood|fishing|gurteen|co-operative organisation)"
        ),
    ),
    (
        "food-consumer-goods",
        re.compile(
            r"(danone|musgrave|nestle|kerry foods|food and drink ireland|\bfdii\b|primark|brown thomas|dunnes|tesco|\blidl\b|\baldi\b|supervalu|pepsico|supermac|kellanova)"
        ),
    ),
    ("retail-trade", re.compile(r"(retail|\brgdata\b|grocer|hardware association|shopping|consumer)")),
    ("field-sports-game", re.compile(r"(game council|\bnargc\b)")),
    (
        "sport",
        re.compile(
            r"(cricket|rowing ireland|cycling ireland|federation of irish sport|horse racing|greyhound|special olympics|\bgaa\b|gaelic athletic|rugby|\bfai\b|football association|olympic|sport ireland|athletics|ireland active|basketball|tennis|physical activity|little fitness)"
        ),
    ),
    (
        "arts-culture-heritage",
        re.compile(
            r"(theatre|gallery|museum|concert hall|national library|screen producers|business to arts|piobairi|conradh na gaeilge|gaeilge|gaeloideachas|gaeltacht|udaras|waterways|airfield|\bzoo\b|heritage|arts council|\bfilm\b|poetry|project arts|music generation|bookselling|writers centre|troy studios|omniplex)"
        ),
    ),
    (
        "education-research",
        re.compile(
            r"(universit|\bucd\b|\bdcu\b|trinity college|maynooth|institute of technology|educate together|skillnet|\biiea\b|international and european affairs|\beducation\b|colleges|\bresearch\b|alexandra college)"
        ),
    ),
    (
        "professional-services",
        re.compile(
            r"(chartered accountants|\bmazars\b|\bkpmg\b|\bpwc\b|deloitte|grant thornton|engineers ireland|planning institute|kings inns|law society|bar council|architects|surveyors|solicitors|recruitment federation|eversheds|william fry|\bturley\b|consulting|institute of directors|medical protection)"
        ),
    ),
    (
        "unions-labour",
        re.compile(
            r"(trade union|\bictu\b|congress of trade|postmasters|\bsiptu\b|\bforsa\b|\btui\b|teachers union|national teachers organisation|secondary teachers|workers union|garda representative|garda sergeants|sergeants and inspectors|national union of journalists|union of students|students in ireland)"
        ),
    ),
    ("local-government", re.compile(r"(county council|city council|county and city|local authorit)")),
    (
        "business-reps-chambers",
        re.compile(
            r"(chamber|\bibec\b|\bisme\b|small and medium enterprises|\bsfa\b|guaranteed irish|exporters|business association|european movement|asia matters|sme recovery|business in the community|workforce ireland|small firms|employers|ireland canada|business district)"
        ),
    ),
    # patient & disease advocacy charities included here (NOT healthcare-health-bodies) so all
    # health NGOs sit in one bucket alongside cancer/heart: diabetes, parkinson, endometriosis, etc.
    (
        "charity-ngo",
        re.compile(
            r"(foundation|charit|\bngo\b|inclusion|threshold|\brespond\b|carers|crosscare|barnardos|hospice|rape crisis|\bcancer\b|mental health|disabilit|down syndrome|diabetes|parkinson|endometriosis|family planning|simon communit|depaul|st vincent|social justice|womens council|women for election|national women|womens link|youth work|victim support|advocacy|thalidomide|coalition 2030|co-operation ireland|friends of the earth|environmental|ruhama|prosper|refugee|migrant|neurological|special needs|st michael|grow remote|age action|merchants quay|extern|open doors|dignity ireland|family resource|community development|acquired brain injury|aoibhneas|blossom|cope galway|climate and health|crime victims|cuan mhuire|cystic fibrosis|early childhood|\bempower\b|glencree|inner city|civil liberties|mens sheds|penal reform|traveller|pavee|mcverry|\bpieta\b|rare diseases|sadaka|see change|sex workers|sexual violence|alzheimer|red door|trocaire|uplift|womens aid|\bymca\b|one family|one in four|natural capital|rediscovery|avista|st joseph|vincent de paul|peace and reconciliation)"
        ),
    ),
]


def _fold(name: str) -> str:
    """Fold for sector matching: drop apostrophes, NFKD→ASCII, '&'→'and', collapse, lowercase.

    Apostrophes are stripped (not spaced) so "Women's"→"womens", "King's Inns"→"kings inns"
    match plain tokens regardless of straight (') vs curly (’) form.
    """
    s = name.replace("'", "").replace("’", "").replace("‘", "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", s.replace("&", " and ")).strip().lower()


def sector_for(org_name: str | None) -> str:
    """First-matching SECTOR_PATTERNS label for an org name, else 'other' (heuristic)."""
    if not org_name:
        return "other"
    folded = _fold(org_name)
    for label, rx in SECTOR_PATTERNS:
        if rx.search(folded):
            return label
    return "other"


# The diary↔register join keys on a normalised org name, but the two registers SPELL the
# same body differently: the lobbying register tags the acronym onto the name ("Construction
# Industry Federation (CIF)", "The Irish Farmers' Association - IFA") or files under the bare
# acronym ("Ibec"), while the diary gazetteer carries the clean/expanded form ("Construction
# Industry Federation", "Irish Farmers Association", "Irish Business and Employers Confederation").
# norm() alone leaves a trailing "cif"/"ifa" token or a lone "ibec" → the join MISSES exactly the
# heaviest representative-body lobbyists (CIF 545 returns, IFA 3,666, Ibec) and they read as
# corroborated=false. The SQL view flagged this as the "alias map" debt. org_key() bridges it by
# REUSING the hand-curated ACRONYMS map (single source of truth, same canonical the diary emits):
# a name that IS an acronym, or that redundantly tags its OWN acronym, collapses to the acronym's
# canonical norm; everything else falls back to plain norm(). Two guards keep it precise:
#   * whole-name match is case-insensitive but anchored to the ENTIRE name ("Ibec"→IBEC, never
#     "Ida" mid-prose) — this table is registered org names, not free subject text;
#   * a tagged acronym only collapses when the name MINUS the tag normalises to the acronym's OWN
#     expansion, so self-acronyms ("… (CIF)") fold but a PARENT tag does not — "BioPharmaChem
#     Ireland (Ibec)" stays BioPharmaChem, it is not Ibec.
# Applied IDENTICALLY to both the diary matched_org_name and the register lobbyist_name so the two
# sides converge. Adds no new manual list and inherits ACRONYMS' curated single-meaning precision.
_ACR_TAG = {
    a: re.compile(rf"(\(\s*{re.escape(a)}\s*\))|([-–]\s*{re.escape(a)}\s*$)|(^\s*{re.escape(a)}\s*[-–])", re.IGNORECASE)
    for a in ACRONYMS
}


# Curated NON-ACRONYM aliases: residual diary↔register misses the acronym bridge can't reach,
# where the register files under a fuller legal entity / alternate name than the diary's brand
# form. Keys are the org_key() OUTPUT of the REGISTER variant (post-norm); values the diary
# canonical key. Hand-vetted (lobbying register, verified a real entry with returns exists), same
# idiom as ACRONYMS/CURATED_ORGS. PRECISION: each maps ONE specific register entity, never a
# substring — "Dublin Chamber of Commerce"→"dublin chamber" does NOT pull in "South Dublin Chamber";
# "Trinity College Dublin Students' Union" is deliberately ABSENT (a distinct body, not the college).
DIARY_REGISTER_ALIASES: dict[str, str] = {
    "dublin chamber of commerce": "dublin chamber",
    "microsoft operations": "microsoft",
    "apple distribution international": "apple",
    "siptu services industrial professional trade union": "siptu",
    "pricewaterhousecoopers": "pwc",
    "cisco international": "cisco",
    "lidl gmbh": "lidl",
    "cumann peile na h eireann football association of": "football association of",
}


def org_key(name: str | None) -> str:
    """Normalised org-identity key bridging register names to the diary canonical: folds an
    acronym-tagged/bare-acronym register name onto its ACRONYMS canonical, then applies the
    curated non-acronym alias map. Falls back to norm(). Applied IDENTICALLY to either side."""
    if not name:
        return ""
    s = name.strip()
    if s.upper() in ACRONYMS:  # whole name IS the acronym: "Ibec", "SIPTU"
        key = norm(ACRONYMS[s.upper()])
    else:
        key = norm(s)
        for acr, rx in _ACR_TAG.items():  # "Full Name (ACR)" / "Full Name - ACR": fold only its OWN acronym
            if rx.search(s) and norm(rx.sub(" ", s)) == norm(ACRONYMS[acr]):
                key = norm(ACRONYMS[acr])
                break
    return DIARY_REGISTER_ALIASES.get(key, key)


def surname_key(name: str | None) -> str:
    """Crude surname key: ASCII-folded last token, trailing possessive 's' stripped.

    Aligns the diary minister filename-guess ("Ryans", "Brownes") with the lobbying
    register full_name ("Eamon Ryan"). Deliberately lossy — collisions possible.
    """
    if not name:
        return ""
    toks = unicodedata.normalize("NFKD", str(name)).encode("ascii", "ignore").decode().lower().split()
    if not toks:
        return ""
    last = toks[-1]
    return last[:-1] if last.endswith("s") and len(last) > 4 else last


def main() -> int:
    setup_standalone_logging("diary_lobbying_overlap")
    for p in (ENTRIES, MENTIONS, POL):
        if not p.exists():
            log.error("missing input: %s", p)
            return 1

    entries = pl.read_parquet(ENTRIES)
    if "entry_id" not in entries.columns or "entry_class" not in entries.columns:
        log.error(
            "entries parquet lacks entry_id/entry_class — re-run diary_entry_classify.py "
            "then diary_org_match.py before this builder (they stamp those columns)."
        )
        return 1

    mentions = pl.read_parquet(MENTIONS)
    overlap = (
        mentions.filter(~pl.col("matched_org_name").is_in(list(PERSON_DENYLIST)))
        .filter(~pl.col("matched_org_name").str.contains("(?i)" + _GOV_BODY_RE.pattern))
        .join(entries.select(["entry_id", "entry_class", "department", "source_pdf_url"]), on="entry_id", how="left")
        .filter(~pl.col("entry_class").is_in(list(EXCLUDE_CLASSES)))
        .with_columns(
            # resolve the minister from the source filename (+ dept/date fallback) rather than
            # the fragile raw filename-guess, so ministers_met / corroboration count the right
            # person — recovers the ~25% of meetings the old guess left minister-less.
            pl.struct(["source_pdf_url", "department", "entry_date"])
            .map_elements(
                lambda r: resolve_minister(r["source_pdf_url"], r["department"], r["entry_date"]),
                return_dtype=pl.Utf8,
            )
            .alias("minister_resolved")
        )
        .with_columns(
            [
                pl.col("matched_org_name").map_elements(org_key, return_dtype=pl.Utf8).alias("org_nk"),
                pl.col("minister_resolved").map_elements(surname_key, return_dtype=pl.Utf8).alias("min_sk"),
                pl.col("matched_org_name").map_elements(sector_for, return_dtype=pl.Utf8).alias("sector"),
            ]
        )
    )
    if overlap.is_empty():
        log.error("no overlap rows survived the strict filters — check inputs")
        return 1

    # corroboration: (org_nk, minister surname) pairs the register says were lobbied as a Minister
    pol = pl.read_parquet(POL).with_columns(
        [
            pl.col("lobbyist_name").map_elements(org_key, return_dtype=pl.Utf8).alias("org_nk"),
            pl.col("full_name").map_elements(surname_key, return_dtype=pl.Utf8).alias("min_sk"),
        ]
    )
    pol_min = pol.filter(pl.col("position").fill_null("").str.contains("Minister"))
    lobbied_pairs = (
        pol_min.select(["org_nk", "min_sk"]).unique().with_columns(pl.lit(True).alias("lobbied_same_minister"))
    )
    returns_per_org = pol.group_by("org_nk").agg(pl.col("lobby_url").n_unique().alias("total_lobbying_returns"))

    overlap = (
        overlap.join(lobbied_pairs, on=["org_nk", "min_sk"], how="left")
        .with_columns(pl.col("lobbied_same_minister").fill_null(False))  # noqa: FBT003
        .join(returns_per_org, on="org_nk", how="left")
        .with_columns(pl.col("total_lobbying_returns").fill_null(0))
    )
    save_parquet(overlap, OUT_DETAIL)
    overlap.write_csv(OUT_DETAIL.with_suffix(".csv"))

    # org-level ranking: who met ministers most (DISTINCT meetings — explosion-safe)
    ranked = (
        overlap.group_by("matched_org_name")
        .agg(
            [
                pl.col("sector").first().alias("sector"),
                pl.col("entry_id").n_unique().alias("meetings"),
                pl.col("entry_id").filter(pl.col("match_confidence") == "high").n_unique().alias("high_conf_meetings"),
                pl.col("min_sk").filter(pl.col("min_sk") != "").n_unique().alias("ministers_met"),
                pl.col("min_sk").filter(pl.col("lobbied_same_minister")).n_unique().alias("ministers_lobbied_and_met"),
                pl.col("total_lobbying_returns").max().alias("total_lobbying_returns"),
                pl.col("entry_date").min().alias("first_meeting"),
                pl.col("entry_date").max().alias("last_meeting"),
            ]
        )
        .sort(["meetings", "ministers_met"], descending=True)
    )
    save_parquet(ranked, OUT_RANKED)
    ranked.write_csv(OUT_RANKED.with_suffix(".csv"))

    log.info(
        "OVERLAP: %d meetings | %d distinct orgs | %d distinct ministers | corroborated (lobbied+met same min): %d",
        overlap["entry_id"].n_unique(),
        overlap["matched_org_name"].n_unique(),
        overlap.filter(pl.col("min_sk") != "")["min_sk"].n_unique(),
        overlap.filter(pl.col("lobbied_same_minister")).height,
    )
    sector_summary = (
        overlap.group_by("sector")
        .agg(
            [
                pl.col("entry_id").n_unique().alias("meetings"),
                pl.col("matched_org_name").n_unique().alias("orgs"),
                pl.col("entry_id").filter(pl.col("lobbied_same_minister")).n_unique().alias("corroborated"),
            ]
        )
        .sort("meetings", descending=True)
    )
    log.info("By sector (heuristic tag):")
    for r in sector_summary.iter_rows(named=True):
        log.info(
            "  %-22s %5d meetings  %4d orgs  %4d corroborated", r["sector"], r["meetings"], r["orgs"], r["corroborated"]
        )

    log.info("Top 25 organisations by minister meetings:")
    log.info("  %-42s %8s %5s %12s %8s", "organisation", "meetings", "mins", "lobbied&met", "returns")
    for r in ranked.head(25).iter_rows(named=True):
        log.info(
            "  %-42s %8d %5d %12d %8d",
            r["matched_org_name"][:42],
            r["meetings"],
            r["ministers_met"],
            r["ministers_lobbied_and_met"],
            r["total_lobbying_returns"],
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
