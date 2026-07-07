"""
si_entity_enrichment.py

Builds the Statutory Instrument as a first-class entity: one row per SI,
sourced from the Iris Oifigiúil SI taxonomy directly — NOT gated on a bill
match. Writes data/gold/parquet/statutory_instruments.parquet.

Companion to iris_si_bill_enrichment.py. That script answers "which SIs hang
off this bill" (EXACT title matches only — ~10% of SIs, ~47% of enacted Acts);
this one treats the SI itself as the entity, so the browsable SI surface is not
gated on a thin join. The bill link is carried as one optional attribute.

DERIVES:
  - si_id                 <year>-<number zero-padded to 3>
  - si_is_eu              si_eu_relationship is not in the "no EU" set
  - si_department         canonical department key, mapped from the free-text
                          si_responsible_actor role via the curated
                          data/_meta/si_department_aliases.csv
  - si_department_label
  - bill_id / bill_short_title
                          left-joined from bill_statutory_instruments.parquet
                          where the SI was matched to an enabling bill (exact
                          title match, si_year >= 2012); null otherwise

  - si_minister_member_code / si_minister_name
                          the actual minister, resolved by joining the SI's
                          department + signing date against the ministerial
                          tenure table (data/silver/ministerial_tenure.parquet,
                          built by ministerial_tenure_build.py from Wikidata).
                          member_code is null for ministers no longer in the
                          Oireachtas.

COVERAGE FLOOR: si_year >= 2012 (full-coverage years post-backfill; the
taxonomy thins to citation noise below this — see SI_YEAR_FLOOR).
"""

from __future__ import annotations

import logging
import re

import pandas as pd

from config import DATA_DIR, GOLD_PARQUET_DIR, SILVER_DIR
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_SI_CSV = SILVER_DIR / "iris_oifigiuil" / "iris_si_taxonomy.csv"
_BILL_SI = GOLD_PARQUET_DIR / "bill_statutory_instruments.parquet"
_ALIASES = DATA_DIR / "_meta" / "si_department_aliases.csv"
_TENURE = SILVER_DIR / "ministerial_tenure.parquet"
_OUT = GOLD_PARQUET_DIR / "statutory_instruments.parquet"

# 2012 is the earliest full-coverage year after the 2026-06 bronze backfill
# (2012-2015 each carry ~500 SIs). Below 2012 the taxonomy thins to citation
# noise (2011≈65, sparse before), so the floor still trims that long tail.
SI_YEAR_FLOOR = 2012
MIN_TAXO_CONFIDENCE = 0.5

# si_eu_relationship values that carry no EU dimension.
_EU_NEGATIVE = {"none_detected", "", "nan"}

# ── Title casing ──────────────────────────────────────────────────────────────
# Many Iris PDFs render SI titles in ALL CAPS. Carried into the UI that
# becomes a wall of shouting type — the civic-newspaper register relies on
# readable case. We normalise titles that are ≥80% uppercase letters; titles
# that already mix case (rarer in the corpus but they exist) pass through.

_TITLE_LOWER = {
    "a",
    "an",
    "and",
    "as",
    "at",
    "but",
    "by",
    "for",
    "from",
    "in",
    "of",
    "on",
    "or",
    "the",
    "to",
    "with",
    "nor",
}
# Tokens we want to keep upper-case regardless of position. Drives most of
# the EU/UK/US/state-body acronym handling.
_TITLE_KEEP_UPPER = {
    # Jurisdictions / EU bodies
    "EU",
    "EC",
    "EEA",
    "EFTA",
    "US",
    "UK",
    "UN",
    "EEC",
    "ECB",
    "OECD",
    "WHO",
    "WTO",
    "NATO",
    # Irish taxes / state bodies / agencies
    "VAT",
    "PAYE",
    "PRSI",
    "USC",
    "ESB",
    "RTÉ",
    "RTE",
    "DPP",
    "HSE",
    "IBRC",
    "NAMA",
    "NTMA",
    "AGS",
    "BAI",
    "CAO",
    "IDA",
    "EI",
    "PSO",
    "TII",
    "DIT",
    # Education
    "DCU",
    "UCD",
    "TCD",
    "UCC",
    "UL",
    "GAA",
    # Tech / measurement / standards
    "GDPR",
    "ISO",
    "NUTS",
    "ETS",
    "SI",
    "IR",
    "IT",
    "AI",
    # Science / medicine / health
    "DNA",
    "RNA",
    "HIV",
    "AIDS",
    "COVID",
}


# Preamble bleed cleanup. The Iris parser tries to stop the title at known
# preamble lead-ins but the long tail of patterns is large:
#   "ORDER 2016. The Taoiseach Enda Kenny T.D., in exercise..."
#   "REGULATIONS 2018 Ms Heather Humphreys, Minister for Business..."
#   "RULES 2023 Jack Chambers, Minister of State..."
#   "ORDER In exercise of the powers conferred..."
# Rather than chase every pattern in the regex, we cut the title at the
# first occurrence of a strong preamble marker here as a safety net. Order
# matters: earliest match wins.
_PREAMBLE_CUTS = re.compile(
    # Allow comma-then-space as a valid break point too — "Name, in exercise of"
    # is a common formulation that wraps the cut marker after a comma.
    r"(?:\s+|\.\s+|,\s+)"
    r"(?:"
    # Universal preamble verb — appears as both "In exercise" (preamble
    # start) and "in exercise" (after a name+role). Case-insensitive
    # sub-pattern with (?i:...) so we don't disturb the case-sensitive
    # name alternative below.
    r"(?i:in exercise of)\b|"
    r"(?i:in these regulations)\b|"
    r"(?i:in these rules)\b|"
    r"WHEREAS\b|"
    r"Whereas\b|"
    r"The Minister\b|"
    r"The Taoiseach\b|"
    r"The Government\b|"
    r"The T[áa]naiste\b|"
    r"The Commissioners?\b|"
    r"The Authority\b|"
    r"The President\b|"
    r"The Director\b|"
    r"The Board\b|"
    r"The Council\b|"
    r"The Members\b|"
    r"Minister of State\b|"  # Bare role, no preceding "The"
    r"These Regulations\b|"
    r"This Order\b|"
    r"Copies of\b|"
    r"Under the\b|"
    r"EXPLANATORY NOTE\b|"
    r"\(This note\b|"
    r"Mr\.?\s+[A-Z]|"
    r"Ms\.?\s+[A-Z]|"
    r"Mrs\.?\s+[A-Z]|"
    r"Dr\.?\s+[A-Z]|"
    r"I,\s+[A-Z]|"
    # Bare "Firstname Surname, Minister" — captures Jack Chambers etc.
    r"[A-Z][a-z]+\s+[A-Z][A-Za-z'\-]+,\s+(?:Minister|Taoiseach|T\.D)"
    r")"
)


# SI titles overwhelmingly end with "<Kind> <Year>" — Regulations 2024,
# Order 2019, Rules 2023 etc. The body often cites other SIs by their own
# kind+year (so the LAST occurrence is usually a body citation, not the
# title's tail). We want the FIRST occurrence whose tail looks like
# preamble body — that's the actual title end.
_TITLE_TAIL = re.compile(
    r"\b(Regulations?|Order|Rules?|Scheme|Notice|"
    r"Bye[\-\s]?Laws?|Bylaws?)\s+\d{4}\b",
    flags=re.IGNORECASE,
)
# A year-suffix followed by a sentence-starter capital-word is the
# unambiguous signature of the title ending and the body beginning.
_YEAR_THEN_SENTENCE = re.compile(
    r"\b\d{4}\.?\s+"
    r"(?:The|These|This|For|In|It|Section|Whereas|I,|Pursuant|Under|"
    r"Mr|Ms|Mrs|Dr|[A-Z][a-z]+\s+[A-Z])"
    r"\b"
)


def _strip_preamble(title: str) -> str:
    """Trim everything after the SI title proper. Three ordered passes;
    earliest successful cut wins.
    1. Kind+Year tail (Regulations 2024, Order 2019, Rules 2023, ...) —
       FIRST occurrence whose remainder is non-trivial preamble. Many
       bodies cite older SIs by kind+year, so left-to-right with a
       'rest is preamble' check is what we want, not rightmost.
    2. Year + sentence-starter — catches titles like 'RULES (ORDER 4) 2019.
       The Circuit Court Rules Committee...' where the kind word isn't
       at the end.
    3. First preamble lead-in keyword (`In exercise of`, `The Minister`,
       `Mr.`, etc.) as the catch-all fallback."""
    # Pass 1: first kind+year tail whose remainder looks like preamble.
    for m in _TITLE_TAIL.finditer(title):
        end = m.end()
        rest = title[end:].strip(" .,;:")
        if not rest or rest.startswith(")"):
            continue  # trailing punctuation only — this match is the actual end
        # If the remainder contains any of the strong preamble cues, cut here.
        if _PREAMBLE_CUTS.search(" " + rest) or rest[:1].isupper():
            return title[:end]
    # Pass 2: year + capitalised sentence-starter.
    m = _YEAR_THEN_SENTENCE.search(title)
    if m is not None:
        # Cut at the year+period boundary, not the start of the next sentence.
        end = m.start() + len(re.match(r"\d{4}\.?", title[m.start() :]).group())
        return title[:end].rstrip(" .,;:")
    # Pass 3: first preamble lead-in keyword.
    m = _PREAMBLE_CUTS.search(title)
    if m is None:
        return title
    return title[: m.start()].rstrip(" .,;:")


def _normalise_si_title(title: object) -> object:
    """Clean and case-normalise an SI title. Two steps:
    1. Strip any preamble that bled through the parser stop-markers.
    2. If the result is ≥80% uppercase letters, apply title case. Otherwise
       pass through (the title is already in human casing).
    Hyphenated words split-and-recombine so 'WORK-LIFE' → 'Work-Life'.
    Known acronyms (EU, DNA, COVID, VAT, etc.) stay upper. NaN / empty
    pass through unchanged."""
    if not isinstance(title, str) or not title:
        return title
    title = _strip_preamble(title.strip())
    if not title:
        return title
    # Restore the space between adjacent bracket groups that some source notices
    # drop — "(Iran)(Human Rights)(No.2)" → "(Iran) (Human Rights) (No.2)". The
    # eISB house style separates qualifier brackets with a space; the run-together
    # form is a transcription artefact, not a real citation.
    title = re.sub(r"\)\(", ") (", title)
    letters = [c for c in title if c.isalpha()]
    if not letters:
        return title
    upper_ratio = sum(1 for c in letters if c.isupper()) / len(letters)
    if upper_ratio < 0.8:
        return title

    def case_one(word: str, is_first: bool) -> str:
        # Split leading/trailing non-alnum punctuation; case the core only.
        i = 0
        while i < len(word) and not word[i].isalnum():
            i += 1
        j = len(word)
        while j > i and not word[j - 1].isalnum():
            j -= 1
        prefix, core, suffix = word[:i], word[i:j], word[j:]
        if not core:
            return word
        # Keep upper for known acronyms.
        if core.upper() in _TITLE_KEEP_UPPER:
            return prefix + core.upper() + suffix
        # Numeric token e.g. "2024" — leave alone.
        if core.isdigit():
            return word
        # No. / NO. → "No."
        if core.upper() == "NO":
            return prefix + "No" + suffix
        lowered = core.lower()
        if (not is_first) and lowered in _TITLE_LOWER:
            return prefix + lowered + suffix

        # Hyphen-aware capitalisation: WORK-LIFE → Work-Life,
        # COVID-19 → COVID-19 (per-part keep-upper check).
        def _case_part(p: str) -> str:
            if not p:
                return p
            if p.isdigit():
                return p
            if p.upper() in _TITLE_KEEP_UPPER:
                return p.upper()
            return p[:1].upper() + p[1:]

        parts = lowered.split("-")
        return prefix + "-".join(_case_part(p) for p in parts) + suffix

    tokens = title.split()
    return " ".join(case_one(tok, i == 0) for i, tok in enumerate(tokens))


# ── Title recovery from raw_text (fallback for broken titles) ─────────────────
# _normalise_si_title works on the pre-joined `title` column. For ~1.5% of rows
# that column is malformed — the body preamble is glued on (so the ALL-CAPS head
# never gets case-normalised), multi-line PDF headers are joined with " | ", or a
# leading "S.I. No. X of YYYY." citation survives. raw_text keeps the original
# "//" line structure, so the true title is recoverable: drop a leading citation
# line, then keep lines until the preamble begins. Applied as a STRICT FALLBACK
# (see _resolve_si_title): only when the normalised title is unambiguously broken,
# and only if the recovered title is itself clean — so a good title is never
# touched (verified: 0 non-triggered rows change).
_SI_CITE_LINE = re.compile(r"^\s*S\.?\s?I\.?\s*(?:No\.?)?\s*(?:of\s+)?\d+\s+(?:of\s+)?\d{4}\.?\s*$", re.I)
_PREAMBLE_LINE = re.compile(
    r"^\s*(?:"
    r"The\s+[A-Z]"  # 'The <Capitalised>' — agent / preamble line opener
    r"|Minister\s+(?:for|of\s+State)\b"
    r"|In\s+exercise\s+of\b|WHEREAS\b|Whereas\b|I,\s+[A-Z]"
    r"|Government\s+support\b|Notice\s+is\s+hereby\b|Pursuant\s+to\b"
    r"|Rinne\s+an\s+tAire\b|[A-Z][a-z]+\s+[A-Z][A-Za-z'\-]+,\s+(?:Minister|Taoiseach|T\.D)"
    r")"
)
_PREAMBLE_INLINE = re.compile(r"\b(?:in exercise of|,\s*in exercise|Government support|Notice is hereby)\b", re.I)
# Strong, unambiguous "this title is broken" signals — deliberately NO weak cues
# ("in accordance", "hereby", trailing comma) that also appear in legitimate long
# titles, so the trigger never fires on a good title.
_TITLE_STRONG_BREAK = re.compile(
    r"\b(?:in exercise of|in the exercise of|these regulations|this order|"
    r"government support|with the concurrence|notice is hereby)\b",
    re.I,
)


def _title_caps_ratio(x: str) -> float:
    letters = [c for c in x if c.isalpha()]
    return sum(1 for c in letters if c.isupper()) / len(letters) if letters else 0.0


def _title_is_broken(x: object) -> bool:
    """True when a (normalised) title is unambiguously malformed — caps-dominant
    (un-normalised head), pipe-joined PDF lines, a body double-quote, a surviving
    'S.I. No.' citation prefix, or a hard preamble phrase."""
    if not isinstance(x, str) or not x.strip():
        return True
    return bool(
        " | " in x
        or '"' in x
        or _title_caps_ratio(x) > 0.6
        or re.match(r"^S\.?\s?[Ii]\.\s", x)
        or _TITLE_STRONG_BREAK.search(x)
    )


def _title_from_raw(raw: object) -> str | None:
    """Recover the title from raw_text's '//' line structure: drop a leading
    'S.I. No. X of YYYY.' citation line, then accumulate lines until the preamble
    starts. Returns None when nothing usable is found."""
    if not isinstance(raw, str) or not raw.strip():
        return None
    lines = [ln.strip(" .") for ln in re.split(r"\s*//\s*|\s*\|\s*", raw) if ln.strip(" .")]
    out: list[str] = []
    for i, ln in enumerate(lines):
        if i == 0 and _SI_CITE_LINE.match(ln):
            continue
        if _PREAMBLE_LINE.match(ln):
            break
        m = _PREAMBLE_INLINE.search(ln)
        if m:
            # single-line case: drop a trailing agent clause ("... The X Authority")
            head = re.sub(r"\s+The\s+[A-Z][\w'’&()\- ]+?$", "", ln[: m.start()].rstrip(" ,;")).rstrip(" ,;")
            if head:
                out.append(head)
            break
        out.append(ln)
        if len(out) >= 6:  # a real title is rarely >6 source lines
            break
    return re.sub(r"\s+", " ", " ".join(out)).strip(" .,;") or None


def _resolve_si_title(title: object, raw: object) -> object:
    """Strict-fallback title resolver: the normalised `title` column, unless it is
    unambiguously broken AND raw_text yields a title that is itself clean — in
    which case the recovered title wins. Cannot regress a good title."""
    cur = _normalise_si_title(title)
    if not _title_is_broken(cur):
        return cur
    rec = _normalise_si_title(_title_from_raw(raw))
    if isinstance(rec, str) and not _title_is_broken(rec) and rec.count("(") == rec.count(")"):
        return rec
    return cur


# ── Parent-legislation cleanup ────────────────────────────────────────────────
# The upstream extractor (iris_oifigiuil_etl_polars.py) matches every
# "<Title-Case run> Act <year>" span in the *body text*, which produces three
# classes of artefact the raw column carries into display:
#   1. PROSE LEAD-INS — the lazy match starts at a sentence capital, so "These
#      Regulations amend the Health Act 1947" / "This Order commences certain
#      provisions of the Road Traffic Act 2016" arrive whole.
#   2. ORPHAN-PAREN FRAGMENTS — "(Amendment) Act 1996" surfaces as
#      "Amendment) Act 1996" when the leading words were lost (unbalanced ')').
#   3. SUFFIX FRAGMENTS — a long name also yields its own tail, e.g.
#      "Central Treasury Services Act 2020" + "Services Act 2020".
# si_responsible_actor has tidy_actor() to neutralise the same greedy capture;
# parent_legislation had no equivalent, so this is its tidy_* sibling. Validated
# on the gold corpus: prose lead-ins 235→1 parts, orphan-paren 432→0, zero clean
# Act names damaged.
_PARENT_THIS_LEAD = re.compile(r"^(?:This|These|That)\b.*?\bthe\s+(?=[A-Z][^|]*?\bAct\s+\d{4})", re.I)
_PARENT_VERB_LEAD = re.compile(
    r"^.*?\b(?:amend(?:s|ing)?|revoke(?:s|ing)?|applies(?:\s+part\s+[ivxlc]+)?|application of|"
    r"in\s+exercise\s+of[^.]*?of|pursuant\s+to|conferred\s+by|under)\s+(?:both\s+)?the\s+(?=[A-Z])",
    re.I,
)
_PARENT_BARE_LEAD = re.compile(r"^(?:Under|Of|In|Pursuant\s+to|And)\s+the\s+(?=[A-Z])", re.I)
_PARENT_CAPS_JUNK = re.compile(r"^(?:ACTS?|AND|OR|THE)\s+(?=[A-Z])")


def _tidy_parent_part(p: str) -> str | None:
    """Clean a single parent-Act citation; return None to drop an unusable one."""
    p = re.sub(r"\s+", " ", p).strip(" .,;")
    if not p:
        return None
    p = _PARENT_CAPS_JUNK.sub("", p)
    p = _PARENT_THIS_LEAD.sub("", p)
    p = _PARENT_VERB_LEAD.sub("", p)
    p = _PARENT_BARE_LEAD.sub("", p)
    p = p.strip(" .,;")
    # More ')' than '(' → the opening words of the Act name were lost upstream;
    # the fragment is not a usable citation, so drop it rather than display it.
    if p.count(")") > p.count("("):
        return None
    if "Act" not in p:  # lost the Act token entirely → not a parent-Act cite
        return None
    return p or None


def _tidy_parent_legislation(raw: object) -> object:
    """Clean the '|'-joined parent-Act list: strip prose lead-ins, drop
    truncation fragments, then collapse word-suffix duplicates (a long name's
    own tail, e.g. 'Services Act 2020' under 'Treasury Services Act 2020').
    Order-preserving; returns None when nothing usable survives so the page
    renders no parent rather than garbage."""
    if not isinstance(raw, str) or not raw.strip():
        return raw
    parts = [c for p in raw.split("|") if (c := _tidy_parent_part(p))]
    kept: list[str] = []
    for p in parts:
        # Drop p if some other part ends with " <p>" (p is a word-suffix of it).
        if any(other != p and other.endswith(" " + p) for other in parts):
            continue
        if p not in kept:
            kept.append(p)
    return "|".join(kept) if kept else None


def load_si() -> pd.DataFrame:
    """Clean SI universe from the taxonomy — same filters as the bill matcher
    except the year floor is 2012 (vs 2018), covering the full post-backfill
    corpus."""
    df = pd.read_csv(_SI_CSV, low_memory=False)
    df = df[df["notice_category"] == "statutory_instrument"]
    df = df[~df["is_quarantined"].fillna(False).astype(bool)]
    df = df[df["si_number"].notna() & df["si_year"].notna() & df["title"].notna()]
    df["si_year"] = df["si_year"].astype(int)
    df["si_number"] = df["si_number"].astype(int)
    df = df[df["si_year"] >= SI_YEAR_FLOOR]
    df = df[df["si_taxonomy_confidence"].fillna(0) >= MIN_TAXO_CONFIDENCE]
    df["si_id"] = df["si_year"].astype(str) + "-" + df["si_number"].astype(str).str.zfill(3)
    df["issue_date"] = pd.to_datetime(df["issue_date"], errors="coerce")

    # Recover a signing office for the notices the parser left blank, and
    # capture any literally-printed signatory name. The parser's actor is kept
    # when present; recovery only fills the gaps. See recover_actor_and_signatory.
    recovered = [recover_actor_and_signatory(t) for t in df["raw_text"]]
    rec_actor = pd.Series([a for a, _ in recovered], index=df.index)
    df["si_signatory_name"] = pd.Series([s for _, s in recovered], index=df.index).replace("", pd.NA)
    existing = df["si_responsible_actor"].fillna("").astype(str).str.strip()
    df["si_responsible_actor"] = existing.where(existing != "", rec_actor).map(tidy_actor).replace("", pd.NA)

    return df.drop_duplicates(subset=["si_id"]).reset_index(drop=True)


def load_department_aliases() -> list[tuple[str, str, str]]:
    """Curated (alias, department_key, department_label), sorted longest-alias
    first so specific phrases ('higher education') beat generic ones
    ('education') in the substring scan."""
    df = pd.read_csv(_ALIASES)
    rows = [
        (str(r["alias"]).strip().lower(), str(r["department_key"]).strip(), str(r["department_label"]).strip())
        for r in df.to_dict("records")
        if str(r.get("alias", "")).strip()
    ]
    rows.sort(key=lambda t: len(t[0]), reverse=True)
    return rows


_DEPT_LEAD_RE = re.compile(r"\bminister\s+(?:for|of)\s+(.+)", re.IGNORECASE)


def _lead_department_phrase(text: str) -> str:
    """The department phrase from a role / office title: the text after
    'Minister for', truncated at the first comma — office titles list
    several departments and the first is the primary one. Strings without
    'Minister for' (e.g. 'The Taoiseach', 'The Government') pass through."""
    if not isinstance(text, str) or not text.strip():
        return ""
    m = _DEPT_LEAD_RE.search(text)
    phrase = m.group(1) if m else text
    return phrase.split(",")[0].strip()


def canonicalise_department(text, aliases: list[tuple[str, str, str]]):
    """Map a free-text role / office title to a canonical department, via the
    leading department phrase. Returns (key, label); (None, None) when blank
    or unmatched (e.g. regulators, which are not ministerial departments)."""
    phrase = _lead_department_phrase(text).lower()
    if not phrase:
        return None, None
    for alias, key, label in aliases:
        if alias in phrase:
            return key, label
    return None, None


# ── Signer-aware actor recovery ───────────────────────────────────────────────
# The parser's si_responsible_actor only fires on "The Minister for ..." plus a
# short list of named bodies, leaving ~60% of SI notices with an empty actor.
# The making office is almost always still printed in the notice body, under
# phrasings the parser misses: a signature block ("Eoghan Murphy, Minister for
# Housing"), "Tánaiste and Minister for ...", a non-ministerial maker (Revenue,
# Central Bank, a court rules committee), or a bare "... the Minister for X has
# made". We recover those here from raw_text — NO PDF re-parse.
#
# ACCURACY GUARD: a notice frequently names a *consenting* minister ("with the
# consent of the Minister for Finance") who did NOT make the SI. We delete those
# clauses before extraction so a consenter is never mistaken for the signer.

# Consent / concurrence clauses — stripped before office extraction.
_CONSENT_CLAUSE_RE = re.compile(
    r"(?:with\s+the\s+)?(?:consent|concurrence|approval|sanction|agreement)\s+of\s+"
    r"(?:the\s+)?(?:Tánaiste\s+and\s+)?Minister(?:\s+of\s+State)?\s+(?:for|of)\s+[^,.;/\n]+",
    re.IGNORECASE,
)

# Literal signature block: a personal name (1–3 capitalised tokens), optional
# T.D., then the office. Captures the PRINTED signatory — a stronger fact than
# the tenure-inferred minister name, and the office it pins is the signer's.
_SIGNATORY_RE = re.compile(
    r"([A-ZÁÉÍÓÚ][A-Za-zÁÉÍÓÚáéíóú'’\-]+(?:\s+[A-ZÁÉÍÓÚ][A-Za-zÁÉÍÓÚáéíóú'’\-]+){0,2})"
    r"(?:\s*,?\s*T\.?D\.?)?,\s*"
    r"(Minister\s+(?:of\s+State\s+)?(?:for|at)\b[^,.;/\n]*)"
)

# A ministerial office anywhere in the (de-consented) body. group(1) = the
# "Minister of State ..." dept, group(2) = the "Minister for ..." dept.
_MIN_OFFICE_RE = re.compile(
    r"(?:The\s+)?(?:Tánaiste\s+and\s+)?Minister\s+of\s+State\s+(?:at\s+the\s+Department\s+of|for)\s+([^,.;/\n]+)"
    r"|(?:The\s+)?(?:Tánaiste\s+and\s+)?Minister\s+for\s+([^,.;/\n]+)",
    re.IGNORECASE,
)

# Non-ministerial makers — these populate the office but resolve to no person
# (they are not departments in the ministerial-tenure table, which is correct:
# the SI was made by a body, not a minister).
_BODY_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bRevenue Commissioners\b", re.I), "The Revenue Commissioners"),
    (re.compile(r"\bCentral Bank of Ireland\b|\bCentral Bank\b", re.I), "The Central Bank of Ireland"),
    (
        re.compile(r"\bSuperior Court Rules?\b|\bRules?\s+of\s+the\s+Superior\s+Courts?\b", re.I),
        "Superior Courts Rules Committee",
    ),
    (re.compile(r"\bCircuit Court Rules?\b", re.I), "Circuit Court Rules Committee"),
    (re.compile(r"\bDistrict Court Rules?\b", re.I), "District Court Rules Committee"),
]

# Trim a captured department phrase at verb/clause boundaries the dept-capture
# over-runs into ("Minister for Foreign Affairs and Trade has made ..."). We do
# NOT cut on "and" — it is part of real titles (Foreign Affairs and Trade).
_DEPT_TAIL_RE = re.compile(r"\s+(?:has|have|hereby|in\s+exercise|by|under|shall|may|after)\b.*$", re.I)
_NAME_LEAD_RE = re.compile(r"^(?:Mr|Ms|Mrs|Dr|Deputy|Senator)\s+", re.I)
_NAME_TAIL_RE = re.compile(r"\s*,?\s*T\.?D\.?\s*$", re.I)


def _clean_name(name: str) -> str:
    name = _NAME_LEAD_RE.sub("", name.strip())
    name = _NAME_TAIL_RE.sub("", name).strip(" ,")
    return re.sub(r"\s+", " ", name)


def _office_from_minister_phrase(office: str) -> str:
    """Normalise a captured 'Minister ...' fragment to a clean office string,
    trimming any trailing verb clause the greedy dept capture ran into."""
    office = re.sub(r"\s+", " ", office).strip()
    office = _DEPT_TAIL_RE.sub("", office).strip(" ,;-")
    # "Minister for State" is not an office title — the junior office is
    # "Minister of State". Normalise the misrendering.
    office = re.sub(r"\bMinister for State\b", "Minister of State", office, flags=re.IGNORECASE)
    return f"The {office}" if not office.lower().startswith("the ") else office


def tidy_actor(actor):
    """Normalise a signing-office string for display, fixing pre-existing parser
    artifacts as well as recovered values: strip a name/'TD' prefix from
    signature-form actors ('Canney, Minister for ...' → 'The Minister for ...')
    and trim verb run-ons ('The Minister for Justice has made ...' → 'The
    Minister for Justice'). Non-ministerial bodies pass through untouched except
    for tail trimming. The canonical department token always survives, so
    department→minister resolution is unaffected."""
    if not isinstance(actor, str) or not actor.strip():
        return actor
    m = re.search(r"(Minister\s+(?:of\s+State\s+)?(?:for|at)\b.*)", actor, re.IGNORECASE)
    if m:
        return _office_from_minister_phrase(m.group(1))
    return _DEPT_TAIL_RE.sub("", actor).strip(" ,;-")


def recover_actor_and_signatory(raw_text) -> tuple[str, str]:
    """(actor, signatory_name) recovered from a notice body.

    actor — a clean office string ("The Minister for X", "The Revenue
    Commissioners", ...) or "" if none found. signatory_name — the literally
    printed signer name ("Eoghan Murphy") or "" if no signature block is
    present. Both are accuracy-first: consenting ministers are excluded."""
    if not isinstance(raw_text, str) or not raw_text.strip():
        return "", ""

    actor = ""
    signatory = ""

    # 1) Signature block — strongest signal; pins both name and signing office.
    msig = _SIGNATORY_RE.search(raw_text)
    if msig:
        signatory = _clean_name(msig.group(1))
        actor = _office_from_minister_phrase(msig.group(2))

    # 2) Strip consenting-minister clauses so they cannot be picked up below.
    body = _CONSENT_CLAUSE_RE.sub(" ", raw_text)

    # 3) Any remaining ministerial office in the de-consented body.
    if not actor:
        mo = _MIN_OFFICE_RE.search(body)
        if mo:
            if mo.group(1):  # Minister of State
                actor = _office_from_minister_phrase(f"Minister of State for {mo.group(1)}")
            else:
                actor = _office_from_minister_phrase(f"Minister for {mo.group(2)}")

    # 4) Non-ministerial maker bodies (no person resolution).
    if not actor:
        for rx, label in _BODY_PATTERNS:
            if rx.search(raw_text):
                actor = label
                break

    return actor, signatory


def load_bill_links() -> pd.DataFrame:
    """One row per si_id with its matched enabling bill, from the companion
    bill-matching enrichment. Missing file → empty frame (bill link stays
    null everywhere)."""
    if not _BILL_SI.exists():
        logger.warning(
            "bill_statutory_instruments.parquet not found — bill link will be null for every SI: %s", _BILL_SI
        )
        return pd.DataFrame(columns=["si_id", "bill_id", "bill_short_title"])
    df = pd.read_parquet(_BILL_SI)
    return df[["si_id", "bill_id", "bill_short_title"]].drop_duplicates(subset=["si_id"]).reset_index(drop=True)


def load_ministerial_offices() -> list[tuple[str, str | None, str, pd.Timestamp, pd.Timestamp]]:
    """Senior-minister tenure spans, as tuples of
    (department_key, member_code, minister_name, start, end).

    Sourced from data/silver/ministerial_tenure.parquet — built by
    ministerial_tenure_build.py from Wikidata, covering every Irish
    government since 2016. An open (null) end date — a serving minister —
    is treated as far-future so the date-window join works. member_code is
    null for ministers no longer in the Oireachtas (their SIs resolve to a
    name, not a clickable profile)."""
    if not _TENURE.exists():
        logger.warning(
            "ministerial_tenure.parquet not found — minister person "
            "will be null for every SI; run "
            "ministerial_tenure_build.py first: %s",
            _TENURE,
        )
        return []
    df = pd.read_parquet(_TENURE)
    far_future = pd.Timestamp("2099-12-31")
    out: list[tuple[str, str | None, str, pd.Timestamp, pd.Timestamp]] = []
    for r in df.to_dict("records"):
        start = pd.to_datetime(r.get("start_date"), errors="coerce")
        if pd.isna(start):
            continue
        end = pd.to_datetime(r.get("end_date"), errors="coerce")
        code = r.get("member_code")
        code = None if (code is None or (isinstance(code, float) and pd.isna(code))) else str(code)
        out.append(
            (str(r["department_key"]), code, str(r["minister_name"]), start, far_future if pd.isna(end) else end)
        )
    return out


def resolve_minister(dept_key, signed_date, offices):
    """The minister who held `dept_key` on `signed_date`. Returns
    (member_code, member_name); (None, None) when the SI has no department,
    no date, or falls outside every known office span (e.g. pre-2024 SIs —
    flattened_members has no historical tenures)."""
    if not dept_key or signed_date is None or pd.isna(signed_date):
        return None, None
    sd = pd.Timestamp(signed_date)
    hits = [o for o in offices if o[0] == dept_key and o[3] <= sd <= o[4]]
    if not hits:
        return None, None
    # A reshuffle on the signing date could yield two holders — take the one
    # whose tenure started most recently.
    hits.sort(key=lambda o: o[3], reverse=True)
    return hits[0][1], hits[0][2]


def run() -> dict:
    if not _SI_CSV.exists():
        raise SystemExit(f"SI taxonomy not found: {_SI_CSV}")
    if not _ALIASES.exists():
        raise SystemExit(f"department alias table not found: {_ALIASES}")

    si = load_si()
    aliases = load_department_aliases()
    links = load_bill_links()
    offices = load_ministerial_offices()

    dept = si["si_responsible_actor"].apply(
        lambda a: pd.Series(canonicalise_department(a, aliases), index=["si_department", "si_department_label"])
    )
    si = pd.concat([si, dept], axis=1)

    # Resolve the SI's department + signing date to the minister who held
    # that office then. Current-government coverage only — see
    # load_ministerial_offices.
    minister = si.apply(
        lambda r: pd.Series(
            resolve_minister(r["si_department"], r["issue_date"], offices),
            index=["si_minister_member_code", "si_minister_name"],
        ),
        axis=1,
    )
    si = pd.concat([si, minister], axis=1)

    # A printed signatory is ground truth. Where it contradicts the tenure-
    # inferred senior minister — typically a Minister of State signing, whom the
    # senior-minister tenure table cannot resolve — suppress the inference so we
    # never show, or link to, the wrong person; the printed name is surfaced
    # instead. Where they agree (a senior minister signing under their own
    # name), the inference and its clickable profile link are kept.
    def _surname(n) -> str:
        return str(n).strip().lower().split()[-1] if isinstance(n, str) and n.strip() else ""

    contradicts = si["si_signatory_name"].notna() & (
        si["si_minister_name"].isna() | (si["si_signatory_name"].map(_surname) != si["si_minister_name"].map(_surname))
    )
    si.loc[contradicts, ["si_minister_member_code", "si_minister_name"]] = None

    si = si.merge(links, on="si_id", how="left")

    out = pd.DataFrame(
        {
            "si_id": si["si_id"],
            "si_year": si["si_year"],
            "si_number": si["si_number"],
            "si_title": [_resolve_si_title(t, r) for t, r in zip(si["title"], si["raw_text"], strict=True)],
            # NB: this is the Iris Oifigiúil *publication* date (the gazette
            # issue's front-page date), used as a proxy for the signing/made
            # date, which the notices rarely print. Differs by ~5 days (median);
            # late-Dec instruments publish in the following January. The column
            # name is legacy — the UI labels it "Published in Iris".
            "si_signed_date": si["issue_date"].dt.date,
            "si_operation": si["si_operation_primary"],
            "si_operation_flags": si["si_operation_flags"],
            "si_form": si["si_form"],
            "si_eu_relationship": si["si_eu_relationship"],
            "si_is_eu": ~si["si_eu_relationship"].astype(str).str.lower().isin(_EU_NEGATIVE),
            "si_policy_domain": si["si_policy_domain_primary"],
            "si_policy_domains_all": si["si_policy_domains"],
            "si_responsible_actor": si["si_responsible_actor"],
            "si_signatory_name": si["si_signatory_name"],
            "si_department": si["si_department"],
            "si_department_label": si["si_department_label"],
            "si_minister_member_code": si["si_minister_member_code"],
            "si_minister_name": si["si_minister_name"],
            "si_parent_legislation": si["si_parent_legislation"].map(_tidy_parent_legislation),
            "bill_id": si["bill_id"],
            "bill_short_title": si["bill_short_title"],
            "eisb_url": si["eisb_url"],
            "iris_source_pdf": si["source_file"],
            "si_taxonomy_confidence": si["si_taxonomy_confidence"],
        }
    )

    # Atomic write (services.parquet_io): a crash mid-write can no longer corrupt
    # the canonical SI gold — the reason statutory_instruments.parquet.bak existed.
    save_parquet(out, _OUT)

    total = len(out)
    actor_known = int(si["si_responsible_actor"].notna().sum())
    signatory_printed = int(out["si_signatory_name"].notna().sum())
    dept_known = int(out["si_department"].notna().sum())
    minister_named = int(out["si_minister_name"].notna().sum())
    minister_coded = int(out["si_minister_member_code"].notna().sum())
    bill_linked = int(out["bill_id"].notna().sum())
    eu_count = int(out["si_is_eu"].sum())
    domain_known = int(out["si_policy_domain"].notna().sum())

    summary = {
        "total_sis": total,
        "year_range": f"{int(out['si_year'].min())}-{int(out['si_year'].max())}",
        "actor_present": actor_known,
        "signatory_printed": signatory_printed,
        "department_known": dept_known,
        "minister_named": minister_named,
        "minister_coded": minister_coded,
        "bill_linked": bill_linked,
        "domain_known": domain_known,
        "eu_driven": eu_count,
    }
    logger.info("statutory_instruments: %d SIs (%s)", total, summary["year_range"])
    logger.info("  responsible_actor present : %d (%.0f%%)", actor_known, 100 * actor_known / total)
    logger.info(
        "  department canonicalised  : %d (%.0f%%)  — of actor-present: %.0f%%",
        dept_known,
        100 * dept_known / total,
        100 * dept_known / actor_known if actor_known else 0,
    )
    logger.info(
        "  minister resolved (name)  : %d (%.0f%%)  — of department-known: %.0f%%",
        minister_named,
        100 * minister_named / total,
        100 * minister_named / dept_known if dept_known else 0,
    )
    logger.info(
        "  minister linked (profile) : %d (%.0f%%)  — sitting members only",
        minister_coded,
        100 * minister_coded / total,
    )
    logger.info("  enabling bill linked      : %d (%.0f%%)", bill_linked, 100 * bill_linked / total)
    logger.info("  policy domain known       : %d (%.0f%%)", domain_known, 100 * domain_known / total)
    logger.info("  EU-driven                 : %d (%.0f%%)", eu_count, 100 * eu_count / total)
    return summary


if __name__ == "__main__":
    try:
        from services.logging_setup import setup_logging

        setup_logging()
    except Exception:
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
