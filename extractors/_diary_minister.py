"""Canonical minister resolution for the ministerial-diary chain.

The minister attached to a diary entry is a pure function of the SOURCE FILE
(its filename) plus, for the minority of files whose name carries no surname, the
department + entry date. It needs no PDF text — so both the sandbox extractor and
the gold promotion derive it here, keeping one source of truth.

Why this exists (bug, 2026-06-20): the old derivation was a single regex
``minister[-_ ]+([a-z]+)[-_ ]+diary`` plus a crude trailing-'s' strip. It:
  * missed every multi-token name — ``minister-darragh-obriens-diary`` (O'Brien,
    1,968 Housing meetings), ``minister-breen-s-diary`` (the split possessive);
  * missed ``…-Calendar`` files (Browne) because it required the literal "diary";
  * left ~3,150 meetings (Health, Justice 2020, Education 2025) with NO minister,
    so they vanished from the "By minister" browse;
  * mangled the names it DID catch — Humphreys→"Humphrey", Cummins→"Cummin",
    McGrath→"Mcgrath", McEntee→"Mcentee", O'Callaghan→"Ocallaghan".

Resolution order:
  1. SURNAME_CANON — the curated, verifiable map from the surname token that
     appears in a diary/calendar filename to its correct display name (apostrophes,
     Mc-casing, possessive vs. genuine trailing 's' — "Cummins" is the surname,
     "obriens" is the possessive of O'Brien). This is the _meta-CSV-style
     hand-curated source of truth ([[project_curated_meta_reference_files]]).
  2. DEPT_DATE_RULES — for files whose name carries no surname (generic
     "Ministers_Diary_-_October_2022.pdf" / month-only "April_2025.pdf"), the
     senior minister is fixed by department + date. Each rule is a public fact
     verified against who_was_minister (Donnelly=Health 2020-06-27..2025-01-23,
     McEntee=Justice 2020, McEntee=Education 2025-).
  3. None — genuinely unattributable (e.g. an un-named Minister-of-State diary).
     The meeting still appears in search / by-organisation; it is just not rolled
     up under a named minister rather than being silently dropped.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path

# ── 1. filename surname token → canonical display name ──────────────────────────────────
# Keys are the lower-case token that sits immediately before "diary"/"calendar" in the
# filename (possessive 's' included, since that is how gov.ie names the files). Every token
# observed across the 278-file corpus is mapped; unknown future tokens fall back to a
# best-effort possessive strip in _normalise_token().
SURNAME_CANON: dict[str, str] = {
    "burke": "Burke",
    "burkes": "Burke",
    "obriens": "O'Brien",
    "obrien": "O'Brien",
    "calleary": "Calleary",
    "callearys": "Calleary",
    "humphrey": "Humphreys",
    "humphreys": "Humphreys",
    "martin": "Martin",
    "martins": "Martin",
    "dillon": "Dillon",
    "dillons": "Dillon",
    "cummins": "Cummins",  # John Cummins — NOT a possessive; do not strip to "Cummin"
    "english": "English",
    "breen": "Breen",
    "breens": "Breen",
    "troy": "Troy",
    "troys": "Troy",
    "browne": "Browne",
    "brownes": "Browne",
    "brown": "Browne",  # truncated/variant spelling of Browne in some filenames
    "varadkar": "Varadkar",
    "varadkars": "Varadkar",
    "halligan": "Halligan",
    "halligans": "Halligan",
    "higgins": "Higgins",  # Emer Higgins — NOT a possessive
    "moynihan": "Moynihan",
    "moynihans": "Moynihan",
    "ryan": "Ryan",
    "ryans": "Ryan",
    "coveney": "Coveney",
    "coveneys": "Coveney",
    "mcentee": "McEntee",
    "mcentees": "McEntee",
    "richmond": "Richmond",
    "richmonds": "Richmond",
    "mcgrath": "McGrath",
    "mcgraths": "McGrath",
    "fitzgerald": "Fitzgerald",
    "fitzgeralds": "Fitzgerald",
    "brophy": "Brophy",
    "brophys": "Brophy",
    "collins": "Collins",  # Niall Collins — NOT a possessive
    "harris": "Harris",  # Simon Harris — NOT a possessive
    "naughton": "Naughton",
    "naughtons": "Naughton",
    "mcconalogue": "McConalogue",
    "mcconalogues": "McConalogue",
    "ocallaghan": "O'Callaghan",
    "ocallaghans": "O'Callaghan",
    "ross": "Ross",
    "chambers": "Chambers",
    "donohoe": "Donohoe",
    "donohoes": "Donohoe",
    "smyth": "Smyth",
    "smyths": "Smyth",
}

# Tokens that are role words / noise, never a surname.
_STOP = {
    "minister",
    "ministers",
    "of",
    "state",
    "mos",
    "tanaiste",
    "tnaiste",
    "taoiseach",
    "taoiseachs",
    "the",
    "and",
    "to",
    "diary",
    "calendar",
    "q",
    "q1",
    "q2",
    "q3",
    "q4",
    "s",
    "tds",
    "td",
}
_MONTHS = {
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
    "jan",
    "feb",
    "mar",
    "apr",
    "jun",
    "jul",
    "aug",
    "sep",
    "sept",
    "oct",
    "nov",
    "dec",
}

# ── 2. generic-filename fallback: (department, canonical name, start, end | None) ────────
# Fires only when the filename carries no surname. Each is a verifiable public fact
# (who_was_minister); end=None means still in office.
DEPT_DATE_RULES: list[tuple[str, str, date, date | None]] = [
    ("HEALTH", "Donnelly", date(2020, 6, 27), date(2025, 1, 23)),
    ("JUSTICE", "McEntee", date(2020, 6, 27), date(2021, 4, 27)),
    ("EDUCATION", "McEntee", date(2025, 1, 23), None),
    # Finance publishes generic "April.pdf" calendar exports with no surname in the name, and a
    # single minister's collection page hosts content spanning predecessors — so attribute by date.
    ("FINANCE", "McGrath", date(2022, 12, 17), date(2024, 6, 26)),
    ("FINANCE", "Chambers", date(2024, 6, 26), date(2025, 1, 23)),
    ("FINANCE", "Donohoe", date(2025, 1, 23), date(2025, 11, 18)),
    ("FINANCE", "Harris", date(2025, 11, 18), None),
]


def _normalise_token(tok: str) -> str:
    """Best-effort display name for a surname token not in SURNAME_CANON (future ministers).
    Strips a possessive trailing 's' (len>4, not '-ss') and title-cases."""
    t = tok
    if len(t) > 4 and t.endswith("s") and not t.endswith("ss"):
        t = t[:-1]
    return t[:1].upper() + t[1:]


def minister_from_filename(file_url_or_name: str | None) -> str | None:
    """Canonical minister display name parsed from a diary/calendar filename, or None
    if the filename carries no surname (generic 'Ministers Diary' / month-only files)."""
    if not file_url_or_name:
        return None
    name = Path(str(file_url_or_name).split("?")[0]).name.lower()
    name = re.sub(r"\.pdf$", "", name)
    tokens = [t for t in re.split(r"[^a-z]+", name) if t]
    # locate the diary/calendar keyword; the surname is the last real name token before it
    idx = next((tokens.index(k) for k in ("diary", "calendar") if k in tokens), None)
    candidates = tokens[:idx] if idx is not None else tokens
    names = [t for t in candidates if t not in _STOP and t not in _MONTHS]
    if not names:
        return None
    key = names[-1]
    return SURNAME_CANON.get(key) or _normalise_token(key)


def _coerce_date(value: object) -> date | None:
    if value is None:
        return None
    if isinstance(value, datetime):  # also catches pandas.Timestamp (a datetime subclass)
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value)[:10])
    except ValueError:
        return None


def minister_from_dept_date(department: str | None, entry_date: object) -> str | None:
    """Senior-minister fallback for files whose name carries no surname."""
    d = _coerce_date(entry_date)
    if not department or d is None:
        return None
    for dept, who, start, end in DEPT_DATE_RULES:
        if department == dept and d >= start and (end is None or d < end):
            return who
    return None


def resolve_minister(file_url_or_name: str | None, department: str | None, entry_date: object) -> str | None:
    """The single entry point: filename surname first, then department+date fallback."""
    return minister_from_filename(file_url_or_name) or minister_from_dept_date(department, entry_date)
