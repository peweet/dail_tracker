"""Public Appointments enrichment — Iris-derived gold step (iris_refresh step 5).

Builds the on-mission appointments entity from the Iris `public_appointment`
notices: state boards/agencies, special advisers, and judicial appointments
(military commissions excluded). Translation of the 58% Irish-language notices is
by CURATED MAPPING, not MT — the notices are formulaic constitutional/statutory
templates and many carry the English inline in parentheses. Proper nouns
(person, body) are extracted and preserved, never translated.

Reads  : data/silver/iris_oifigiuil/iris_notice_events_clean.csv
Writes : data/gold/parquet/public_appointments.parquet (when --write)

Row-wise Python extraction (n~1.2k, instant) for readability. Vectorise into
the ETL once the shape is agreed.
"""
from __future__ import annotations

import argparse
import re
import sys

import polars as pl

from config import GOLD_PARQUET_DIR, SILVER_DIR
from paths import PROJECT_ROOT as _ROOT

sys.path.insert(0, str(_ROOT))

_SRC = SILVER_DIR / "iris_oifigiuil" / "iris_notice_events_clean.csv"
_OUT = GOLD_PARQUET_DIR / "public_appointments.parquet"

# ── Curated Irish→English maps (the dozen recurring forms) ──────────────────────
COURTS = {
    "ARD-CHÚIRT": "High Court", "AN ARD-CHÚIRT": "High Court",
    "CÚIRT UACHTARACH": "Supreme Court", "CHÚIRT UACHTARACH": "Supreme Court",
    "CÚIRT ACHOMHAIRC": "Court of Appeal", "CHÚIRT ACHOMHAIRC": "Court of Appeal",
    "CÚIRT ARCHOMHAIRC": "Court of Appeal", "CHÚIRT ACHOMAIRC": "Court of Appeal",
    "CÚIRT CHUARDA": "Circuit Court", "CHÚIRT CHUARDA": "Circuit Court",
    "CÚIRT DÚICHE": "District Court", "CHÚIRT DÚICHE": "District Court",
}
ROLES = {
    "BREITHEAMH": "Judge", "BREITHIÚNA": "Judges", "BHREITHEAMH": "Judge", "BHREITHIÚNA": "Judges",
    "UACHTARÁN": "President", "CATHAOIRLEACH": "Chairperson", "CHATHAOIRLEACH": "Chairperson",
    "STIÚRTHÓIR": "Director", "GHNÍOMHAIREACHT": "Director",
    "COMHALTA": "Member", "CHOMHALTA": "Member", "BHALL": "Member", "BALL": "Member",
    "COMHAIRLEOIR SPEISIALTA": "Special adviser", "CHOMHAIRLEOIR SPEISIALTA": "Special adviser",
}
# Recurring Irish-only body names → English (extend as coverage demands).
BODIES = {
    # Broadcasters / media regulators
    "RADIÓ TEILIFÍS ÉIREANN": "RTÉ",
    "COIMISIÚN NA MEÁN": "Coimisiún na Meán",
    # Environmental / planning
    "GNÍOMHAIREACHT UM CHAOMHNÚ COMHSHAOIL": "Environmental Protection Agency",
    "AN BORD PLEANÁLA": "An Bord Pleanála",
    # Defence / Garda
    "ÓGLAIGH NA HÉIREANN": "Defence Forces",
    "AN GARDA SÍOCHÁNA": "An Garda Síochána",
    # Irish-language statutory bodies
    "ÚDARÁS NA GAELTACHTA": "Údarás na Gaeltachta",
    "BORD NA GAEILGE": "Bord na Gaeilge",
    "FORAS NA GAEILGE": "Foras na Gaeilge",
    # Law / standards / regulators
    "COIMISIÚN UM ATHCHÓIRIÚ AN DLÍ": "Law Reform Commission",
    "AN COIMISIÚN TOGHCHÁIN": "Electoral Commission",
    "AN COIMISIÚN UM CHOSAINT SONRAÍ": "Data Protection Commission",
    "AN COIMISIÚN UM SCRÚDUITHE STÁIT": "State Examinations Commission",
    "COIMISIÚN NA SCRÚDUITHE STÁIT": "State Examinations Commission",  # preposition variant
    "AN COIMISIÚN UM CHAIDHDÉAIN IN OIFIGÍ POIBLÍ": "Standards in Public Office Commission",
    "ÚDARÁS RIALÁLA SEIRBHÍSÍ DLÍ": "Legal Services Regulatory Authority",
    "ÚDARÁS PÓILÍNEACHTA": "Policing Authority",
    "ÚDARÁS CRAOLACHÁIN NA HÉIREANN": "Broadcasting Authority of Ireland",
    "FORAS TAIGHDE AR OIDEACHAS": "Educational Research Centre",
    "AN COIMISIÚN UM IOMAÍOCHT": "Competition and Consumer Protection Commission",
    "AN BORD ACHOMHAIRC UM SHEIRBHÍSÍ MAOINE": "Financial Services Appeals Board",
    "AN BORD ACHOMHAIRC UM SHLÁNDÁIL": "Social Welfare Appeals Office",
    "AN t-ÚDARÁS UM CHOSAINT IASCAIGH MHARA": "Sea-Fisheries Protection Authority",
    "AN T-ÚDARÁS UM CHOSAINT IASCAIGH MHARA": "Sea-Fisheries Protection Authority",  # caps variant
    # State agencies / boards
    "AN CHOMHAIRLE CHOMHAIRLEACH UM ATHRÚ AERÁIDE": "Climate Change Advisory Council",
    "AN tÚDARÁS UM ARD-OIDEACHAS": "Higher Education Authority",
    "RÁSAÍOCHT CON ÉIREANN": "Greyhound Racing Ireland",
    "CÓRAS IOMPAIR ÉIREANN": "Córas Iompair Éireann",  # CIÉ — keeps Irish acronym
    "BORD IASCAIGH MHARA": "Bord Iascaigh Mhara",  # BIM
    "BORD BIA": "Bord Bia",
    # Note: AIRE STÁIT A CHEAPADH / AN tORDÚ CORRTHOGHCHÁIN are order TITLES,
    # not bodies — handled via _TITLE_NOT_BODY rather than added here.
}

# Title first-segments that are preamble/headers, not a body — force the
# body to be recovered from the notice text instead.
_TITLE_NOT_BODY = re.compile(
    r"(?i)^\s*(i bhfeidhmiú|ag gníomhú|in exercise of|in accordance with|pursuant to|"
    r"tá an|do rinne|the minister|the government|notice is|"
    # Order TITLES that look body-shaped but describe the legal instrument,
    # not the body being appointed to: "Appointment of Ministers of State",
    # "Seanad by-election Order", "Attorney General Appointment".
    r"aire stáit a cheapadh|airí stáit a cheapadh|an tard-aighne a cheapadh|"
    r"an tordú corrthoghchán|an tordu corrthoghchán|"
    r"an tordú um an gcoinbhinsiún|an tordu um an gcoinbhinsiún)"
)

_MIL_RE = re.compile(r"(?i)ÓGLAIGH NA hÉIREANN|DEFENCE FORCES\b|\bARMY\b|LIEUTENANT|COMMISSIONED OFFICER")
_CONTAM_RE = re.compile(
    r"(?i)\bMORTGAGE\b|PROPERTY FINANCE|\bRECEIVER\b|\bPART 10A\b|RECORD NO|IN THE MATTER OF|"
    r"provisional (administrator|liquidator)|presented to the high court|petition was|"
    r"BYE-?LAW|appointed stands|taxi regulations|agreements to which ireland|municipal district|"
    r"candidates elected|general election|returning officer|corrthoghch|by-election|panel members\) act|"
    r"supplemental provisions|juvenile business|sittings of the|court business|district court area"
)
_BANKRUPT_RE = re.compile(r"(?i)\bA BANKRUPT\b|ADJUDICATED BANKRUPT|BANKRUPT IN MAIN PROCEEDINGS")

_JUDICIAL_RE = re.compile(r"(?i)BREITHE|JUDGE OF|CHÚIRT|CÚIRT|\bCOURT\b|JUSTICE OF")
_APPT_VERB_EN = re.compile(r"(?i)\b(re-?appointed|appointed|has appointed|have this day)\b")
_APPT_VERB_GA = re.compile(r"(?i)\ba (cheapadh|athcheapadh|cheap|fhoirceannadh|bhuansannadh|bhuanshannadh)\b")
_AS_EN = re.compile(r"(?i)\b(?:re-)?appointed\s+([A-ZÁÉÍÓÚ][A-Za-zÁÉÍÓÚáéíóú.'\- ]+?)\s+as\b")
_SA_PAREN = re.compile(r"\((Appointment of Special Advisers?[^)]*)\)")
_PORTFOLIO_EN = re.compile(r"(?i)\(Appointment of Special Advisers?\s*\(([^)]*)\)")

_NAME_STOP = re.compile(
    r"(?i)rialtas|uachtar|\baire\b|minister|government|department|roinn|chúirt|cúirt|court|"
    r"bord|board|\bact\b|acht|effect|éifeacht|tar éis|powers|conferred|period|term|"
    r"secretary|rúnaí|baile|dublin|foilseach|ordú|order|pursuant|section|alt\b|\[|"
    r"member|chairperson|\bchair\b|director|comhalta|\bball\b|stiúrthóir|cathaoirleach|breithe|following|"
    r"signed|dated|\beffect\b|office|chief|general\b|"
    r"regulations|\bnotice\b|appointment|members|\bstate\b|tribunal|commission|authority|agency|"
    r"persons|follows|university|institute|\bcollege\b|authorisation|instrument|behalf|"
    r"january|february|march|april|\bmay\b|june|july|august|september|october|november|december|"
    r"eanáir|feabhra|márta|aibreán|bealtaine|meitheamh|iúil|lúnasa|fómhair|samhain|nollaig"
)


def _lines(text: str) -> list[str]:
    return [s.strip() for s in str(text).replace(" // ", "\n").split("\n") if s.strip()]


def looks_like_name(s: str) -> bool:
    if not s or len(s) > 60:
        return False
    if re.search(r"\d", s):  # names carry no digits — kills date/term/record lines
        return False
    if _NAME_STOP.search(s):
        return False
    if "(" in s:  # role/translation parentheticals, e.g. "(Judge of the Circuit Court)"
        return False
    if not any(len(w) >= 3 for w in re.findall(r"[A-Za-zÁÉÍÓÚáéíóú]+", s)):
        return False  # rejects bare post-nominals/abbreviations like "AS", "TD"
    words = s.split()
    if not (1 <= len(words) <= 6):
        return False
    caps = sum(1 for w in words if w[:1].isupper())
    return caps >= 1 and bool(re.search(r"[A-Za-zÁÉÍÓÚ]", s))


def appointing_authority(t: str, title: str = "") -> str:
    u = t.upper()
    if "UACHTARÁN" in u or "PRESIDENT" in u:
        return "President"
    if re.search(r"(?i)an rialtas|the government|chomhairle an rialtais", t):
        return "Government"
    if re.search(r"(?i)\bthe minister\b|\ban aire\b|minister for\b|minister of state|"
                 r"powers conferred on the minister|aire stáit|ag an aire|by the minister", t):
        return "Minister"
    # Department-header notices with no explicit appointer are ministerial acts.
    if re.match(r"(?i)\s*(department of|an roinn)\b", title):
        return "Minister"
    return "Unknown"


def appointment_type(subtype: str, title: str, t: str) -> str:
    if _MIL_RE.search(t):
        return "military"
    if subtype == "special_adviser_appointment":
        return "special_adviser"
    if _JUDICIAL_RE.search(title) or re.search(r"(?i)mar bhreithe|a cheapadh.{0,40}bhreithe", t):
        return "judicial"
    return "state_board"


def _clean_name_line(s: str) -> str:
    s = re.sub(r"^\(?\d+\)?[.\)]?\s*", "", s)  # strip "(1)" / "1." / "1)" enumeration
    s = re.sub(r"(?i)^(?:and\s+)?(?:re-)?appointed\s+", "", s)  # strip leaked verb prefix
    s = s.lstrip("•*-– ").strip()
    if s.endswith(":"):  # header lines ("Maynooth University:") aren't names
        return ""
    return s.strip(" .,")


def _name_from_line(s: str) -> str | None:
    """A clean name from a candidate line, including the 'Name 23 January 2024 -…'
    case where the appointee and their term share one line, and 'Name, Role/Org…'
    where a role or organisation trails the name after a comma."""
    c = _clean_name_line(s)
    if looks_like_name(c):
        return c
    m = re.match(r"([A-ZÁÉÍÓÚ][A-Za-zÁÉÍÓÚáéíóú.'\- ]{2,40}?)\s+\d", c)  # name then a date/number
    if m and looks_like_name(m.group(1).strip(" .,")):
        return m.group(1).strip(" .,")
    pre = c.split(",")[0].strip(" .")  # "Sheila Nunan, Member of…" -> "Sheila Nunan"
    if pre != c and looks_like_name(pre):
        return pre
    return None


def _split_names(chunk: str) -> list[str]:
    """Split a 'X and Y'/'X, Y agus Z' run into individual validated names."""
    parts = re.split(r"(?i)\s+(?:and|agus|&)\s+|,\s+", chunk)
    out = []
    for p in parts:
        nm = _name_from_line(p)
        if nm and nm not in out:
            out.append(nm)
    return out


# Name(s) appearing on the same line as the appointment verb, with or without a
# trailing "as …": "…has re-appointed Ms Carol Gibbons", "appointed X and Y as …".
_VERB_TAIL = re.compile(r"(?i)\b(?:re-)?appointed\b[,:]?\s+(?:the following\s+)?(.+)")
_ROLE_CUT = re.compile(
    r"(?i)\s+(?:as\b|to be\b|to the\b|to serve|with effect|for a\b|under\b|pursuant|"
    r"in accordance|of the board|a member|an ordinary|chairperson|with the consent|having)"
)


def _names_after_verb(line: str) -> list[str]:
    m = _VERB_TAIL.search(line)
    if not m:
        return []
    tail = _ROLE_CUT.split(m.group(1))[0].strip(" .,")
    return _split_names(tail)


def extract_appointees(t: str) -> list[str]:
    """All appointee names in a notice (judicial notices list several). Returns
    [] when the notice names no individual (e.g. special-adviser/registrar Orders)."""
    lines = _lines(t)

    def _dedup(xs: list[str]) -> list[str]:
        out: list[str] = []
        for x in xs:
            if x not in out:
                out.append(x)
        return out

    # Irish/bilingual span: names sit between the "…tar éis"/"appointed[,.]"
    # formula line and the "a cheapadh/athcheapadh/…" verb line.
    names: list[str] = []
    start = None
    for i, line in enumerate(lines):
        if re.search(r"(?i)tar éis$|have this day$|appointed[,:.]?$", line):
            start = i
        if start is not None and _APPT_VERB_GA.search(line) and i > start:
            for cand in lines[start + 1:i]:
                if re.fullmatch(r"(?i)agus|and|&", _clean_name_line(cand)):
                    continue
                nm = _name_from_line(cand)
                if nm:
                    names.append(nm)
            if names:
                return _dedup(names)
            break

    # English: name(s) on the same line as the verb — "appointed Ms Carol Gibbons",
    # "appointed X and Y as board members". Highest-yield English pattern.
    for line in lines:
        got = _names_after_verb(line)
        if got:
            names.extend(got)
    if names:
        return _dedup(names)

    # English "the following…:" / bullet list of names (name may trail a sentence).
    for i, line in enumerate(lines):
        if re.search(r"(?i)following\b", line) or line.rstrip().endswith(":"):
            for cand in lines[i + 1:i + 12]:
                nm = _name_from_line(cand)
                if nm:
                    names.append(nm)
                elif names:
                    break
            if names:
                return _dedup(names)

    # English: verb line then the next proper-noun line.
    for i, line in enumerate(lines):
        if _APPT_VERB_EN.search(line):
            for cand in lines[i + 1:i + 3]:
                nm = _name_from_line(cand)
                if nm:
                    return [nm]

    # Irish fallback: nearest name line before the verb.
    for i, line in enumerate(lines):
        if _APPT_VERB_GA.search(line):
            for j in range(i - 1, max(i - 6, -1), -1):
                nm = _name_from_line(lines[j])
                if nm:
                    return [nm]
    return names


def extract_role(t: str, atype: str) -> str | None:
    if atype == "special_adviser":
        return "Special adviser"
    if atype == "judicial":
        if re.search(r"(?i)mar uachtarán ar|president of the", t):
            return "Court President"
        return "Judge"
    m = re.search(r"(?i)\bas\s+(?:a|an|the)?\s*(ordinary member|member|chairperson|chair|director|deputy chair[^,\n.]*)", t)
    if m:
        return m.group(1).strip().title()
    u = t.upper()
    for ga, en in ROLES.items():
        if ga == "UACHTARÁN":  # appointer, not a role, in these notices
            continue
        if re.search(rf"MAR\s+{ga}\b", u):
            return en
    return None


def extract_body(title: str, t: str, atype: str) -> str | None:
    if atype == "judicial":
        u = t.upper()
        for ga, en in COURTS.items():
            if ga in u:
                return en
        return "Courts"
    seg = re.split(r"\s*(?:\||//)\s*", title)[0] if title else ""
    # Title segments that are preamble/headers/statute names carry no body —
    # go to text fallback.
    if _TITLE_NOT_BODY.match(seg) or re.search(r"(?i)\bact\b.{0,6}(19|20)\d\d", seg):
        seg = ""
    seg = re.sub(r"(?i)^(notice of )?appointment(?:s)?(?:/re-appointment)?\s+(?:of members\s+)?to\s+(?:the board of\s+)?(?:the\s+)?", "", seg)
    seg = re.sub(r"(?i)^the board of\s+", "", seg)
    seg = re.sub(r"(?i)^department of .*|^an roinn.*", "", seg)  # dept header isn't the body
    seg = seg.strip(" .,|")
    if seg:
        su = seg.upper()
        for ga, en in BODIES.items():
            if ga in su:
                return en
        return seg
    # Title was a department header — recover the body from the notice text.
    m = re.search(r"(?i)\b(?:to the board of|as (?:a |an )?member of (?:the )?|appointment to (?:the )?|"
                  r"mar (?:chomhalta|bhall|stiúrthóir|chathaoirleach) (?:de |den |ar )?)"
                  r"([A-ZÁÉÍÓÚ][^\n.,:•]{3,60})", t)
    if m:
        return m.group(1).strip(" .")
    # Last resort: the department name itself.
    dh = re.split(r"\s*(?:\||//)\s*", title)[0].strip(" .,|") if title else ""
    return dh or None


def extract_portfolio(t: str) -> str | None:
    m = _PORTFOLIO_EN.search(t)
    if not m:
        m = re.search(r"(?i)\(Appointment of Special Advisers?\s*\(?([^)]*?)\)?\s*Order", t)
    return re.sub(r"\s*//\s*", " ", m.group(1)).strip() if m else None


def _who(appointees: list[str]) -> str:
    if not appointees:
        return "—"
    if len(appointees) == 1:
        return appointees[0]
    return f"{appointees[0]} + {len(appointees) - 1} others"


def english_summary(authority, atype, appointees, role, body, portfolio, t) -> str | None:
    if atype == "special_adviser":
        m = _SA_PAREN.search(t)
        if m:
            return re.sub(r"\s*//\s*", " ", m.group(1)).strip()
        if portfolio:
            return f"Special adviser appointed — {portfolio}"
    who = _who(appointees)
    r = role or "Member"
    if atype == "judicial":
        return f"{authority} appointed {who} as {r}, {body or 'the Courts'}"
    return f"{authority} appointed {who} as {r}, {body}" if body else f"{authority} appointed {who} as {r}"


def enrich(df: pl.DataFrame) -> pl.DataFrame:
    rows = []
    for r in df.iter_rows(named=True):
        t = str(r.get("raw_text") or "")
        title = str(r.get("title") or "")
        if _BANKRUPT_RE.search(t) or _CONTAM_RE.search(t):
            continue
        atype = appointment_type(str(r.get("notice_subtype") or ""), title, t)
        if atype == "military":
            continue
        auth = appointing_authority(t, title)
        if atype == "special_adviser" and auth == "Unknown":
            auth = "Government"  # special-adviser Orders are made by the Government
        appointees = [] if atype == "special_adviser" else extract_appointees(t)
        role = extract_role(t, atype)
        portfolio = extract_portfolio(t) if atype == "special_adviser" else None
        body = portfolio if atype == "special_adviser" else extract_body(title, t, atype)
        # Language detection — the OLD rule flagged the whole notice "Irish" if
        # ANY Irish marker appeared in the raw_text, including the standard
        # Irish signature "MARTIN FRASER, Ard-Rúnaí an Rialtais" at the END of
        # otherwise-English notices (e.g. "APPOINTMENT TO THE BOARD OF HORSE
        # RACING IRELAND ...") — so English ministerial notices were
        # mis-tagged as Irish and got the shallower Irish-flow treatment.
        # New rule: an Irish marker must appear within the LEAD (first ~6 lines)
        # of the notice — the body of the appointment, not the closing
        # signature block. The English-title kicker exclusion is preserved.
        lead = "\n".join(_lines(t)[:6])
        is_irish = bool(
            re.search(r"(?i)tá an|ag gníomhú|cheapadh|uachtarán|an rialtas|an roinn|gníomhaireacht|"
                      r"an aire|tar éis", lead)
            and not re.search(r"(?i)^appointment to|the government today", title)
        )
        rows.append({
            "notice_ref": r.get("notice_ref"),
            "issue_date": r.get("issue_date"),
            "appointing_authority": auth,
            "appointment_type": atype,
            "body": body,
            "appointee": "; ".join(appointees) if appointees else None,
            "appointee_count": len(appointees),
            "role": role,
            "portfolio": portfolio,
            "english_summary": english_summary(auth, atype, appointees, role, body, portfolio, t),
            "lang": "Irish" if is_irish else "English",
            "title": title,
            "iris_source_pdf": r.get("source_file"),
        })
    return pl.DataFrame(rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="write the gold parquet")
    args = ap.parse_args()

    src = pl.read_csv(_SRC, infer_schema_length=20000)
    pa = src.filter(pl.col("notice_category") == "public_appointment")
    out = enrich(pa)

    n = out.height
    print(f"input public_appointment rows : {pa.height}")
    print(f"after exclude (military/contaminant/bankrupt): {n}")
    print("\nby appointment_type:")
    for r in out["appointment_type"].value_counts(sort=True).iter_rows():
        print(f"   {r[0]:16s} {r[1]}")
    print("\nby appointing_authority:")
    for r in out["appointing_authority"].value_counts(sort=True).iter_rows():
        print(f"   {r[0]:16s} {r[1]}")
    print("\nfield coverage:")
    for c in ["appointee", "body", "role", "english_summary"]:
        f = out.filter(pl.col(c).is_not_null() & (pl.col(c).cast(pl.Utf8).str.strip_chars() != "")).height
        print(f"   {c:18s} {f:5d} ({100*f/n:3.0f}%)")
    # appointee only meaningful where a person is named (boards + judicial)
    nonsa = out.filter(pl.col("appointment_type") != "special_adviser")
    naf = nonsa.filter(pl.col("appointee").is_not_null()).height
    print(f"   appointee (non-SA) {naf:5d}/{nonsa.height} ({100*naf/max(nonsa.height,1):3.0f}%)")
    print(f"   total appointees named: {int(out['appointee_count'].sum())}")
    sa = out.filter(pl.col("appointment_type") == "special_adviser")
    saf = sa.filter(pl.col("portfolio").is_not_null()).height
    print(f"   portfolio (of SAs) {saf:5d}/{sa.height}")

    print("\nsample enriched rows:")
    for r in out.select(["appointment_type", "appointing_authority", "appointee", "role", "body", "english_summary"]).head(16).iter_rows():
        print("  ", [str(x)[:34] for x in r])

    if args.write:
        _OUT.parent.mkdir(parents=True, exist_ok=True)
        out.write_parquet(_OUT, compression="zstd", compression_level=3, statistics=True)
        print(f"\nwrote {n} rows -> {_OUT}")


if __name__ == "__main__":
    main()
