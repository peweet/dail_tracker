"""THROWAWAY SPIKE: parse the Courts Service daily Legal Diary (.docx) into tiered outputs
so we can compare a privacy-safe build (A+B) against an anonymised case-level build (C).

NOT pipeline ETL. Sandbox only. Reads the cached C:/tmp/diary.docx (pulled 2026-06-04).
Outputs -> data/sandbox/judiciary/legal_diary_*.{parquet,csv} + prints a comparison report.

Privacy model for Tier C
------------------------
1. DROP entire statutory in-camera / vulnerable categories (minors, family, wards, special
   care, childcare, asylum) -> never parsed into the kept set, never linked.
2. For everything else, ANONYMISE every natural person to initials. Organisations and State
   bodies are kept in clear (that is the accountability signal: "X v Minister for Justice").
3. Strip every case reference and solicitor annotation (quasi-identifiers).
4. Attach a provenance LINK to the official source so anyone can verify against the primary
   record -- we minimise in our own dataset and cite the public original.

Run: .venv/Scripts/python.exe pipeline_sandbox/legal_diary_parse_spike.py
"""

import hashlib
import html
import re
import warnings
import zipfile
from pathlib import Path

warnings.filterwarnings("ignore")
import polars as pl

SRC = Path("C:/tmp/diary.docx")
OUT = Path("data/sandbox/judiciary")
OUT.mkdir(parents=True, exist_ok=True)
DIARY_DATE = "2026-06-04"
SOURCE_NAME = "Courts Service Legal Diary"
SOURCE_URL = "https://legaldiary.courts.ie/"  # live page (current day); archive daily for history


def write_both(df: pl.DataFrame, name: str):
    df.write_parquet(OUT / f"{name}.parquet", compression="zstd",
                     compression_level=3, statistics=True)
    df.write_csv(OUT / f"{name}.csv")
    print(f"  wrote {name:34} {df.shape[0]:>4} rows x {df.shape[1]} cols")


def read_docx_lines(path: Path) -> list[str]:
    xml = zipfile.ZipFile(path).read("word/document.xml").decode("utf-8", "ignore")
    out = []
    for para in re.split(r"</w:p>", xml):
        txt = "".join(re.findall(r"<w:t[^>]*>([^<]*)</w:t>", para))
        txt = html.unescape(txt).replace("’", "'").strip()
        if txt:
            out.append(txt)
    return out


# ---------------------------------------------------------------- structural classifiers
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
PARTY_RE = re.compile(r"\b-?\s*v\s*-?\b", re.I)

# ---------------------------------------------------------------- privacy classifiers
# statutory in-camera / vulnerable -> DROP entirely (not anonymised, not linked)
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


def is_protected(list_type: str, case: str) -> str | None:
    blob = f"{list_type} || {case}".lower()
    for k in PROTECTED_KEYS:
        if k in blob:
            return k
    return None


def category(list_type: str, case: str) -> str:
    blob = f"{list_type} || {case}".lower()
    if any(k in blob for k in PROSECUTOR_KEYS):
        return "criminal"
    if any(k in blob for k in STATE_KEYS):
        return "public-law"
    if any(k in blob for k in ORG_KEYS):
        return "commercial"
    return "civil"


# ---------------------------------------------------------------- anonymisation
def strip_refs(t: str) -> str:
    t = re.sub(r"\([^)]*\)", " ", t)                              # solicitor/duration parens
    t = re.sub(r":[A-Z]{2,}:[A-Z0-9:]+", " ", t)                 # :LCA:OLCA:2026:000144
    t = re.sub(r"\b\d*\s*H\.?\s*JR\.?\s*\d{4}\.?\d+[A-Z]?", " ", t, flags=re.I)
    t = re.sub(r"\bH\.?\s*P\.?\s*\d{4}\.?\d+", " ", t, flags=re.I)
    t = re.sub(r"\b\d*\s*CCDP\d+/\d+", " ", t, flags=re.I)
    t = re.sub(r"\b\d*\s*CJA/\d+", " ", t, flags=re.I)
    t = re.sub(r"\bPI\s*\d+", " ", t, flags=re.I)
    t = re.sub(r"\b\d{4}\s+\d+\s+[A-Z]\b", " ", t)               # 2022 3507 P
    t = re.sub(r"\b\d+\s*/\s*\d+\b", " ", t)                     # 260/22, 174/25
    t = re.sub(r"\d{3,}", " ", t)                                # any long digit run / glued ref
    t = re.sub(r"^[A-Z]{1,4}\d[\w./:]*", " ", t)                 # leading glued alnum ref
    return re.sub(r"\s+", " ", t).strip(" -:.,")


_SKIP = {"the", "and", "of", "for", "mr", "mrs", "ms", "dr", "an", "na", "orse",
         "through", "nf", "trading", "as", "formerly", "also", "known", "minor", "a"}
_TAIL = re.compile(r"&\s*(ors|anor|others)\b", re.I)


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


def anonymise(raw: str) -> str:
    t = strip_refs(raw)
    if not t:
        return ""
    parts = re.split(r"\s*-?\s*\bv\b\.?\s*-?\s*", t, maxsplit=1, flags=re.I)
    if len(parts) != 2:
        return t if _is_org(t) else _initials(t)
    a, b = parts
    a2 = a.strip() if _is_org(a) else _initials(a)
    b2 = b.strip() if _is_org(b) else _initials(b)
    return f"{a2} v {b2}"


# ---------------------------------------------------------------- state machine
def parse(lines: list[str]):
    court = room = judge = list_type = time_s = status = None
    schedule, cases, seq = {}, [], 0

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
        if ln.isupper() and ("LIST" in ln or re.search(
                r"FAMILY|CHANCERY|COMMERCIAL|CRIMINAL|PERSONAL INJ|JUDICIAL REVIEW|PROBATE|"
                r"BANKRUPT|EXAMIN|APPEAL|MENTION|WARDS|EDUCATION|ASYLUM|INSOLVENCY|"
                r"COMPETITION|PLANNING|ADMIRALTY", ln)):
            list_type = ln.title()
            if judge:
                schedule.setdefault(key(), {"court": court, "courtroom": room, "judge": judge,
                                            "list_type": list_type, "time": time_s, "n_items": 0})
            continue
        if PARTY_RE.search(f" {ln} ") or ln.upper().startswith("IN THE MATTER"):
            seq += 1
            prot = is_protected(list_type or "", ln)
            cases.append({"seq": seq, "court": court, "courtroom": room, "judge": judge,
                          "list_type": list_type, "time": time_s, "status": status,
                          "raw_case": ln, "category": category(list_type or "", ln),
                          "protected": bool(prot), "protected_reason": prot})
            if judge:
                s = schedule.setdefault(key(), {"court": court, "courtroom": room, "judge": judge,
                                                "list_type": list_type, "time": time_s, "n_items": 0})
                s["n_items"] += 1
    return list(schedule.values()), cases


# ================================================================ run
print("Parsing", SRC, "->", OUT.resolve())
sha = hashlib.sha256(SRC.read_bytes()).hexdigest()[:16]
lines = read_docx_lines(SRC)
sessions, cases = parse(lines)

sched = (pl.DataFrame(sessions).with_columns(pl.lit(DIARY_DATE).alias("diary_date"))
         .filter(pl.col("judge").is_not_null()).unique().sort(["court", "courtroom", "judge"]))
write_both(sched, "legal_diary_a_schedule")

counts = (sched.select(["diary_date", "court", "judge", "list_type", "n_items"])
          .filter(pl.col("n_items") > 0).sort("n_items", descending=True))
write_both(counts, "legal_diary_b_counts")

cdf = pl.DataFrame(cases).with_columns(pl.lit(DIARY_DATE).alias("diary_date"))
write_both(cdf, "legal_diary_c_audit")

strict = (cdf.filter(~pl.col("protected"))
          .with_columns(pl.col("raw_case").map_elements(anonymise, return_dtype=pl.Utf8)
                        .alias("case_anonymised"))
          .filter(pl.col("case_anonymised").str.len_chars() > 2)
          .with_columns([pl.lit(SOURCE_NAME).alias("source"),
                         pl.lit(SOURCE_URL).alias("source_url"),
                         pl.lit(sha).alias("source_sha256")])
          .select(["diary_date", "court", "judge", "list_type", "status", "category",
                   "case_anonymised", "source", "source_url", "source_sha256"])
          .sort(["court", "judge"]))
write_both(strict, "legal_diary_c_strict")
print("DONE.")
