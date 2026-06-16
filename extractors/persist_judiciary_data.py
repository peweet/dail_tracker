"""Persist the validated judiciary exploration datasets (2026-06-04) to CSV + Parquet.
THROWAWAY/sandbox persistence — NOT pipeline ETL. Outputs -> data/sandbox/judiciary/.
Run: .venv/Scripts/python.exe extractors/persist_judiciary_data.py
Sources: data/gold/parquet/public_appointments.parquet (spine), C:/tmp cached PDFs/CSVs,
ROSTER literal in probe_judiciary_join.py, and values captured during the 2026-06-04 pulls.
"""

import re
import sys
import unicodedata
import warnings
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # repo root, for first-party imports

warnings.filterwarnings("ignore")
import polars as pl
import fitz  # PyMuPDF (in .venv)

from extractors._judiciary_roster import ROSTER

OUT = Path("data/sandbox/judiciary")
OUT.mkdir(parents=True, exist_ok=True)
TMP = Path("C:/tmp")


def write_both(df: pl.DataFrame, name: str):
    df.write_parquet(OUT / f"{name}.parquet", compression="zstd",
                     compression_level=3, statistics=True)
    df.write_csv(OUT / f"{name}.csv")
    print(f"  wrote {name:38} {df.shape[0]:>4} rows x {df.shape[1]} cols")


# ---------- name normalisation (shared) ----------
HON = {"mr", "mrs", "ms", "dr", "judge", "justice", "the", "hon", "honourable"}


def norm(name: str) -> frozenset:
    if not name:
        return frozenset()
    n = unicodedata.normalize("NFD", name)
    n = "".join(c for c in n if unicodedata.category(c) != "Mn")
    n = n.lower().replace("'", " ").replace(".", " ").replace(",", " ")
    return frozenset(t for t in re.split(r"\s+", n) if len(t) > 1 and t not in HON)


print("Persisting judiciary datasets ->", OUT.resolve())

# ============ 1. appointment spine (clean judicial subset) ============
df = pl.read_parquet("data/gold/parquet/public_appointments.parquet")
jud = df.filter(pl.col("appointment_type") == "judicial")
REAL = ["High Court", "District Court", "Circuit Court", "Supreme Court", "Court of Appeal"]
spine = (jud.with_columns(pl.col("body").is_in(REAL).alias("is_real_court"))
            .select(["notice_ref", "issue_date", "appointing_authority", "body", "appointee",
                     "appointee_count", "role", "lang", "is_real_court", "iris_source_pdf"])
            .rename({"body": "court"}))
write_both(spine, "judicial_appointments_spine")

# exploded one-row-per-named-person (clean real-court only)
ex_rows = []
for r in jud.filter(pl.col("body").is_in(REAL)).iter_rows(named=True):
    if not r["appointee"]:
        continue
    for nm in r["appointee"].split(";"):
        nm = nm.strip()
        if nm and len(norm(nm)) >= 2:
            ex_rows.append({"appointee": nm, "issue_date": r["issue_date"],
                            "court": r["body"], "role": r["role"],
                            "appointing_authority": r["appointing_authority"]})
spine_ex = pl.DataFrame(ex_rows)
write_both(spine_ex, "judicial_appointments_exploded")

# ============ 2. current roster (curated data module extractors/_judiciary_roster.py) ============
rows = [{"judge_name": nm, "court": court} for court, names in ROSTER.items() for nm in names]
roster = pl.DataFrame(rows)
# ex-officio = same name listed under >1 court
dups = roster.group_by("judge_name").len().filter(pl.col("len") > 1)["judge_name"].to_list()
roster = roster.with_columns(pl.col("judge_name").is_in(dups).alias("is_ex_officio_or_multi"))
write_both(roster, "judiciary_current_roster")

# ============ 3. spine -> roster join (incl. elevations) ============
roster_list = [(norm(nm), nm, court) for court, names in ROSTER.items() for nm in names]
roster_exact = {}
for t, nm, c in roster_list:
    roster_exact.setdefault(t, (nm, c))


def match(tok):
    if tok in roster_exact:
        return roster_exact[tok]
    for rset, rnm, rc in roster_list:
        if len(tok & rset) >= 2 and (tok <= rset or rset <= tok):
            return (rnm, rc)
    return None


jrows = []
for r in ex_rows:
    m = match(norm(r["appointee"]))
    if m is None:
        status, cur = "unmatched", None
    else:
        cur = m[1]
        status = "elevated" if cur != r["court"] else "matched"
    jrows.append({"appointee": r["appointee"], "appointed_court": r["court"],
                  "appointed_date": r["issue_date"], "current_court": cur, "status": status})
joined = pl.DataFrame(jrows)
write_both(joined, "judiciary_appointment_roster_join")

# ============ 4. gov.ie nominations (captured 2026-06-04, 4 announcements) ============
GOVIE = [
    # (announce_date, nominee, target_court, prior_career, vacancy_cause, predecessor)
    ("2026-04-01", "Edward Carroll", "District Court", "Cork solicitor (partner since 1995)", "resignation", "Judge O'Shea (Jul 2025)"),
    ("2026-04-01", "Paula Cullinane", "District Court", "Dublin solicitor (partner since 2021)", "Circuit Court elevation", None),
    ("2026-04-01", "Andrew Gubbins", "District Court", "Tipperary barrister (called 2005)", "Circuit Court elevation", None),
    ("2026-04-01", "Tom MacSharry", "District Court", "Sligo solicitor (sole practitioner since 2007)", "Circuit Court elevation", None),
    ("2026-04-01", "Mary McAveety", "District Court", "Cavan solicitor (principal since 2006)", "retirement", None),
    ("2026-04-01", "Darach McCarthy", "District Court", "Limerick solicitor (principal since 2006)", "retirement", None),
    ("2026-04-01", "Catriona Murray", "District Court", "Wicklow solicitor (partner since 2006)", "retirement", None),
    ("2026-04-01", "Olivia Traynor", "District Court", "Galway solicitor (sole practitioner since 2012)", "retirement", None),
    ("2025-12-18", "Mark Dunne", "High Court", "Senior Counsel", "elevation", "A.M. Owens elevated to Court of Appeal (29 Sep 2025)"),
    ("2025-12-18", "Micheál O'Connell", "High Court", "Senior Counsel", "retirement", "Judge Mary Ellen Ring (27 Dec 2025)"),
    ("2024-10-31", "Denis McDonald", "Court of Appeal", None, "new post (2024 legislation)", None),
    ("2024-10-31", "Anthony Collins", "Court of Appeal", "former Advocate General (CJEU)", "new post (2024 legislation)", None),
    ("2024-10-31", "Sara Phelan", "High Court", "Senior Counsel", "elevation", "N. Hyland elevated to Court of Appeal (3 Oct 2024)"),
    ("2024-10-31", "Peter White", "District Court", "Solicitor", "retirement", "Judge Cormac Dunne (15 Jun 2024)"),
    ("2024-10-31", "Catherine Ghent", "District Court", "Solicitor", "death", "Judge Elizabeth MacGrath (3 Jul 2024)"),
    ("2024-10-31", "Áine Clancy", "District Court", "Solicitor and BL", "retirement", "Judge Marian O'Leary (30 Aug 2024)"),
]
govie = pl.DataFrame(GOVIE, schema=["announce_date", "nominee", "target_court",
                                    "prior_career", "vacancy_cause", "predecessor"], orient="row")
write_both(govie, "judicial_nominations_govie")

# ============ 5. High Court assignments — Hilary Term 2026 (22 roles) ============
ASSIGN = [
    ("McDermott", "Central Criminal Court, Extradition, CAB and Bail"),
    ("Hunt", "Special Criminal Court"), ("Coffey", "Personal Injuries List (incl. Clinical Negligence)"),
    ("Gearty", "Non-Jury / Judicial Review and Asylum, Immigration and Citizenship Lists"),
    ("Cregan", "Chancery List"), ("Sanfey", "Commercial List"),
    ("Humphreys", "Planning and Environment List"), ("Jackson", "Family List"),
    ("Jordan", "Minors in Special Care List"), ("O'Connor", "Civil Jury List"),
    ("Cahill", "Personal Insolvency List"), ("Barrett", "Competition List"),
    ("O'Donnell", "Hague Luxembourg Convention List"), ("Stack", "Probate List"),
    ("Reynolds", "Garda Compensation List"), ("Kennedy", "Bankruptcy List and Examiner's List"),
    ("Kennedy & O. Quinn", "Criminal Assets Bureau List"),
    ("Simons", "Adjudication List (Construction Contracts Act 2013)"),
    ("Mulcahy", "Directors Disqualifications List"),
    ("M. Quinn, Roberts, Mulcahy", "Examinership matters"),
    ("Mulcahy", "Admiralty matters"), ("O'Regan", "Land Registry matters"),
]
assign = pl.DataFrame(ASSIGN, schema=["judge", "assignment"], orient="row").with_columns(
    pl.lit("Hilary Term 2026").alias("term"), pl.lit("High Court").alias("court"))
write_both(assign, "judiciary_hc_assignments")

# ============ 6. conduct stats — Section 87(4) (extract 2024 report, has 2024+2023) ============
def conduct_from_2024():
    doc = fitz.open(str(TMP / "jc_annual_2024.pdf"))
    out = []
    for i in range(doc.page_count):
        pg = doc.load_page(i)
        if "section 87" not in pg.get_text().lower():
            continue
        for tab in pg.find_tables().tables:
            for r in tab.extract():
                c = [(x or "").strip() for x in r]
                if len(c) == 3 and re.fullmatch(r"\d{1,4}", c[0]) and re.fullmatch(r"\d{1,4}", c[2]):
                    desc = c[1].replace("\n", " ")
                    out.append({"item": desc[:90], "count_2024": int(c[0]), "count_2023": int(c[2])})
    doc.close()
    return out


cond = conduct_from_2024()
# tidy long form + the 2022 partial-year received count
long = []
for row in cond:
    long.append({"year": 2024, "item": row["item"], "count": row["count_2024"]})
    long.append({"year": 2023, "item": row["item"], "count": row["count_2023"]})
long.append({"year": 2022, "item": "(a) the number of complaints received,", "count": 17})
conduct = pl.DataFrame(long)
write_both(conduct, "judicial_conduct_stats")

# ============ 7. courts clearance (union 2017,2020,2022,2023,2024) ============
clr_files = {2017: TMP / "ca_2017.csv", 2020: TMP / "ca_2020.csv", 2022: TMP / "ca_2022.csv",
             2023: TMP / "ca_2023.csv", 2024: TMP / "courts_annual_2024.csv"}
frames = []
for y, f in clr_files.items():
    if f.exists() and f.stat().st_size > 200:
        frames.append(pl.read_csv(f, infer_schema_length=3000))
clearance = pl.concat(frames, how="diagonal_relaxed")
write_both(clearance, "courts_clearance")

# ============ 8. waiting times (extract AR 2024 pp129-141) ============
def waiting():
    doc = fitz.open(str(TMP / "courts_ar_2024.pdf"))
    wk = re.compile(r"(week|month|day|immediately|date)", re.I)
    out = []
    for p in range(129, 142):
        for tab in doc.load_page(p - 1).find_tables().tables:
            for r in tab.extract():
                c = [(x or "").strip().replace("\n", " ") for x in r]
                if len(c) >= 3 and c[0] and wk.search(" ".join(c[-2:]).lower()):
                    h = c[0].lower()
                    if h not in ("nature of application", "venue") and "waiting time" not in h:
                        out.append({"page": p, "matter_or_venue": c[0][:80],
                                    "wait_2024": c[-2][:30], "wait_2023": c[-1][:30]})
    doc.close()
    return out


wait = pl.DataFrame(waiting())
write_both(wait, "courts_waiting_times")

# ============ 9. courthouses ============
ch = pl.read_csv(TMP / "court_offices.csv", infer_schema_length=2000)
write_both(ch, "courthouses")

# ============ 10. judicial salaries (SI 323/2021) ============
SAL = [("Chief Justice", 266295), ("President of the High Court", 247276),
       ("Ordinary Judge, Supreme Court", 232060), ("President of the Circuit Court", 224452),
       ("Ordinary Judge, High Court", 218748), ("President of the District Court", 171327),
       ("Judge, Circuit Court", 165421), ("Judge, District Court", 147961)]
sal = pl.DataFrame(SAL, schema=["office", "salary_eur"], orient="row").with_columns(
    pl.lit("SI 323/2021").alias("source"), pl.lit("2021-07-01").alias("effective"))
write_both(sal, "judicial_salaries")

# ============ 11. European court seats (Wikidata, captured) ============
EURO = [("Cearbhall Ó Dálaigh", "CJEU"), ("John L. Murray", "CJEU"), ("Fidelma Macken", "CJEU"),
        ("Aindrias Ó Caoimh", "CJEU"), ("Eugene Regan", "CJEU"), ("Tom O'Higgins", "CJEU"),
        ("Kevin O'Higgins", "CJEU"), ("Conor Maguire", "ECtHR")]
euro = pl.DataFrame(EURO, schema=["name", "court"], orient="row")
write_both(euro, "judges_european_seats")

# NOTE: revolving-door (TD/AG -> judge) dataset REMOVED 2026-06-04 — it relied on
# surname/fuzzy matching against the bench, which yields false positives (common
# surnames colliding with historical people). Do not reintroduce fuzzy name joins.

print("DONE.")
