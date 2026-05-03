"""
iris_oifigiuil_evidence2.py — second-pass evidence answering follow-ups.

  Q5. Standards: are specific I.S. codes ever published, and in what
      shape? Is the boilerplate-only pattern actually correct, or are
      late adoptions enumerated? Show actual notices.
  Q6. ICAV migrations: do the notices identify the SOURCE jurisdiction?
      If yes, is the London → Dublin claim defensible? If no, withdraw it.
  Q7. 'Surrender of authorisation' notices: read 5 in detail and explain
      plainly what's happening.
  Q8. SI breakdowns: parent Act, issuing Department, subject keywords.
  Q9. SI volume year-over-year + around the 2020 + 2024 elections.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import fitz

# TODO: replace hardcoded path with `BRONZE_DIR / "iris_oifigiuil"` from config when promoting out of experimental
PDF_DIR = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")
DELIM = re.compile(r"_{6,}")
SI_HEAD = re.compile(r"S\.I\. No\.\s*(\d+) of (\d{4})\.?")


def pub_date(name: str) -> str:
    m = re.match(r"[Ii][Rr](\d{2})(\d{2})(\d{2})\.pdf", name)
    return datetime.strptime("".join(m.groups()), "%d%m%y").date().isoformat() if m else ""


def load(p: Path) -> str | None:
    if p.stat().st_size < 5_000: return None
    try:
        with fitz.open(p) as doc:
            return "\n".join(doc[i].get_text() for i in range(doc.page_count))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Q5 — Standards
# ---------------------------------------------------------------------------

# Try several shapes for an actual standard reference.
STD_SHAPES = [
    ("I.S. EN ###",       re.compile(r"\bI\.S\. EN[\s/]+\d{2,5}[^\n]{0,180}")),
    ("EN ISO ###",        re.compile(r"\bEN ISO\s+\d{2,5}[^\n]{0,180}")),
    ("I.S. ###",          re.compile(r"\bI\.S\.\s+\d{3,5}\b[^\n]{0,180}")),
    ("ISO ###",           re.compile(r"\bISO\s+\d{4,5}\b[^\n]{0,180}")),
    ("CEN/CENELEC EN",    re.compile(r"\bEN\s+\d{4,5}(?::\d{4})?[^\n]{0,180}")),
]

REVOCATION_HINT = re.compile(r"REVOC|WITHDRAW|REPLACED BY|SUPERSED", re.I)


def q5_standards(corpus: dict[str, str]) -> None:
    print(f"\n{'='*78}\nQ5. Standards — what shapes appear?\n{'='*78}")
    overall: dict[str, int] = Counter()
    pdfs_with_specific: set[str] = set()
    samples: list[tuple[str, str, str]] = []  # (shape, pdf, line)
    for name, text in corpus.items():
        for shape, pat in STD_SHAPES:
            for m in pat.finditer(text):
                line = re.sub(r"\s+", " ", m.group(0)).strip()[:180]
                # filter out the boilerplate intro paragraph
                if "developed by" in line.lower() or "adopted by NSAI" in line:
                    continue
                overall[shape] += 1
                pdfs_with_specific.add(name)
                if len(samples) < 30:
                    samples.append((shape, name, line))
    print(f"  Hits by shape (excluding boilerplate sentences):")
    for shape, n in overall.most_common():
        print(f"    {shape:20s} {n}")
    print(f"  Distinct PDFs containing any specific code: {len(pdfs_with_specific)} / {len(corpus)}")
    print(f"\n  Up to 15 actual matches:")
    for shape, name, line in samples[:15]:
        print(f"    [{shape}] [{name}]  {line}")
    # check whether the matches sit near revocation language
    pdfs_revoc = sum(1 for n in pdfs_with_specific if REVOCATION_HINT.search(corpus[n]))
    print(f"\n  Of those PDFs, {pdfs_revoc} contain revocation/withdrawal language nearby.")


# ---------------------------------------------------------------------------
# Q6 — ICAV source jurisdiction
# ---------------------------------------------------------------------------

ICAV_BLOCK = re.compile(
    r"(Notice of Migrating Body[^\n]{0,40}\n.{0,2500})", re.S | re.I
)
JURISDICTION_HINTS = [
    "Cayman", "Luxembourg", "United Kingdom", "U\\.K\\.", "Jersey", "Guernsey",
    "Isle of Man", "British Virgin", "Bermuda", "Delaware", "Liechtenstein",
    "London", "Singapore", "Hong Kong"
]
JUR_RE = re.compile(r"\b(" + "|".join(JURISDICTION_HINTS) + r")\b", re.I)


def q6_icav(corpus: dict[str, str]) -> None:
    print(f"\n{'='*78}\nQ6. ICAV migrations — source jurisdiction inspection\n{'='*78}")
    found: list[tuple[str, str, list[str]]] = []
    for name, text in corpus.items():
        for m in ICAV_BLOCK.finditer(text):
            block = m.group(1)
            jurs = [j for j in JUR_RE.findall(block)]
            found.append((name, re.sub(r"\s+", " ", block[:600]).strip(), jurs))
    print(f"  Total ICAV migration notices found: {len(found)}")
    juris_count = Counter()
    for _, _, jurs in found:
        for j in jurs:
            juris_count[j.lower()] += 1
    print(f"  Source-jurisdiction mentions inside notice text:")
    for j, n in juris_count.most_common():
        print(f"    {j:20s} {n}")
    print(f"\n  Sample of 4 actual notices (text + detected jurisdictions):")
    for name, body, jurs in found[:4]:
        print(f"\n    [{name}] jurisdictions detected: {jurs or '(none)'}")
        print(f"    {body[:500]}")


# ---------------------------------------------------------------------------
# Q7 — 'Surrender of authorisation' explained
# ---------------------------------------------------------------------------

SURRENDER = re.compile(
    r"(revoc(?:ation|ed)|cancel(?:lation|led)|surrender)[^\n]{0,250}"
    r"(authoris|licen[cs]e|registration)",
    re.I,
)


def q7_surrender(corpus: dict[str, str]) -> None:
    print(f"\n{'='*78}\nQ7. 'Surrender of authorisation' — what is happening?\n{'='*78}")
    samples: list[tuple[str, str]] = []
    for name, text in corpus.items():
        for m in SURRENDER.finditer(text):
            ctx = re.sub(r"\s+", " ", text[max(0, m.start()-200):m.end()+150]).strip()
            samples.append((name, ctx[:480]))
    print(f"  Total hits: {len(samples)}; sampling 5:")
    for name, ctx in samples[:5]:
        print(f"\n    [{name}]\n    {ctx}")


# ---------------------------------------------------------------------------
# Q8 — SI sub-classification: parent Act, Department, subject
# ---------------------------------------------------------------------------

SI_TITLE = re.compile(
    r"S\.I\. No\.\s*\d+ of \d{4}\.?\s*\n(.{20,400}?)(?=\n\s*\n|\n[A-Z][a-z])",
    re.S,
)
PARENT_ACT = re.compile(
    r"made under (?:the )?([A-Z][^,\n]{8,80} Act,? \d{4})|"
    r"powers conferred (?:on (?:him|her|me) )?by (?:the )?([A-Z][^,\n]{8,80} Act,? \d{4})|"
    r"in exercise of the powers conferred on (?:him|her) by (?:the )?([A-Z][^,\n]{8,120} Act \d{4})",
    re.I,
)
DEPT = re.compile(
    r"\bMinister (?:for|of) ([A-Z][\w,\s\-]{3,80}?)(?:\sand|,)",
)
SUBJECT_KW = {
    "TAXATION":     re.compile(r"\b(tax|revenue|stamp dut|customs|excise|VAT)\b", re.I),
    "PLANNING":     re.compile(r"\b(planning|housing|development|tenancy|residential)\b", re.I),
    "AGRICULTURE":  re.compile(r"\b(agricultur|farm|livestock|veterinary|forestry)\b", re.I),
    "FISHING":      re.compile(r"\b(fish|aquaculture|sea-fish|salmon|quota)\b", re.I),
    "EU_ALIGN":     re.compile(r"\bEuropean (Union|Communities)\b", re.I),
    "TRANSPORT":    re.compile(r"\b(transport|aviation|road|rail|maritime)\b", re.I),
    "HEALTH":       re.compile(r"\b(health|medicin|pharma|hospital|public health)\b", re.I),
    "EDUCATION":    re.compile(r"\b(education|school|university|skill)\b", re.I),
    "ENVIRONMENT":  re.compile(r"\b(environment|climate|emission|pollut|waste)\b", re.I),
    "JUSTICE":      re.compile(r"\b(criminal|justice|garda|prison|court)\b", re.I),
    "FINANCE":      re.compile(r"\b(financ|bank|insur|pension|securit)\b", re.I),
    "COMMENCEMENT": re.compile(r"\bCOMMENCEMENT\b", re.I),
    "REVOCATION":   re.compile(r"\bREVOCATION\b", re.I),
    "AMENDMENT":    re.compile(r"\bAMEND", re.I),
}


def q8_si_breakdown(corpus: dict[str, str]) -> dict:
    print(f"\n{'='*78}\nQ8. SI sub-classification — parent Acts, Departments, subjects\n{'='*78}")
    si_records = []
    for name, text in corpus.items():
        # split on SI heading; capture body up to next heading or 1500 chars
        positions = [(m.start(), m.group(1), m.group(2)) for m in SI_HEAD.finditer(text)]
        for i, (start, num, year) in enumerate(positions):
            end = positions[i + 1][0] if i + 1 < len(positions) else min(start + 1500, len(text))
            body = text[start:end]
            title_m = SI_TITLE.search(body)
            title = re.sub(r"\s+", " ", title_m.group(1)).strip()[:200] if title_m else ""
            parent_m = PARENT_ACT.search(body)
            parent = next((g for g in parent_m.groups() if g), "") if parent_m else ""
            dept_m = DEPT.search(body)
            dept = dept_m.group(1).strip() if dept_m else ""
            subjects = [k for k, p in SUBJECT_KW.items() if p.search(body)]
            si_records.append({
                "pdf": name, "pub_date": pub_date(name), "si_no": int(num),
                "si_year": int(year), "title": title, "parent_act": parent,
                "department": dept, "subjects": subjects,
            })
    print(f"  Total SI records: {len(si_records)}")
    print(f"  With non-empty title:    {sum(1 for r in si_records if r['title'])}")
    print(f"  With parent Act named:   {sum(1 for r in si_records if r['parent_act'])}")
    print(f"  With Department named:   {sum(1 for r in si_records if r['department'])}")
    print(f"\n  Top 15 Departments:")
    for d, n in Counter(r["department"] for r in si_records if r["department"]).most_common(15):
        print(f"    {n:4d}  {d}")
    print(f"\n  Top 15 parent Acts:")
    for a, n in Counter(r["parent_act"] for r in si_records if r["parent_act"]).most_common(15):
        print(f"    {n:4d}  {a[:80]}")
    print(f"\n  Subject-tag distribution (multi-tag per SI):")
    sub_counts = Counter()
    for r in si_records:
        for s in r["subjects"]: sub_counts[s] += 1
    for s, n in sub_counts.most_common():
        print(f"    {n:5d}  {s}")
    return si_records


# ---------------------------------------------------------------------------
# Q9 — SI volume by year + around elections
# ---------------------------------------------------------------------------

ELECTION_DATES = {
    "2020 GE": "2020-02-08",
    "2024 GE": "2024-11-29",
}


def q9_si_volume(records: list[dict]) -> None:
    print(f"\n{'='*78}\nQ9. SI volume by year + 6-month windows around elections\n{'='*78}")
    by_year = Counter(r["si_year"] for r in records if r["pub_date"])
    print(f"  SI volume by si_year (note: counts SI numbers as referenced in the corpus):")
    for y in sorted(by_year):
        print(f"    {y}: {by_year[y]}")
    # by month of publication
    by_month = Counter(r["pub_date"][:7] for r in records if r["pub_date"])
    months_sorted = sorted(by_month)
    print(f"\n  SI publication count by month (last 30 months):")
    for m in months_sorted[-30:]:
        print(f"    {m}: {by_month[m]}")
    # 6-month windows pre/post each election
    for label, dt in ELECTION_DATES.items():
        d0 = datetime.strptime(dt, "%Y-%m-%d").date()
        pre = sum(1 for r in records if r["pub_date"]
                  and 0 < (d0 - datetime.strptime(r["pub_date"], "%Y-%m-%d").date()).days <= 90)
        post = sum(1 for r in records if r["pub_date"]
                   and 0 < (datetime.strptime(r["pub_date"], "%Y-%m-%d").date() - d0).days <= 90)
        print(f"  {label} ({dt}):  90 days BEFORE = {pre} SIs published   90 days AFTER = {post}")


def main() -> None:
    print("Loading corpus...")
    corpus = {p.name: load(p) for p in sorted(PDF_DIR.glob("*.pdf")) if p.stat().st_size >= 5_000}
    corpus = {k: v for k, v in corpus.items() if v}
    print(f"  loaded {len(corpus)} valid PDFs")

    q5_standards(corpus)
    q6_icav(corpus)
    q7_surrender(corpus)
    si_records = q8_si_breakdown(corpus)
    q9_si_volume(si_records)


if __name__ == "__main__":
    main()
