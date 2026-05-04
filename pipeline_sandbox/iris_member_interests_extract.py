"""
iris_member_interests_extract.py — v1 extractor for Iris Oifigiúil
member-interest supplements (Sections 6 and 29 of the Ethics in Public
Office Acts 1995/2001).

Walks every PDF in `BRONZE_DIR / iris_oifigiuil`, anchors on every
`STATEMENT UNDER SECTION 6|29 OF THE ETHICS IN PUBLIC OFFICE ACTS`
occurrence, and writes one CSV row per declaration to
`pipeline_sandbox/iris_member_interests.csv`.

Scope is intentionally narrow per `iris_oifigiuil_probe_findings.md` §13
(v1 = extractor only, member-interests + SI; this file = member-interests
only). No SI extraction, no resolver, no join against the annual register,
no silver/gold writes.

Output schema mirrors §5 of the findings doc minus `member_uri` (resolution
is explicit v3 work):

    source_pdf
    pub_date                     # IR{DDMMYY}.pdf -> YYYY-MM-DD
    section_trigger              # '6' or '29'
    registration_period          # raw 'in respect of the registration period ...' clause
    member_raw                   # 'Richard O'Donoghue TD' (no normalisation)
    interest_code                # 1..9 from the 'Category of Registrable Interest(s)' line
    interest_label               # e.g. 'Land (including property)'
    declaration_text             # prose after the category line, whitespace-collapsed, capped 2000
    matter_under_consideration   # §29 only — 'in respect of ...' clause before Name of Member

Validation: rows missing `member_raw` or `interest_code` are still emitted
and listed in the summary so regex misses are visible — no silent drops.
"""
from __future__ import annotations

import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path

import fitz
import pandas as pd

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))

from config import BRONZE_DIR  # noqa: E402

PDF_DIR = BRONZE_DIR / "iris_oifigiuil"
OUT_CSV = Path(__file__).with_name("iris_member_interests.csv")

# 146-byte 404 stubs in the corpus. Findings §1 records 31 of these.
_MIN_PDF_SIZE = 5_000

# Cap declaration_text so a runaway block (the §3 fat tail) doesn't blow up
# the CSV. 2 000 chars holds roughly five sub-clauses of declaration prose.
_MAX_DECLARATION = 2_000

_SECTION_ANCHOR = re.compile(
    r"STATEMENT UNDER SECTION (6|29) OF THE ETHICS IN PUBLIC OFFICE ACTS",
    re.I,
)
_REG_PERIOD = re.compile(
    r"in respect of the registration period\s+(.+?)(?=\n|Name of Member|$)",
    re.I | re.S,
)
_MATTER = re.compile(
    r"in respect of\s+(.+?)(?=Name of Member concerned|$)",
    re.I | re.S,
)
_MEMBER = re.compile(r"Name of Member concerned:\s*([^\n\r]+)", re.I)
# Category line on its own row: "3 - Land (including property)" or
# "3 – Land (including property)" (en-dash, em-dash, hyphen all seen).
_CATEGORY = re.compile(
    r"^\s*(\d{1,2})\s*[-–—]\s*([A-Z][A-Z\s\(\)/&,]+?)\s*$",
    re.M,
)


def _pub_date(name: str) -> str | None:
    m = re.match(r"[Ii][Rr](\d{2})(\d{2})(\d{2})\.pdf$", name)
    if not m:
        return None
    try:
        return datetime.strptime("".join(m.groups()), "%d%m%y").date().isoformat()
    except ValueError:
        return None


def _load_text(p: Path) -> str | None:
    if p.stat().st_size < _MIN_PDF_SIZE:
        return None
    try:
        with fitz.open(p) as doc:
            return "\n".join(doc[i].get_text() for i in range(doc.page_count))
    except Exception:
        return None


def _collapse(s: str) -> str:
    return re.sub(r"\s+", " ", s).strip()


def _extract(text: str, source_pdf: str, pub_date: str | None) -> list[dict]:
    """Return one row per Section 6/29 declaration in `text`.

    Stanza window = [anchor.start(), next_anchor.start()) — robust to
    multiple §6/§29 stanzas appearing back-to-back in one supplement block
    (Senator Crowe sample in findings §5).
    """
    anchors = list(_SECTION_ANCHOR.finditer(text))
    if not anchors:
        return []

    bounds = [m.start() for m in anchors] + [len(text)]
    rows: list[dict] = []
    for i, anchor in enumerate(anchors):
        section = anchor.group(1)
        stanza = text[anchor.start():bounds[i + 1]]
        anchor_local_end = anchor.end() - anchor.start()

        reg_m = _REG_PERIOD.search(stanza)
        registration_period = _collapse(reg_m.group(1))[:300] if reg_m else None

        members = list(_MEMBER.finditer(stanza))
        if not members:
            # Anchor present but no Name of Member — surface as a partial row
            # so the summary flags it. Better than dropping silently.
            rows.append({
                "source_pdf":                 source_pdf,
                "pub_date":                   pub_date,
                "section_trigger":            section,
                "registration_period":        registration_period,
                "member_raw":                 None,
                "interest_code":              None,
                "interest_label":             None,
                "declaration_text":           _collapse(stanza)[:_MAX_DECLARATION] or None,
                "matter_under_consideration": None,
            })
            continue

        member_bounds = [m.start() for m in members] + [len(stanza)]
        for j, member_m in enumerate(members):
            member_raw = member_m.group(1).strip().rstrip(".") or None
            body = stanza[member_m.end():member_bounds[j + 1]]

            cat_m = _CATEGORY.search(body)
            interest_code  = cat_m.group(1) if cat_m else None
            interest_label = _collapse(cat_m.group(2)) if cat_m else None
            decl_text = _collapse(body[cat_m.end():] if cat_m else body)[:_MAX_DECLARATION] or None

            # 'Matter under consideration' is the §29-specific phrase BEFORE
            # the first Name of Member line. §6 has no such concept.
            matter = None
            if section == "29" and j == 0:
                pre = stanza[anchor_local_end:member_m.start()]
                mm = _MATTER.search(pre)
                if mm:
                    matter = _collapse(mm.group(1))[:600]

            rows.append({
                "source_pdf":                 source_pdf,
                "pub_date":                   pub_date,
                "section_trigger":            section,
                "registration_period":        registration_period,
                "member_raw":                 member_raw,
                "interest_code":              interest_code,
                "interest_label":             interest_label,
                "declaration_text":           decl_text,
                "matter_under_consideration": matter,
            })
    return rows


def main(limit: int | None = None) -> None:
    pdfs = sorted(PDF_DIR.glob("[Ii][Rr]*.pdf"))
    if limit:
        pdfs = pdfs[:limit]
    print(f"Scanning {len(pdfs)} PDFs in {PDF_DIR}")

    rows: list[dict] = []
    loaded = 0
    skipped = 0
    pdfs_with_stanzas = 0
    for p in pdfs:
        text = _load_text(p)
        if text is None:
            skipped += 1
            continue
        loaded += 1
        pdf_rows = _extract(text, p.name, _pub_date(p.name))
        if pdf_rows:
            pdfs_with_stanzas += 1
        rows.extend(pdf_rows)

    cols = [
        "source_pdf", "pub_date", "section_trigger", "registration_period",
        "member_raw", "interest_code", "interest_label", "declaration_text",
        "matter_under_consideration",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df.to_csv(OUT_CSV, index=False, encoding="utf-8")

    print(f"\nLoaded {loaded} PDFs (skipped {skipped} <5KB stubs / unreadable).")
    if df.empty:
        print("No member-interest stanzas found. Check the section anchor regex.")
        return

    print(f"Stanzas extracted: {len(df)} across {pdfs_with_stanzas} PDFs.")
    sec_counts = Counter(df["section_trigger"])
    print(f"  Section 6:  {sec_counts.get('6', 0)}")
    print(f"  Section 29: {sec_counts.get('29', 0)}")

    by_year = Counter(d[:4] for d in df["pub_date"].dropna() if d)
    if by_year:
        print(f"  Year span: {dict(sorted(by_year.items()))}")

    distinct_members = df["member_raw"].dropna().nunique()
    print(f"  Distinct members named: {distinct_members}")

    partial = df[df["member_raw"].isna() | df["interest_code"].isna()]
    if not partial.empty:
        print(f"\n  {len(partial)} stanza(s) missing member_raw and/or interest_code:")
        for _, r in partial.iterrows():
            print(
                f"    [{r['source_pdf']}] section={r['section_trigger']} "
                f"member={r['member_raw']!r} code={r['interest_code']!r}"
            )

    print(f"\nWrote {OUT_CSV.relative_to(_ROOT)} ({len(df)} rows)")


if __name__ == "__main__":
    main()
