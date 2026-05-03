"""
iris_oifigiuil_value_probe.py — surface additional value categories.

Hunts for content categories not yet covered in the main category list,
to surface anything else with potential business value or intrinsic
journalistic / civic interest. Reports counts + sample wording.

Tested ideas:
  V1.  Revenue Commissioners forfeitures / seizures  (Finance Acts §§139–142)
  V2.  Tax defaulters / penalty publication           (statutory quarterly list)
  V3.  Defence Forces commissions / promotions       (Óglaigh na hÉireann)
  V4.  Coroner appointments
  V5.  Charity / co-operative registrations
  V6.  Trade union registrations & cancellations
  V7.  Foreshore Act licences (offshore wind, ports, marinas)
  V8.  Naturalisation / citizenship grants
  V9.  Bills brought to the President for signature  (Act commencement timeline)
  V10. Tribunals & inquiries — terms of reference, sole-member appointments
  V11. Dáil / Seanad sittings adjournments / writs of election (by-elections)
  V12. State pensions Authority schemes
  V13. National Lottery prize claims notifications
  V14. Court rule changes (Rules of the Superior Courts)
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from pathlib import Path

import fitz

PDF_DIR = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")


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


CATEGORIES: dict[str, re.Pattern[str]] = {
    "V1_revenue_forfeiture":   re.compile(r"Section 14[12] of the Finance Act 2001|seized by Officers of the Revenue Commissioners", re.I),
    "V2_tax_defaulters":       re.compile(r"List of Tax Defaulters|Section 1086|Defaulters Lists?", re.I),
    "V3_defence_forces":       re.compile(r"\bÓglaigh na hÉireann\b|\bDEFENCE FORCES\b|commissioned officers", re.I),
    "V4_coroner":              re.compile(r"\bCORONER\b|Coroners Act", re.I),
    "V5_charity_coop":         re.compile(r"Industrial and Provident Societies Act|Friendly Societies|Co-?operative Society", re.I),
    "V6_trade_union":          re.compile(r"Trade Union Act|TRADE UNION", re.I),
    "V7_foreshore":            re.compile(r"FORESHORE ACT|foreshore licen|foreshore lease", re.I),
    "V8_naturalisation":       re.compile(r"Irish Nationality and Citizenship Act|certificate of naturalisation|citizenship", re.I),
    "V9_bills_to_president":   re.compile(r"Bill[s]? (?:will be|have been|were) presented to the President|presented to the President for|Tugadh an Bille", re.I),
    "V10_tribunals_inquiries": re.compile(r"COMMISSION OF INVESTIGATION|TRIBUNAL OF INQUIRY|terms of reference|sole member", re.I),
    "V11_byelection_writ":     re.compile(r"writ for the holding of a by-?election|moved that a writ|by-?election", re.I),
    "V12_pensions_authority":  re.compile(r"Pensions Authority|occupational pension scheme|pension scheme registration", re.I),
    "V13_national_lottery":    re.compile(r"National Lottery|unclaimed prize", re.I),
    "V14_court_rules":         re.compile(r"Rules of the Superior Courts|Rules of the Circuit Court|Rules of the District Court", re.I),
    # bonus surfaces from prior probe top-headers
    "V15_high_court_orders":   re.compile(r"^THE HIGH COURT$", re.M),
    "V16_friendly_societies":  re.compile(r"REGISTRY OF FRIENDLY SOCIETIES", re.I),
    "V17_planning_appeals":    re.compile(r"AN BORD ACHOMHAIRC UM SHEIRBH|Planning Appeals Board|An Bord Pleanála", re.I),
    "V18_envir_protection":    re.compile(r"GNÍOMHAIREACHT UM CHAOMHN|Environmental Protection Agency", re.I),
    "V19_to_whom_concerns":    re.compile(r"^TO WHOM IT CONCERNS$", re.M),  # often Revenue forfeitures
    "V20_political_funding":   re.compile(r"Standards in Public Office Commission|SIPO|Electoral Acts", re.I),
}


def main() -> None:
    print("Loading corpus...")
    corpus = {p.name: load(p) for p in sorted(PDF_DIR.glob("*.pdf")) if p.stat().st_size >= 5_000}
    corpus = {k: v for k, v in corpus.items() if v}
    print(f"  loaded {len(corpus)} valid PDFs\n")

    totals: Counter[str] = Counter()
    pdfs_with: dict[str, set[str]] = {k: set() for k in CATEGORIES}
    samples: dict[str, list[tuple[str, str]]] = {k: [] for k in CATEGORIES}

    for name, text in corpus.items():
        for cat, pat in CATEGORIES.items():
            for m in pat.finditer(text):
                totals[cat] += 1
                pdfs_with[cat].add(name)
                if len(samples[cat]) < 3:
                    ctx = re.sub(r"\s+", " ", text[max(0, m.start()-80):m.end()+260]).strip()
                    samples[cat].append((name, ctx[:340]))

    print("=== Counts (occurrences across corpus) ===")
    print(f"{'category':30s} {'hits':>7s} {'pdfs':>5s}")
    for cat, n in totals.most_common():
        print(f"{cat:30s} {n:7d} {len(pdfs_with[cat]):5d}")

    print("\n=== Sample wording (first 2 per non-empty category) ===")
    for cat in totals:
        for name, ctx in samples[cat][:2]:
            print(f"\n  [{cat}] [{name}]")
            print(f"  {ctx}")


if __name__ == "__main__":
    main()
