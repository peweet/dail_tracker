"""
iris_oifigiuil_evidence.py — answer four targeted questions with corpus evidence.

  Q1. ICAV / Central Bank: do migration / re-domiciliation notices actually exist?
      Cite specific PDFs + counts per year.
  Q2. Standards: is the subject (medical-devices, construction, electrical)
      extractable from notice text? Show samples.
  Q3. Central Bank cancellations: punitive (regulatory failure) or
      voluntary/routine? Read sample wording and classify.
  Q4. Member-Interest Supplement: which TDs are named in Section 6/29
      notices, and is what they declare *also* in the annual register?
      Surface concrete diffs between Iris and member_interests.py output.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

import fitz
import pandas as pd

# TODO: replace hardcoded paths with `BRONZE_DIR / "iris_oifigiuil"` and `SILVER_DIR / "dail_member_interests_combined.csv"` from config when promoting out of experimental
PDF_DIR  = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")
REG_CSV  = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/silver/dail_member_interests_combined.csv")

DELIM = re.compile(r"_{6,}")
SI_HEAD = re.compile(r"S\.I\. No\. \d+ of \d{4}\.?")

NOISE = re.compile(r"All notices and advertisements are published in Iris Oifigi", re.I)


def pub_date(name: str) -> str:
    m = re.match(r"[Ii][Rr](\d{2})(\d{2})(\d{2})\.pdf", name)
    if not m: return ""
    return datetime.strptime("".join(m.groups()), "%d%m%y").date().isoformat()


def load(p: Path) -> str | None:
    if p.stat().st_size < 5_000: return None
    try:
        with fitz.open(p) as doc:
            return "\n".join(doc[i].get_text() for i in range(doc.page_count))
    except Exception:
        return None


def hybrid_blocks(text: str) -> list[str]:
    """Split on _{6,}; for blocks with ≥2 SI headings, re-split on the headings."""
    out: list[str] = []
    for b in DELIM.split(text):
        anchors = [m.start() for m in SI_HEAD.finditer(b)]
        if len(anchors) > 1:
            cuts = [0, *anchors, len(b)]
            for i in range(len(cuts) - 1):
                seg = b[cuts[i]:cuts[i + 1]]
                if seg.strip(): out.append(seg)
        else:
            out.append(b)
    return out


# ---------------------------------------------------------------------------
# Q1 — ICAV / Central Bank migrations
# ---------------------------------------------------------------------------

ICAV_MIGRATION = re.compile(
    r"(re[-\s]?domicili|migration|migrating|continuation under)\b.{0,200}",
    re.I,
)
ICAV_AUTH      = re.compile(r"(authoris(?:ed|ation)|registered as).{0,150}\bICAV\b", re.I)
ICAV_REVOKE    = re.compile(r"(revoc|cancell|withdraw)(?:ation|ed|ing)\b.{0,200}\bauthor", re.I)


def q1_icav(corpus: dict[str, str]) -> dict:
    migrations: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for name, text in corpus.items():
        year = name[-6:-4]
        for m in ICAV_MIGRATION.finditer(text):
            ctx = re.sub(r"\s+", " ", text[max(0, m.start()-80):m.end()+50]).strip()
            if "ICAV" in ctx or "fund" in ctx.lower() or "domicil" in ctx.lower():
                migrations[year].append((name, ctx[:280]))
    return migrations


# ---------------------------------------------------------------------------
# Q2 — Standards subject extractability
# ---------------------------------------------------------------------------

# Standards notices typically read: "I.S. EN 15224:2016 Quality management
# systems for healthcare" — subject is the trailing prose after the colon/year.
IS_EN = re.compile(r"\bI\.S\. EN(?: ISO)? \d+(?:[-:]\d+)?(?::\d{4})?(?:\+A\d+:\d{4})?\s+([^\n]{10,200})")


def q2_standards(corpus: dict[str, str]) -> tuple[Counter[str], list[tuple[str, str]]]:
    """Look for actual per-standard subject lines (not the boilerplate intro)."""
    samples: list[tuple[str, str]] = []
    keywords = Counter()
    # Try multiple shapes: I.S. EN ###, EN ISO ###, I.S. ### only
    code_re = re.compile(r"\b(?:I\.S\.\s+)?EN(?:\s+ISO)?\s+\d{2,5}(?:[-:]\d+)*(?::\d{4})?(?:\+A\d+:\d{4})?\s+([A-Z][^\n]{8,180})")
    sector_map = {
        "medical": ["medical", "healthcare", "implant", "vitro", "device"],
        "construction": ["concrete", "steel", "construction", "cement", "building", "façade", "structural"],
        "electrical": ["electrical", "voltage", "earthing", "low-voltage", "electromagnetic"],
        "food": ["food", "dairy", "cereal", "feed"],
        "energy": ["solar", "photovoltaic", "wind turbine", "energy"],
        "water": ["water", "drinking", "potable", "wastewater"],
    }
    for name, text in corpus.items():
        for m in code_re.finditer(text):
            subject = re.sub(r"\s+", " ", m.group(1)).strip()[:150]
            samples.append((name, subject))
            sl = subject.lower()
            for sector, kws in sector_map.items():
                if any(k in sl for k in kws):
                    keywords[sector] += 1
                    break
            else:
                keywords["other_or_unknown"] += 1
    return keywords, samples


# ---------------------------------------------------------------------------
# Q3 — Central Bank cancellations: voluntary or punitive?
# ---------------------------------------------------------------------------

CB_REVOKE = re.compile(
    r"(revoc(?:ation|ed)|cancel(?:lation|led)|withdraw(?:al|n))[^\n]{0,200}",
    re.I,
)


def q3_cancellations(corpus: dict[str, str]) -> list[tuple[str, str, str]]:
    """Returns (pdf, classification, text) for each cancellation notice found."""
    results: list[tuple[str, str, str]] = []
    voluntary = re.compile(r"\bvoluntar|own request|surrender|merger|amalgamation|scheme of arrangement", re.I)
    punitive = re.compile(r"\bdirection|breach|fail(?:ed|ure)|sanction|prosecution|enforcement|inadequate|materially", re.I)
    for name, text in corpus.items():
        for m in CB_REVOKE.finditer(text):
            ctx_lo = max(0, m.start() - 200)
            ctx_hi = min(len(text), m.end() + 200)
            ctx = re.sub(r"\s+", " ", text[ctx_lo:ctx_hi]).strip()
            if "Central Bank" not in ctx and "Authority" not in ctx and "Authorisation" not in ctx:
                continue
            cls = "voluntary" if voluntary.search(ctx) else (
                "punitive" if punitive.search(ctx) else "unspecified"
            )
            results.append((name, cls, ctx[:320]))
    return results


# ---------------------------------------------------------------------------
# Q4 — Member-Interest Supplement vs annual register diff
# ---------------------------------------------------------------------------

SUPPLEMENT = re.compile(r"SUPPLEMENT TO REGISTER OF INTERESTS|FORLÍONADH LE CLÁR LEAS", re.I)
MEMBER_RX  = re.compile(r"Name of Member concerned:\s*([^\n\r]+)", re.I)
SECTION_RX = re.compile(r"STATEMENT UNDER SECTION (6|29) OF THE ETHICS", re.I)
CAT_RX     = re.compile(r"^\s*(\d)\s*[-–]\s*([A-Z][A-Z\s\(\)/]+)", re.M)


def q4_member_interests(corpus: dict[str, str], reg: pd.DataFrame) -> list[dict]:
    declarations: list[dict] = []
    for name, text in corpus.items():
        for blk in hybrid_blocks(text):
            if not SUPPLEMENT.search(blk): continue
            section_m = SECTION_RX.search(blk)
            section = section_m.group(1) if section_m else "?"
            for mm in MEMBER_RX.finditer(blk):
                member = mm.group(1).strip().rstrip(".")
                # find the category for this member's stanza
                rest = blk[mm.end(): mm.end() + 1200]
                cat_m = CAT_RX.search(rest)
                cat_code = cat_m.group(1) if cat_m else None
                cat_label = cat_m.group(2).strip() if cat_m else None
                # capture ~400 chars of declaration prose after category
                body = re.sub(r"\s+", " ", rest[cat_m.end() if cat_m else 0:][:400]).strip()
                declarations.append({
                    "pdf": name, "pub_date": pub_date(name), "section": section,
                    "member_raw": member, "interest_code": cat_code,
                    "interest_label": cat_label, "body": body,
                })
    # Compare against the annual register: for each declaration, is there a
    # matching row in `reg` (same person, same interest_code, same year)?
    reg_norm = reg.copy()
    reg_norm["_name"] = reg_norm["full_name"].str.lower().str.replace(r"[^a-z]", "", regex=True)
    reg_norm["interest_code"] = reg_norm["interest_code"].astype(str)
    out = []
    for d in declarations:
        name_norm = re.sub(r"[^a-z]", "", d["member_raw"].lower())
        if not name_norm:
            hits = pd.DataFrame()
        else:
            stub = name_norm[:10]
            hits = reg_norm[reg_norm["_name"].str.contains(stub, na=False, regex=False)]
        if d["interest_code"]:
            hits = hits[hits["interest_code"] == d["interest_code"]]
        # restrict to the registration year (Iris pub_date year - 1, since
        # supplements typically cite "registration period 1 Jan YYYY-1 to 31 Dec YYYY-1")
        try:
            yr = int(d["pub_date"][:4]) - 1
            year_hits = hits[hits["year_declared"] == yr]
        except Exception:
            year_hits = hits
        d["matches_in_annual_register"] = len(year_hits)
        d["sample_register_text"] = (
            year_hits["interest_description_cleaned"].iloc[0][:200]
            if len(year_hits) else ""
        )
        out.append(d)
    return out


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------

def main() -> None:
    print("Loading corpus...")
    corpus: dict[str, str] = {}
    for p in sorted(PDF_DIR.glob("*.pdf")):
        text = load(p)
        if text:
            corpus[p.name] = NOISE.sub("", text)
    print(f"  loaded {len(corpus)} valid PDFs")

    # ---- Q1 ------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("Q1. ICAV / Central Bank — do migration notices exist?")
    print("=" * 78)
    migs = q1_icav(corpus)
    for yr in sorted(migs):
        print(f"  20{yr}: {len(migs[yr])} migration-language hits")
    # show 3 actual examples
    print("\n  Sample wording (first 3 hits):")
    for hits in migs.values():
        for pdf, ctx in hits[:1]:
            print(f"    [{pdf}] {ctx[:240]}")
            break

    # ---- Q2 ------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("Q2. Standards — is the subject extractable?")
    print("=" * 78)
    sectors, samples = q2_standards(corpus)
    print(f"  Total I.S. EN matches: {len(samples)}")
    print(f"  Sector distribution by keyword on subject text:")
    for sec, n in sectors.most_common():
        print(f"    {sec:20s} {n}")
    print("\n  Random 10 (pdf, subject):")
    for pdf, subj in samples[::max(1, len(samples)//10)][:10]:
        print(f"    [{pdf}] {subj}")

    # ---- Q3 ------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("Q3. Central Bank cancellations — voluntary or punitive?")
    print("=" * 78)
    cancels = q3_cancellations(corpus)
    cls_counts = Counter(c[1] for c in cancels)
    print(f"  Total cancellation/revocation language hits: {len(cancels)}")
    print(f"  Classification: {dict(cls_counts)}")
    print("\n  Sample of each class:")
    seen = set()
    for pdf, cls, ctx in cancels:
        if cls in seen: continue
        seen.add(cls)
        print(f"    [{cls}] [{pdf}]  {ctx[:280]}")
        if len(seen) >= 3: break

    # ---- Q4 ------------------------------------------------------------------
    print("\n" + "=" * 78)
    print("Q4. Member-Interest Supplement vs annual register")
    print("=" * 78)
    if not REG_CSV.exists():
        print(f"  Skipping — {REG_CSV} not found.")
        return
    reg = pd.read_csv(REG_CSV)
    diffs = q4_member_interests(corpus, reg)
    print(f"  Total Iris declarations parsed: {len(diffs)}")
    no_match = [d for d in diffs if d["matches_in_annual_register"] == 0]
    has_match = [d for d in diffs if d["matches_in_annual_register"] >= 1]
    print(f"  Iris declarations with NO matching annual-register row: {len(no_match)}")
    print(f"  Iris declarations WITH a matching annual-register row: {len(has_match)}")
    print("\n  Up to 6 'Iris-only' declarations (member appears in supplement but no matching annual entry):")
    for d in no_match[:6]:
        print(f"\n    PDF: {d['pdf']}  ({d['pub_date']})  Section {d['section']}")
        print(f"    Member: {d['member_raw']}")
        print(f"    Interest: {d['interest_code']} — {d['interest_label']}")
        print(f"    Iris body: {d['body'][:280]}")
    print("\n  Up to 3 'matched' declarations to compare wording:")
    for d in has_match[:3]:
        print(f"\n    PDF: {d['pdf']}  Member: {d['member_raw']}  Interest: {d['interest_code']}")
        print(f"    Iris :  {d['body'][:200]}")
        print(f"    Annual: {d['sample_register_text'][:200]}")


if __name__ == "__main__":
    main()
