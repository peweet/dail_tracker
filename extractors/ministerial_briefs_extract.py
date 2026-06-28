"""Structure the incoming-minister BRIEF corpus into a queryable dataset (SANDBOX/experimental).

One row per department that publishes an incoming-minister brief on gov.ie (~10 of 18; see
[[project_minister_briefs_corpus]]). Fields: strategic_goals, immediate_priorities,
machinery_of_government, key_issue_areas, vision_mission — the AGENDA the diaries/payments
don't carry. HYBRID provenance:
  - born-digital briefs (8): key_issue_areas auto-extracted from the cached PDF's contents page
    (fitz text); strategic_goals / priorities / MoG curated from the verified reads.
  - scanned briefs (2: DECC, Education): all fields curated from the 2026-06-28 vision read
    (render→Read; Tesseract not installed) — Justice Jan-2025 likewise vision-read.
Sources fetched via the diary extractor's warmed session (gov.ie 403s WebFetch); PDFs cached in
the session scratchpad. This is NOT a fully-automated pipeline (scanned fields need vision) and so
is NOT in pipeline.py — rebuild deliberately. PROMOTED to gold 2026-06-28: writes
data/gold/parquet/minister_briefs.parquet, surfaced via sql_views/diary/minister_briefs.sql ->
v_minister_briefs -> the "Department priorities" section of the ministerial_diaries page.

Run: ./.venv/Scripts/python.exe extractors/ministerial_briefs_extract.py
"""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

OUT_PARQUET = ROOT / "data/gold/parquet/minister_briefs.parquet"  # PROMOTED to gold 2026-06-28
OUT_JSON = ROOT / "data/_meta/minister_briefs.json"
OUT_DIR = OUT_PARQUET.parent

# Curated, VERIFIED brief records (2026-06-27/28). edition + source_type + url per dept; the
# qualitative fields are from the actual reads (born-digital text / scanned vision).
BRIEFS: list[dict] = [
    {
        "department": "Justice, Home Affairs & Migration",
        "slug": "department-of-justice-home-affairs-and-migration",
        "edition": "January 2025",
        "source_type": "scanned",
        "source_url": "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/publications/briefing-document-for-the-minister-for-justice/",
        "vision_mission": None,
        "strategic_goals": [],
        "immediate_priorities": [
            "Transfer of functions (machinery-of-government changes)",
            "Garda recruitment — workforce 14,191 sworn (end-2024) toward 15,000 target",
            "Prison overcrowding — 112% capacity; Thornton prison; +1,500 spaces",
        ],
        "machinery_of_government": [
            "IN: Integration/Accommodation (asylum) from D/CEDIY (~400 staff, €2.2bn)",
            "IN: National Cyber Security Centre from D/ECC (~75 staff, €11m)",
            "IN: National Security Authority from D/Foreign Affairs",
            "OUT: Property Services Regulatory Authority to D/Housing",
            "OUT: Irish Film Classification Office to D/Media",
        ],
        "key_issue_areas": [
            "Civil Justice: Immigration Service Delivery", "Civil Justice: Policy & Legislation",
            "Civil Justice: Governance", "Criminal Justice: Policing", "Criminal Justice: Penal Policy",
            "Criminal Justice: DSGBV/Victims/Youth Justice", "Criminal Justice: Cyber",
            "Security & Northern Ireland", "Justice Service Delivery", "Central: Transparency",
        ],
    },
    {
        "department": "Climate, Energy & Environment (DECC)",
        "slug": "department-of-climate-energy-and-the-environment",
        "edition": "2025", "source_type": "scanned",
        "source_url": "https://www.gov.ie/en/department-of-climate-energy-and-the-environment/organisation-information/briefing-for-ministers-2025/",
        "vision_mission": None,
        "strategic_goals": [
            "Be a recognised leader in climate action",
            "Transform our energy system for a secure and affordable net zero emissions future",
            "Restore, protect and enhance our natural environment",
            "Deliver world class connectivity and communications",
            "Deliver improved cyber security defence and resilience across Government and Society",
            "Develop our people, culture and organisation",
        ],
        "immediate_priorities": [], "machinery_of_government": [],
        "key_issue_areas": ["Climate action", "Energy / net zero", "Natural environment",
                            "Connectivity & communications", "Cyber security (NCSC)", "EU & international"],
    },
    {
        "department": "Education & Youth",
        "slug": "department-of-education",
        "edition": "2025 (Statement of Strategy 2025-2028)", "source_type": "scanned",
        "source_url": "https://www.gov.ie/en/department-of-education/publications/briefing-for-ministers-2025/",
        "vision_mission": "A world-class education system where every child and young person is supported and nurtured to achieve their full potential and where barriers to their learning are removed.",
        "strategic_goals": [
            "Deliver a learning experience for students to highest international standards",
            "Break down barriers for groups at risk of exclusion",
            "Equip learners of all ages and abilities to participate and succeed in a changing world",
            "Support Ireland to be a leader across a broad range of fields",
            "Support students and all those who work in the education sector",
        ],
        "immediate_priorities": [], "machinery_of_government": [],
        "key_issue_areas": ["Curriculum, Assessment & Teacher Professional Learning",
                            "Senior Cycle Redevelopment", "Special Education (NEPS)",
                            "Residential Institutions Redress", "Schools, Social Inclusion & Youth",
                            "Teacher Allocations"],
    },
    {
        "department": "Health",
        "slug": "department-of-health",
        "edition": "2025", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/organisation-information/9071c-department-of-health-divisional-briefing-for-new-minister/",
        "vision_mission": None, "strategic_goals": [],
        "immediate_priorities": [
            "Access to Hospital Services — reduce wait times and lists (Waiting List Action Plan)",
            "Children's Hospital Programme",
            "Reform of Pre-Hospital Emergency Care",
            "National Strategies & associated strategic developments",
            "Development and oversight of Acute Hospital Services",
        ],
        "machinery_of_government": [], "key_issue_areas": [],
    },
    {
        "department": "Further & Higher Education (DFHERIS)",
        "slug": "department-of-further-and-higher-education-research-innovation-and-science",
        "edition": "February 2025", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/department-of-further-and-higher-education-research-innovation-and-science/publications/briefing-for-minister-february-2025/",
        "vision_mission": None,
        "strategic_goals": [
            "Empowerment through Education", "Economic and Social Contribution",
            "Competitiveness and Innovation", "Regional Development", "Future-Oriented Vision",
        ],
        "immediate_priorities": [], "machinery_of_government": [], "key_issue_areas": [],
    },
    {
        "department": "Public Expenditure (DPER)",
        "slug": "department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation",
        "edition": "2025 (Incoming Minister's Brief)", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/organisation-information/incoming-ministers-brief/",
        "vision_mission": None, "strategic_goals": [],
        "immediate_priorities": ["Priorities for the short to medium term (per division)"],
        "machinery_of_government": [],
        "key_issue_areas": [  # divisions, each with "Key Strategic Issues for Incoming Minister"
            "Climate Division", "Expenditure Policy Division", "Public Service Reform",
            "Infrastructure / NDP Delivery", "Digitalisation (OGCIO)", "Public Procurement (OGP)",
        ],
    },
    {
        "department": "Transport",
        "slug": "department-of-transport",
        "edition": "January 2025 (Incoming Ministerial Brief)", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/department-of-transport/publications/incoming-ministerial-brief/",
        "vision_mission": None, "strategic_goals": [],
        "immediate_priorities": [
            "Land Transport Funding", "Transport Decarbonisation — climate commitments & sectoral emissions ceilings",
            "EU, North-South and International engagement",
        ],
        "machinery_of_government": [],
        "key_issue_areas": [  # Strategic Priorities 2025 by pillar
            "Aviation & Emergency Planning", "Transport Investment & Public Transport Policy",
            "Climate Action & EU and International Affairs", "Maritime",
            "Road Transport Services and Digital Hub", "Irish Coast Guard",
            "Corporate Affairs and Central Policy",
        ],
    },
    {
        "department": "Finance",
        "slug": "department-of-finance",
        "edition": "December 2025", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/department-of-finance/publications/ministers-brief-december-2025/",
        "vision_mission": None, "strategic_goals": [],
        "immediate_priorities": [], "machinery_of_government": [],
        "key_issue_areas": [  # divisions
            "Economic / Strategic Economic Development", "Domestic & Indirect Tax Policy",
            "Business & International Tax Policy", "Capital Taxes, Stamp Duties & Residential Zoned Land Tax",
            "Tax Administration, Revenue Powers & Local Property Tax", "Financial Services",
        ],
    },
    {
        "department": "Enterprise, Tourism & Employment (DETE)",
        "slug": "department-of-enterprise-tourism-and-employment",
        "edition": "January 2025", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/department-of-enterprise-tourism-and-employment/collections/department-brief-for-minister/",
        "vision_mission": None, "strategic_goals": [],
        "immediate_priorities": [], "machinery_of_government": [],
        "key_issue_areas": [  # divisions (each "key priorities and strategic issues for…")
            "Commerce, Consumer and Competition", "Corporate Services", "Digital and EU Affairs",
            "Enterprise Strategy, Competitiveness & Evaluation", "Indigenous Enterprise",
            "Innovation and Investment", "Trade",
        ],
    },
    {
        "department": "Defence",
        "slug": "department-of-defence",
        "edition": "January 2025 (2025 Ministerial Brief)", "source_type": "born-digital",
        "source_url": "https://www.gov.ie/en/publication/818d5-2025-ministerial-brief/",
        "vision_mission": None, "strategic_goals": [],
        "immediate_priorities": [], "machinery_of_government": [],
        "key_issue_areas": [  # brief is statutory/organisational rather than priority-led
            "Statutory framework & organisation of Defence", "The Defence Forces (military)",
            "Civil-military relationship", "Bodies under aegis",
        ],
    },
]

# Cached PDFs from the session (born-digital → auto-extract contents headings to fill
# key_issue_areas where curation left it empty). Optional: only used if present.
_CACHE = next(iter(__import__("glob").glob(str(Path.home() / "AppData/Local/Temp/claude/*/*/scratchpad/briefs"))), None)
_CONTENTS_RE = re.compile(r"^\s*(?:\d+(?:\.\d+)*\.?\s+)?([A-Z][A-Za-z][^.]{6,70}?)\s*\.{4,}\s*\d{1,3}\s*$")


def _auto_key_issues(slug: str) -> list[str]:
    """Best-effort: pull dotted-leader contents headings from a cached born-digital brief PDF."""
    if not _CACHE:
        return []
    import fitz

    # cached files were saved under a short name per dept; try a few stems
    stems = {"department-of-finance": "Finance", "department-of-transport": "Transport",
             "department-of-defence": "Defence",
             "department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation": "DPER",
             "department-of-further-and-higher-education-research-innovation-and-science": "DFHERIS",
             "department-of-health": "Health", "department-of-enterprise-tourism-and-employment": "DETE"}
    name = stems.get(slug)
    if not name:
        return []
    p = Path(_CACHE) / f"{name}.pdf"
    if not p.exists():
        return []
    try:
        doc = fitz.open(p)
        txt = "\n".join(doc[i].get_text("text") for i in range(min(8, doc.page_count)))
        doc.close()
    except Exception:
        return []
    out = []
    for ln in txt.splitlines():
        m = _CONTENTS_RE.match(ln.strip())
        if m:
            h = re.sub(r"\s+", " ", m.group(1)).strip()
            toks = h.split()
            # drop letter-spaced font artifacts ("St at ut o r y F r a m e w o r k") — headings
            # where most tokens are 1-2 chars are not real headings.
            if toks and sum(len(t) <= 2 for t in toks) / len(toks) > 0.4:
                continue
            if h.lower() not in {"contents", "table of contents"} and h not in out:
                out.append(h)
    return out[:14]


def main() -> None:
    rows = []
    for b in BRIEFS:
        b = dict(b)
        if not b["key_issue_areas"] and b["source_type"] == "born-digital":
            b["key_issue_areas"] = _auto_key_issues(b["slug"])
        b["n_strategic_goals"] = len(b["strategic_goals"])
        b["n_priorities"] = len(b["immediate_priorities"])
        b["n_mog_changes"] = len(b["machinery_of_government"])
        b["extraction_method"] = "vision-read" if b["source_type"] == "scanned" else "fitz-text + curated"
        rows.append(b)

    cols = ["department", "slug", "edition", "source_type", "source_url", "vision_mission",
            "strategic_goals", "immediate_priorities", "machinery_of_government", "key_issue_areas",
            "n_strategic_goals", "n_priorities", "n_mog_changes", "extraction_method"]
    df = pl.DataFrame(rows).select(cols)
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_PARQUET)
    OUT_JSON.write_text(json.dumps(rows, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {df.height} department briefs -> {OUT_PARQUET}")
    print(f"  born-digital={sum(r['source_type']=='born-digital' for r in rows)} scanned={sum(r['source_type']=='scanned' for r in rows)}")
    print(f"  with strategic_goals={sum(r['n_strategic_goals']>0 for r in rows)} | with priorities={sum(r['n_priorities']>0 for r in rows)} | with MoG={sum(r['n_mog_changes']>0 for r in rows)}")
    with pl.Config(fmt_str_lengths=44, tbl_rows=12):
        print(df.select(["department", "edition", "source_type", "n_strategic_goals", "n_priorities", "n_mog_changes"]))


if __name__ == "__main__":
    main()
