"""
iris_oifigiuil_single_pdf_dump.py — one-shot reporter.

Run against a single PDF; emit a structured Markdown dump of every block
the splitter detects, the category we'd assign, the body preview, and
specific extractions (SI metadata, member-interest member names).

Designed to be eyeballed against a manual reading of the same PDF.
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime
from pathlib import Path

import fitz

# TODO: replace hardcoded paths with `BRONZE_DIR / "iris_oifigiuil" / "IR310326.pdf"` and `Path(__file__).parent / "iris_oifigiuil_single_pdf_findings.md"` when promoting out of experimental
PDF_PATH = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil/IR310326.pdf")
OUT_MD   = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/pipeline_sandbox/iris_oifigiuil_single_pdf_findings.md")

DELIM_RE   = re.compile(r"_{6,}")
SI_HEAD_RE = re.compile(r"S\.I\. No\. (\d+) of (\d{4})\.?")
CODE_RE    = re.compile(r"\[([CGSLF])-(\d+)\]")

CATEGORY_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("MEMBER_INTEREST_SUPPLEMENT",
        re.compile(r"SUPPLEMENT TO REGISTER OF INTERESTS|FORLÍONADH LE CLÁR LEAS|SECTION (?:6|29) OF THE ETHICS", re.I)),
    ("AGREEMENT_INTO_FORCE",
        re.compile(r"AGREEMENTS? WHICH (?:HAVE )?ENTERED INTO FORCE", re.I)),
    ("EXCHEQUER_STATEMENT",
        re.compile(r"EXCHEQUER (?:STATEMENT|ACCOUNT)|FISCAL MONITOR", re.I)),
    ("COMMISSION_OF_INVESTIGATION",
        re.compile(r"COMMISSION OF INVESTIGATION", re.I)),
    ("REFERENDUM_OR_ELECTION",
        re.compile(r"REFERENDUM|TOGHCHÁN|POLLING DAY ORDER", re.I)),
    ("BILL_SIGNED_BY_PRESIDENT",
        re.compile(r"signed (?:the |above[- ]named )?Bill|TUGADH AN BILLE|RINNE.*?ACHT", re.I)),
    ("APPOINTMENT",
        re.compile(r"^APPOINTMENT(?:S)? (?:AS|OF)\b|CEAPACH[ÁA]N", re.M | re.I)),
    ("POLITICAL_PARTY_REGISTRATION",
        re.compile(r"REGISTRATION OF POLITICAL PARTIES|CLÁRÚ PÁIRTITHE POLAITÍOCHTA", re.I)),
    ("BANKRUPTCY",
        re.compile(r"\bBANKRUPT(?:CY|CIES|S)?\b", re.I)),
    ("WINDING_UP_LIQUIDATION",
        re.compile(r"WINDING[\s\-]?UP|VOLUNTARY LIQUIDATION|MEMBERS'? VOLUNTARY", re.I)),
    ("PROCESS_ADVISER_SCARP",
        re.compile(r"PROCESS ADVISER|SCARP", re.I)),
    ("ICAV_CENTRAL_BANK",
        re.compile(r"\bICAV\b|Central Bank of Ireland", re.I)),
    ("IRISH_STANDARDS",
        re.compile(r"\bIRISH STANDARDS?\b|\bI\.S\. EN\b|NSAI", re.I)),
    ("PLANNING_FORESHORE",
        re.compile(r"FORESHORE ACT|DEVELOPMENT PLAN|PLANNING (?:AUTHORITY|PERMISSION)", re.I)),
    ("FISHING",
        re.compile(r"\b(?:fishery|fisheries|fishing|sea[-\s]?fish|aquaculture)\b", re.I)),
    ("STATUTORY_INSTRUMENT",
        SI_HEAD_RE),
    ("FOGRA_NOTICE",
        re.compile(r"\bFÓGRA\b|^NOTICE\b", re.I | re.M)),
    ("MINISTER_ATTRIBUTION",
        re.compile(r"(?:The\s)?Minister (?:for|of State)\s.{0,80}?(?:Mr|Mrs|Ms|Miss|Dr)\s[A-Z][\w'’\-]+\sT\.D\.?", re.I)),
]
SKIP_BOILERPLATE = re.compile(
    r"All notices and advertisements are published in Iris Oifigi.{0,4}il for general information"
    r"|Communications relating to Iris Oifigi.{0,4}il should be addressed"
    r"|GOVERNMENT PUBLICATIONS,|FOILSEACH.{0,4}IN RIALTAIS,",
    re.I,
)


def pub_date_from_name(name: str) -> str:
    m = re.match(r"[Ii][Rr](\d{2})(\d{2})(\d{2})\.pdf", name)
    if not m: return ""
    dd, mm, yy = m.groups()
    return datetime.strptime(f"{dd}{mm}{yy}", "%d%m%y").date().isoformat()


def first_real_line(block: str) -> str:
    for ln in block.splitlines():
        s = ln.strip()
        if s and not s.isdigit() and not re.fullmatch(r"\[[CGSLF]-\d+\]", s):
            return s[:120]
    return ""


def classify(block: str) -> str:
    if SKIP_BOILERPLATE.search(block):
        return "BOILERPLATE_HEADER_FOOTER"
    for cat, pat in CATEGORY_RULES:
        if pat.search(block):
            return cat
    return "OTHER_UNCLASSIFIED"


def main() -> None:
    pdf = PDF_PATH
    pub_date = pub_date_from_name(pdf.name)
    doc = fitz.open(pdf)
    pages = doc.page_count
    text = "\n".join(doc[i].get_text() for i in range(pages))
    doc.close()

    blocks = DELIM_RE.split(text)
    # secondary split: any block with 2+ SI headings → split on those headings
    expanded: list[str] = []
    for b in blocks:
        si_pos = [m.start() for m in SI_HEAD_RE.finditer(b)]
        if len(si_pos) > 1:
            cuts = [0] + si_pos + [len(b)]
            for i in range(len(cuts) - 1):
                seg = b[cuts[i]:cuts[i+1]]
                if seg.strip(): expanded.append(seg)
        else:
            expanded.append(b)

    rows = []
    for i, b in enumerate(expanded):
        cat = classify(b)
        head = first_real_line(b)
        code_match = CODE_RE.search(b)
        code = f"{code_match.group(1)}-{code_match.group(2)}" if code_match else ""
        si_match = SI_HEAD_RE.search(b)
        si = f"S.I. No. {si_match.group(1)} of {si_match.group(2)}" if si_match else ""
        members = re.findall(r"Name of Member concerned:\s*([^\n\r]+)", b)
        rows.append({
            "idx": i, "category": cat, "code": code, "char_len": len(b),
            "header": head, "si_anchor": si, "members": members,
            "body_preview": re.sub(r"\s+", " ", b[:500]).strip(),
        })

    # category tally
    tally = Counter(r["category"] for r in rows)
    tally_lines = "\n".join(f"| `{c}` | {n} |" for c, n in tally.most_common())

    out = []
    out.append(f"# Single-PDF probe: `{pdf.name}`\n")
    out.append(f"- **Publication date** (from filename): {pub_date}")
    out.append(f"- **Pages**: {pages}")
    out.append(f"- **File size**: {pdf.stat().st_size:,} bytes")
    out.append(f"- **Underscore delimiters (`_{{6,}}`) found**: {len(blocks) - 1}")
    out.append(f"- **Blocks after secondary SI-anchor split**: {len(expanded)}")
    out.append(f"- **Total characters extracted**: {len(text):,}\n")
    out.append("## Category tally\n\n| Category | Block count |\n|---|---|\n" + tally_lines + "\n")
    out.append("## Block-by-block findings\n")
    out.append("| # | Cat | Code | chars | Header | SI | Members declared |")
    out.append("|---|---|---|---:|---|---|---|")
    for r in rows:
        members_str = "; ".join(r["members"]) if r["members"] else ""
        head_safe = r["header"].replace("|", "\\|")
        out.append(
            f"| {r['idx']} | {r['category']} | {r['code']} | {r['char_len']} "
            f"| {head_safe} | {r['si_anchor']} | {members_str} |"
        )

    out.append("\n## Block bodies (first 500 chars, whitespace-collapsed)\n")
    for r in rows:
        out.append(f"### Block {r['idx']} — `{r['category']}`")
        if r["si_anchor"]:
            out.append(f"- SI anchor: **{r['si_anchor']}**")
        if r["members"]:
            out.append(f"- Members declared: **{', '.join(r['members'])}**")
        out.append(f"- Header: _{r['header']}_  · code=`{r['code']}` · chars={r['char_len']}")
        out.append(f"\n```\n{r['body_preview']}\n```\n")

    OUT_MD.write_text("\n".join(out), encoding="utf-8")
    print(f"Wrote {OUT_MD} ({len(rows)} blocks across {len(expanded)} segments).")


if __name__ == "__main__":
    main()
