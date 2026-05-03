"""
iris_oifigiuil_probe.py
-----------------------
Throwaway probe. Scans every PDF in data/bronze/iris_oifigiuil and reports:
  - corrupted / unreadable PDFs (skipped)
  - block headers (line preceding each '__________' delimiter)
  - frequency of candidate category patterns
  - candidate "members interest" declaration sites

Goal: surface evidence to choose 10+ regex categories. NOT for production.
"""

from __future__ import annotations

import re
from collections import Counter
from pathlib import Path

import fitz

# TODO: replace hardcoded path with `BRONZE_DIR / "iris_oifigiuil"` from config when promoting out of experimental
PDF_DIR = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")

# --- splitter -----------------------------------------------------------------
# 5+ underscores possibly with spaces; appears alone on a line
DELIM_RE = re.compile(r"^[\s_]*_{5,}[\s_]*$", re.M)

# --- candidate category sniffers (line-anchored where useful) ----------------
PROBES: dict[str, re.Pattern[str]] = {
    "SI":                  re.compile(r"\bS\.I\. No\. \d+ of \d{4}\b"),
    "MINISTER_TD":         re.compile(r"\b(?:Mr|Mrs|Ms|Miss|Dr)\s[A-Z][a-zA-Z'’\-]+(?:\s[A-Z][a-zA-Z'’\-]+)*\sT\.D\.?"),
    "MINISTER_TITLE":      re.compile(r"Minister (?:for|of State)\s"),
    "AGREEMENTS_FORCE":    re.compile(r"AGREEMENTS? WHICH (?:HAVE )?ENTERED INTO FORCE", re.I),
    "FISHING":             re.compile(r"\b(?:fishery|fisheries|fishing|sea[-\s]?fish|aquaculture)\b", re.I),
    "BANKRUPTCY":          re.compile(r"\bBANKRUPT(?:CY|CIES|S)?\b", re.I),
    "WINDING_UP":          re.compile(r"\bWINDING[\s\-]?UP\b|VOLUNTARY LIQUIDATION", re.I),
    "ICAV_CB":             re.compile(r"\bICAV\b|Central Bank of Ireland", re.I),
    "IRISH_STANDARDS":     re.compile(r"\bIRISH STANDARDS?\b|\bI\.S\. EN\b|NSAI", re.I),
    "FOGRA":               re.compile(r"\bFÓGRA\b|\bNOTICE\b", re.I),
    "BILL_SIGNED":         re.compile(r"signed (?:the )?(?:above[-\s]named )?Bill|TUGADH AN BILLE", re.I),
    "APPOINTMENT":         re.compile(r"^APPOINTMENT(?:S)? (?:AS|OF)\b", re.M | re.I),
    "COMMISSION":          re.compile(r"COMMISSION OF INVESTIGATION", re.I),
    "EXCHEQUER":           re.compile(r"EXCHEQUER (?:STATEMENT|ACCOUNT)|FISCAL MONITOR", re.I),
    "REFERENDUM":          re.compile(r"\bREFERENDUM\b|POLLING DAY ORDER", re.I),
    "PROCESS_ADVISER":     re.compile(r"PROCESS ADVISER|SCARP", re.I),
    "PLANNING":            re.compile(r"DEVELOPMENT PLAN|PLANNING (?:AUTHORITY|PERMISSION)", re.I),
    # interest-disclosure within Iris (key user ask)
    "INTEREST_DECL":       re.compile(r"\bINTEREST(?:S)?\b.{0,80}\b(DECLARE|DISCLOS|REGISTER)\b", re.I | re.S),
    "INTEREST_KEYWORD":    re.compile(r"declaration of interest|disclosable interest|pecuniary interest", re.I),
}

# noise to mask before measuring
NOISE = re.compile(
    r"Price:\s*€[\d.,]+|Praghas:\s*€[\d.,]+|"
    r"This publication is registered for transmission|"
    r"All notices and advertisements are published in Iris Oifigi.{0,4}il for general information|"
    r"GOVERNMENT PUBLICATIONS,|FOILSEACH.{0,4}IN RIALTAIS,",
    re.I,
)


def read_text(path: Path, max_pages: int = 60) -> str | None:
    try:
        with fitz.open(path) as doc:
            if doc.page_count == 0:
                return None
            pages = [doc[i].get_text() for i in range(min(doc.page_count, max_pages))]
        return NOISE.sub("", "\n".join(pages))
    except Exception:
        return None


def main() -> None:
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    valid, corrupt = 0, 0
    cat_counts: Counter[str] = Counter()
    cat_files: dict[str, list[str]] = {k: [] for k in PROBES}
    block_headers: Counter[str] = Counter()
    interest_examples: list[tuple[str, str]] = []  # (pdf, snippet)

    for p in pdfs:
        # skip the obvious corrupted stubs (146-byte HTML error responses) early
        if p.stat().st_size < 5_000:
            corrupt += 1
            continue
        text = read_text(p)
        if not text:
            corrupt += 1
            continue
        valid += 1

        # block headers: line right before each delimiter
        for m in DELIM_RE.finditer(text):
            preceding = text[max(0, m.start() - 200): m.start()].splitlines()
            head = next((ln.strip() for ln in reversed(preceding) if ln.strip()), "")
            if head and len(head) < 120:
                block_headers[head] += 1

        for cat, pat in PROBES.items():
            hits = pat.findall(text)
            if hits:
                cat_counts[cat] += len(hits)
                if len(cat_files[cat]) < 3:
                    cat_files[cat].append(p.name)

        # capture interest-declaration snippets for review
        for pat in (PROBES["INTEREST_DECL"], PROBES["INTEREST_KEYWORD"]):
            for m in pat.finditer(text):
                start = max(0, m.start() - 120)
                end = min(len(text), m.end() + 200)
                snippet = re.sub(r"\s+", " ", text[start:end]).strip()
                interest_examples.append((p.name, snippet))
                if len(interest_examples) >= 25:
                    break

    print(f"PDFs scanned: {len(pdfs)}  valid: {valid}  corrupt/skipped: {corrupt}\n")
    print("=== Category hit totals (across corpus) ===")
    for cat, n in cat_counts.most_common():
        print(f"  {cat:18s} {n:6d}   sample: {cat_files[cat][:2]}")
    print(f"\n=== Top 30 block headers (text immediately before '___' delimiter) ===")
    for h, n in block_headers.most_common(30):
        print(f"  {n:4d}  {h}")
    print(f"\n=== Up to 15 interest-declaration snippets ===")
    for name, snip in interest_examples[:15]:
        print(f"--- {name}\n  {snip}\n")


if __name__ == "__main__":
    main()
