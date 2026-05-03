"""
iris_oifigiuil_probe2.py — focused dig:
  1. What are [C-N] / [G-N] block codes?
  2. Where do members declare interests in Iris (if at all)?
  3. Real minister-name patterns ("The Minister for X, Mr Y T.D.")
  4. Block taxonomy: list distinct titles immediately following each delimiter.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from pathlib import Path

import fitz

# TODO: replace hardcoded path with `BRONZE_DIR / "iris_oifigiuil"` from config when promoting out of experimental
PDF_DIR = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")

DELIM_RE = re.compile(r"^[\s_]*_{5,}[\s_]*$", re.M)
CODE_RE  = re.compile(r"\[(?:C|G|S|F|B|P|N|L|R)-\d+\]")
MIN_NAME = re.compile(
    r"(?:The\s)?Minister (?:for|of)\s[A-Z][\w,\s\-]{2,80}?,\s*(?:Mr|Mrs|Ms|Miss|Dr)\s+([A-Z][\w'’\-]+(?:\s[A-Z][\w'’\-]+){0,3})\s+T\.D\.?",
    re.I,
)
INTEREST_HUNT = re.compile(
    r"(?:declar(?:e|ation|es|ed)|disclos(?:e|ed|ure)|register).{0,40}?interest"
    r"|hereby (?:give|gives) notice"
    r"|gift|hospitality|donation",
    re.I,
)

NOISE = re.compile(
    r"Price:\s*€[\d.,]+|Praghas:\s*€[\d.,]+|"
    r"This publication is registered for transmission|"
    r"All notices and advertisements are published in Iris Oifigi.{0,4}il for general information",
    re.I,
)


def load(path: Path, max_pages: int = 60) -> str | None:
    try:
        with fitz.open(path) as doc:
            if doc.page_count == 0:
                return None
            return NOISE.sub("", "\n".join(doc[i].get_text() for i in range(min(doc.page_count, max_pages))))
    except Exception:
        return None


def main() -> None:
    pdfs = [p for p in sorted(PDF_DIR.glob("*.pdf")) if p.stat().st_size > 5_000]

    code_to_first_lines: dict[str, Counter[str]] = defaultdict(Counter)
    block_titles: Counter[str] = Counter()
    minister_examples: list[str] = []
    interest_hits: list[tuple[str, str]] = []

    for p in pdfs:
        text = load(p)
        if not text:
            continue

        # 1) For each [X-N] code, capture line right after it (the section title)
        for m in CODE_RE.finditer(text):
            after = text[m.end(): m.end() + 200]
            line = next((ln.strip() for ln in after.splitlines() if ln.strip()), "")
            if line:
                code_to_first_lines[m.group(0)][line[:80]] += 1

        # 2) Block titles: first non-empty line after each delimiter
        for m in DELIM_RE.finditer(text):
            after = text[m.end(): m.end() + 400]
            line = next((ln.strip() for ln in after.splitlines() if ln.strip()), "")
            line = re.sub(r"^\[(?:C|G|S|F|B|P|N|L|R)-\d+\]\s*", "", line)
            if line and len(line) < 120 and not line.startswith(("Tuesday", "Dé", "or through", "leathanach", "nó")):
                block_titles[line] += 1

        # 3) Real minister regex
        for m in MIN_NAME.finditer(text):
            if len(minister_examples) < 15:
                snippet = re.sub(r"\s+", " ", text[max(0, m.start()-30): m.end()+30]).strip()
                minister_examples.append(snippet)

        # 4) Members-interest hunting
        for m in INTEREST_HUNT.finditer(text):
            ctx = re.sub(r"\s+", " ", text[max(0, m.start()-100): m.end()+200]).strip()
            interest_hits.append((p.name, ctx))

    print("=== Distinct [X-N] code prefixes seen ===")
    prefixes = Counter(c.split("-")[0] for c in code_to_first_lines)
    for px, n in prefixes.most_common():
        print(f"  {px:4s} {n:4d} distinct codes")

    print("\n=== Sample first-line for codes (5 random per prefix) ===")
    for prefix in sorted({c.split("-")[0] for c in code_to_first_lines}):
        sample_codes = sorted(c for c in code_to_first_lines if c.startswith(prefix))[:5]
        for code in sample_codes:
            top_line = code_to_first_lines[code].most_common(1)[0][0]
            print(f"  {code:8s}  {top_line}")

    print(f"\n=== Top 40 block titles (after delimiter) ===")
    for t, n in block_titles.most_common(40):
        print(f"  {n:5d}  {t}")

    print(f"\n=== Minister-name regex hits ({len(minister_examples)}): ===")
    for ex in minister_examples[:10]:
        print(f"  - {ex}")

    print(f"\n=== Interest-related snippets ({len(interest_hits)}); showing 12 ===")
    seen_ctx = set()
    for name, ctx in interest_hits:
        # dedup similar
        key = ctx[:100]
        if key in seen_ctx:
            continue
        seen_ctx.add(key)
        print(f"--- {name}\n  {ctx[:400]}\n")
        if len(seen_ctx) >= 12:
            break


if __name__ == "__main__":
    main()
