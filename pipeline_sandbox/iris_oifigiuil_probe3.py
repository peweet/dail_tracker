"""
iris_oifigiuil_probe3.py — close-out probe.

Goals before we build the extractor:
 1. Locate the 'Supplement to Register of Interests' block(s) and report:
    boundaries (chars), how often, in which PDFs, sample column headers.
 2. Measure block-length distribution after splitting on _{6,} so we can
    pick a defensible 'this block exceeds ~5 pages' threshold in chars.
 3. Final tally: combine the user's nominated categories with any extra
    high-signal categories surfaced by probes 1 + 2.

Uses fitz only.
"""

from __future__ import annotations

import re
import statistics
from collections import Counter
from pathlib import Path

import fitz

# TODO: replace hardcoded path with `BRONZE_DIR / "iris_oifigiuil"` from config when promoting out of experimental
PDF_DIR = Path("C:/Users/pglyn/PycharmProjects/dail_extractor/data/bronze/iris_oifigiuil")
DELIM_RE = re.compile(r"_{6,}")

# ~ chars per page measured empirically from earlier probe (avg ~1k per block,
# 8-page doc → 26 blocks ≈ ~3k chars/page). Five pages ≈ 15k chars.
LONG_BLOCK_CHARS = 15_000

INTEREST_SUPPLEMENT_RE = re.compile(
    r"SUPPLEMENT TO REGISTER OF INTERESTS|"
    r"REGISTER OF INTERESTS OF MEMBERS|"
    r"FORLÍONADH LE CLÁR LEASA",
    re.I,
)


def load(p: Path, max_pages: int = 200) -> tuple[str, int] | None:
    if p.stat().st_size < 5_000:
        return None
    try:
        with fitz.open(p) as doc:
            n = doc.page_count
            if n == 0:
                return None
            return "\n".join(doc[i].get_text() for i in range(min(n, max_pages))), n
    except Exception:
        return None


def main() -> None:
    pdfs = sorted(PDF_DIR.glob("*.pdf"))

    block_lens: list[int] = []
    long_blocks: list[tuple[str, int, int, str]] = []   # (pdf, idx, chars, head)
    interest_blocks: list[tuple[str, int, int, str]] = []  # (pdf, idx, chars, head)
    valid = 0

    for p in pdfs:
        loaded = load(p)
        if loaded is None:
            continue
        text, page_count = loaded
        valid += 1
        blocks = DELIM_RE.split(text)
        for i, b in enumerate(blocks):
            n = len(b)
            block_lens.append(n)
            head = next((ln.strip() for ln in b.splitlines() if ln.strip()), "")[:90]
            if n > LONG_BLOCK_CHARS:
                long_blocks.append((p.name, i, n, head))
            if INTEREST_SUPPLEMENT_RE.search(b):
                interest_blocks.append((p.name, i, n, head))

    print(f"Valid PDFs: {valid}; total blocks: {len(block_lens)}")
    print(f"Block char length — min:{min(block_lens)} median:{statistics.median(block_lens):.0f} "
          f"p90:{statistics.quantiles(block_lens, n=10)[8]:.0f} max:{max(block_lens)}")

    print(f"\n=== Blocks exceeding {LONG_BLOCK_CHARS:,} chars (~5 pages) — {len(long_blocks)} found ===")
    for pdf, idx, n, head in long_blocks[:15]:
        print(f"  {pdf}  block#{idx:3d}  chars={n:6d}  head={head!r}")

    print(f"\n=== 'Supplement to Register of Interests' blocks — {len(interest_blocks)} found in {len({b[0] for b in interest_blocks})} PDFs ===")
    for pdf, idx, n, head in interest_blocks[:10]:
        print(f"  {pdf}  block#{idx:3d}  chars={n:6d}  head={head!r}")

    # Show a representative interest-supplement block snippet (post-header)
    if interest_blocks:
        pdf, idx, _, _ = interest_blocks[0]
        loaded = load(PDF_DIR / pdf)
        if loaded:
            blk = DELIM_RE.split(loaded[0])[idx]
            print(f"\n--- {pdf} block#{idx} first 1200 chars ---")
            # encode-safe print
            print(blk[:1200].encode("ascii", "replace").decode())


if __name__ == "__main__":
    main()
