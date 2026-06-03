"""Regenerate the AFS test fixtures.

1. afs_2020_ie_page.txt          — the Income & Expenditure page text (parse target)
2. afs_amalgamated_divisions.parquet — the full 2016–2023 golden series

Run AFTER pipeline_sandbox/afs_amalgamated_extract.py (which produces the sandbox parquet
and caches the per-year PDFs under c:/tmp/afs/). Then:

    python test/fixtures/afs/_generate.py

Commit the updated fixtures alongside any extractor change in the same PR, so a reviewer
sees the data delta next to the code delta (same discipline as the payments golden).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[3]
FX = Path(__file__).resolve().parent
SANDBOX_PARQUET = ROOT / "data/sandbox/parquet/afs_amalgamated_divisions.parquet"
PDF_2020 = Path("c:/tmp/afs/afs_full_2020.pdf")


def main() -> None:
    doc = fitz.open(PDF_2020)
    (FX / "afs_2020_ie_page.txt").write_text(doc[12].get_text("text"), encoding="utf-8")
    doc.close()
    shutil.copy(SANDBOX_PARQUET, FX / "afs_amalgamated_divisions.parquet")
    print("regenerated:", *(p.name for p in sorted(FX.glob("afs_*"))))


if __name__ == "__main__":
    main()
