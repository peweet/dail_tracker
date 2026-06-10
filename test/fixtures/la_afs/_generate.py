"""Regenerate the per-LA AFS test fixture.

la_afs_divisions.parquet — the committed golden fact (multi-year 2016–2025, 22 councils ×
8 divisions) that test/extractors/test_la_afs.py runs its data-integrity invariants against,
so they execute without re-harvesting 31 council websites.

Run AFTER extractors/la_afs_extract.py (which writes the silver parquet). Then:

    python test/fixtures/la_afs/_generate.py

Commit the updated fixture alongside any extractor change in the same PR (same discipline
as the amalgamated-AFS and payments goldens). NOTE: the blanket *.parquet gitignore swallows
it — a negation rule (`!test/fixtures/la_afs/*.parquet`) is required for CI to see it.
"""

from __future__ import annotations

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
FX = Path(__file__).resolve().parent
SILVER_PARQUET = ROOT / "data/silver/parquet/la_afs_divisions.parquet"


def main() -> None:
    shutil.copy(SILVER_PARQUET, FX / "la_afs_divisions.parquet")
    print("regenerated:", *(p.name for p in sorted(FX.glob("*.parquet"))))


if __name__ == "__main__":
    main()
