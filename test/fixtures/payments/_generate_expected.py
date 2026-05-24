"""
Generate the expected-output parquet for the payments golden-file test.

Run this script when:
  (a) you add or change a payment-PDF fixture, or
  (b) the parser intentionally changes output shape and you've confirmed the
      change is correct (CR your own diff).

The committed `.expected.parquet` next to each fixture PDF is the contract.
A subsequent run of `_iter_rows_from_pdf()` against the fixture must produce
identical output. See test/test_payments_golden.py for the assertion.

Usage:
    python test/fixtures/payments/_generate_expected.py
"""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from payments_full_psa_etl import _iter_rows_from_pdf

FIXTURES_DIR = Path(__file__).resolve().parent


def _expected_path(pdf_path: Path) -> Path:
    return pdf_path.with_suffix(".expected.parquet")


def main() -> None:
    pdfs = sorted(FIXTURES_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {FIXTURES_DIR}", file=sys.stderr)
        sys.exit(1)

    for pdf in pdfs:
        rows = [asdict(r) for r in _iter_rows_from_pdf(pdf)]
        if not rows:
            print(f"WARNING: parser yielded zero rows for {pdf.name}")
            continue
        df = pl.from_dicts(rows)
        out = _expected_path(pdf)
        df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
        print(f"Wrote {out.name} — {df.height} rows, {len(df.columns)} cols")


if __name__ == "__main__":
    main()
