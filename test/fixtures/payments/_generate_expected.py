"""
Generate the expected-output parquet for the payments golden-file test.

Run this script when:
  (a) you add or change a payment-PDF fixture, or
  (b) the parser intentionally changes output shape and a reviewer must inspect
      the output against the source PDF.

By default, this writes a `.candidate.expected.parquet` next to each fixture.
Independently review that candidate against the source PDF before using
`--replace-reviewed` to copy those exact reviewed candidate bytes into the
`.expected.parquet` contract without rerunning the parser.

Usage:
    python test/fixtures/payments/_generate_expected.py
    python test/fixtures/payments/_generate_expected.py --replace-reviewed
"""

from __future__ import annotations

import argparse
import shutil
import sys
from dataclasses import asdict
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
from payments.payments_full_psa_etl import _iter_rows_from_pdf

FIXTURES_DIR = Path(__file__).resolve().parent


def _expected_path(pdf_path: Path) -> Path:
    return pdf_path.with_suffix(".expected.parquet")


def output_path(pdf_path: Path, *, replace_reviewed: bool) -> Path:
    """Keep a candidate separate until a reviewer accepts it as the contract."""
    if replace_reviewed:
        return _expected_path(pdf_path)
    return pdf_path.with_suffix(".candidate.expected.parquet")


def promote_candidate(pdf_path: Path) -> Path:
    """Copy the independently reviewed candidate into the golden contract."""
    candidate = output_path(pdf_path, replace_reviewed=False)
    reviewed = output_path(pdf_path, replace_reviewed=True)
    if not candidate.is_file():
        raise FileNotFoundError(
            f"Missing reviewed candidate {candidate.name}; generate and inspect it before promotion"
        )
    shutil.copyfile(candidate, reviewed)
    return reviewed


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--replace-reviewed",
        action="store_true",
        help="replace the reviewed expectation after independent source-PDF review",
    )
    args = parser.parse_args(argv)
    pdfs = sorted(FIXTURES_DIR.glob("*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {FIXTURES_DIR}", file=sys.stderr)
        sys.exit(1)

    if args.replace_reviewed:
        missing = [pdf for pdf in pdfs if not output_path(pdf, replace_reviewed=False).is_file()]
        if missing:
            missing_names = ", ".join(pdf.name for pdf in missing)
            parser.error(f"missing reviewed candidate output for: {missing_names}")
        for pdf in pdfs:
            out = promote_candidate(pdf)
            print(f"Promoted {out.name} from its reviewed candidate")
        return

    for pdf in pdfs:
        rows = [asdict(r) for r in _iter_rows_from_pdf(pdf)]
        if not rows:
            print(f"WARNING: parser yielded zero rows for {pdf.name}")
            continue
        df = pl.from_dicts(rows)
        out = output_path(pdf, replace_reviewed=args.replace_reviewed)
        df.write_parquet(out, compression="zstd", compression_level=3, statistics=True)
        print(f"Wrote {out.name} — {df.height} rows, {len(df.columns)} cols")


if __name__ == "__main__":
    main()
