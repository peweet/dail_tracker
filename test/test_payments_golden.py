"""
Golden-file test for the payments PDF parser (payments_full_psa_etl.py).

The parser is the single most fragile piece of the pipeline. PDF layout
drifts silently — a font change, a column-width nudge, an extra header
row — and the next refresh ships subtly corrupted rows to gold parquet,
then to Streamlit, then to a journalist.

Strategy
--------
Each fixture PDF under `test/fixtures/payments/` has a committed
`.expected.parquet` capturing the parser's exact output. This test re-runs
the parser and asserts the result matches the committed expected output.

If the parser drifts, this test fails loudly. To intentionally accept a
change, regenerate the expected file:

    python test/fixtures/payments/_generate_expected.py

…and commit the new `.expected.parquet` alongside the parser change in the
same PR. A reviewer can then see the data delta next to the code delta.

What this catches (that schema tests don't)
-------------------------------------------
  - Column-extraction logic changes (e.g. _split_position eating a comma).
  - Date parsing regressions (e.g. dd/mm/yyyy → mm/dd/yyyy).
  - Amount parsing (e.g. €4,422.08 → 4422.08).
  - Header-row detection on schema-variant PDFs (Jan-2020 vs Jul-2020 layout).
  - Payment-kind classification logic.

What this does NOT catch
------------------------
  - PDFs not represented in fixtures. Add more `.pdf` + `.expected.parquet`
    pairs to expand coverage to other Oireachtas layout eras.
"""

from __future__ import annotations

import sys
from dataclasses import asdict
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from payments_full_psa_etl import _iter_rows_from_pdf

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "payments"


def _discover_pdf_fixtures() -> list[tuple[Path, Path]]:
    """Find every (pdf, expected_parquet) pair under FIXTURES_DIR."""
    pairs: list[tuple[Path, Path]] = []
    for pdf in sorted(FIXTURES_DIR.glob("*.pdf")):
        expected = pdf.with_suffix(".expected.parquet")
        if expected.exists():
            pairs.append((pdf, expected))
    return pairs


FIXTURES = _discover_pdf_fixtures()


def _parser_output_as_df(pdf_path: Path) -> pl.DataFrame:
    """Run the parser and shape its output the same way the generator does."""
    rows = [asdict(r) for r in _iter_rows_from_pdf(pdf_path)]
    if not rows:
        pytest.fail(f"Parser yielded zero rows for {pdf_path.name}")
    return pl.from_dicts(rows)


@pytest.mark.skipif(not FIXTURES, reason="No fixture PDFs found under test/fixtures/payments/")
@pytest.mark.parametrize("pdf,expected_path", FIXTURES, ids=lambda p: p.name)
def test_payments_parser_matches_golden_output(pdf: Path, expected_path: Path):
    """The parser output must equal the committed expected parquet exactly.

    Frame equality compares column names, dtypes, AND row values in order.
    A regression in any one fails this test with a clear diff.
    """
    actual = _parser_output_as_df(pdf)
    expected = pl.read_parquet(expected_path)

    assert_frame_equal(actual, expected, check_row_order=True, check_column_order=True)


@pytest.mark.skipif(not FIXTURES, reason="No fixture PDFs found under test/fixtures/payments/")
@pytest.mark.parametrize("pdf,expected_path", FIXTURES, ids=lambda p: p.name)
def test_payments_parser_yields_nonzero_rows(pdf: Path, expected_path: Path):
    """Independent sanity check — even if the golden file gets accidentally
    emptied, the parser must produce some rows for a real payment PDF.
    """
    actual = _parser_output_as_df(pdf)
    assert actual.height > 0, f"Parser produced empty output for {pdf.name}"


@pytest.mark.skipif(not FIXTURES, reason="No fixture PDFs found under test/fixtures/payments/")
@pytest.mark.parametrize("pdf,expected_path", FIXTURES, ids=lambda p: p.name)
def test_payments_parser_includes_required_columns(pdf: Path, expected_path: Path):
    """The downstream pipeline reads these columns by name — if any are
    missing the gold parquet write step would silently produce garbage.
    """
    actual = _parser_output_as_df(pdf)
    required = {
        "member_name",
        "position",
        "payment_kind",
        "taa_band_raw",
        "date_paid",
        "amount",
        "source_pdf",
    }
    missing = required - set(actual.columns)
    assert not missing, f"Parser output missing required columns: {missing}"
