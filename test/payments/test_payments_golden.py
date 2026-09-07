"""Golden-file contracts for the payments PDF parser.

The parser is especially vulnerable to PDF layout drift. A font, column width,
or header change can silently corrupt rows sent to gold parquet and downstream
users.

Each locally held fixture PDF can have a reviewed `.expected.parquet` contract.
This integration test reruns the parser and compares its output to that reviewed
contract. Fixture PDFs and their reviewed outputs are intentionally not required
in a fresh checkout's default fast lane; the explicit integration task fails
closed if its local fixture source is absent.

To propose an intentional parser change, create a candidate output:

    python test/fixtures/payments/_generate_expected.py

Independently inspect the candidate against its source PDF. Only then replace
the reviewed contract deliberately:

    python test/fixtures/payments/_generate_expected.py --replace-reviewed

This catches extraction, date, amount, header, and payment-kind regressions for
the fixture layouts represented locally. It cannot cover a PDF layout without a
reviewed fixture pair.
"""

from __future__ import annotations

import os
import sys
from dataclasses import asdict
from pathlib import Path

import polars as pl
import pytest
from polars.testing import assert_frame_equal

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from payments.payments_full_psa_etl import _iter_rows_from_pdf

pytestmark = pytest.mark.integration

FIXTURES_DIR = Path(__file__).resolve().parents[1] / "fixtures" / "payments"


def _discover_pdf_fixtures() -> list[tuple[Path, Path]]:
    """Find every reviewed (PDF, expected parquet) pair under FIXTURES_DIR."""
    pairs: list[tuple[Path, Path]] = []
    for pdf in sorted(FIXTURES_DIR.glob("*.pdf")):
        expected = pdf.with_suffix(".expected.parquet")
        if expected.exists():
            pairs.append((pdf, expected))
    return pairs


FIXTURES = _discover_pdf_fixtures()


def test_reviewed_payment_fixture_is_available_for_the_explicit_integration_lane() -> None:
    """Do not let the official local integration command pass on a missing contract."""
    if os.environ.get("DAIL_INTEGRATION_TESTS") != "1":
        pytest.skip("run through tools/dev.py test-integration to require local payment fixtures")
    assert FIXTURES, (
        "No reviewed payment PDF/parquet fixture is available. Supply the approved local fixture source "
        "before treating the integration lane as evidence."
    )


def _parser_output_as_df(pdf_path: Path) -> pl.DataFrame:
    """Run the parser and shape its output the same way the generator does."""
    rows = [asdict(row) for row in _iter_rows_from_pdf(pdf_path)]
    if not rows:
        pytest.fail(f"Parser yielded zero rows for {pdf_path.name}")
    return pl.from_dicts(rows)


@pytest.mark.skipif(not FIXTURES, reason="No reviewed fixture PDFs found under test/fixtures/payments/")
@pytest.mark.parametrize("pdf,expected_path", FIXTURES, ids=lambda path: path.name)
def test_payments_parser_matches_golden_output(pdf: Path, expected_path: Path) -> None:
    """The parser output must equal the reviewed expected parquet exactly."""
    actual = _parser_output_as_df(pdf)
    expected = pl.read_parquet(expected_path)

    assert_frame_equal(actual, expected, check_row_order=True, check_column_order=True)


@pytest.mark.skipif(not FIXTURES, reason="No reviewed fixture PDFs found under test/fixtures/payments/")
@pytest.mark.parametrize("pdf,expected_path", FIXTURES, ids=lambda path: path.name)
def test_payments_parser_yields_nonzero_rows(pdf: Path, expected_path: Path) -> None:
    """A reviewed fixture must never parse to an empty output."""
    actual = _parser_output_as_df(pdf)
    assert actual.height > 0, f"Parser produced empty output for {pdf.name}"


@pytest.mark.skipif(not FIXTURES, reason="No reviewed fixture PDFs found under test/fixtures/payments/")
@pytest.mark.parametrize("pdf,expected_path", FIXTURES, ids=lambda path: path.name)
def test_payments_parser_includes_required_columns(pdf: Path, expected_path: Path) -> None:
    """The parser must retain the downstream columns used by the pipeline."""
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
