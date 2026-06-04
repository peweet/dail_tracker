"""Tests for the per-LA AFS extractor (pipeline_sandbox/la_afs_extract.py).

Two layers (mirrors test_afs_amalgamated.py):
  1. Pure-function unit tests — the year primitives that two real bugs hid in:
     `title_year` (filename year, must unquote %20 so 'Statement%202018' does NOT yield a
     phantom 2020) and `statement_year` (authoritative modal year off the I&E page), plus
     `select_afs` (audited > unaudited). These run in CI (no data files).
  2. Data-integrity invariants on the committed golden fact (19 councils): all rows
     reconcile to the printed total, accounting identity (net = gross − income), 8 divisions
     per council, sign sanity, taxonomy tags, parser provenance, printed-total == Σ gross.
     Skips if the fixture isn't committed yet (gitignore negation deferred).

Regenerate the fixture: re-run la_afs_extract.py, then test/fixtures/la_afs/_generate.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
from la_afs_extract import select_afs, statement_year, title_year  # noqa: E402

FX = ROOT / "test" / "fixtures" / "la_afs" / "la_afs_divisions.parquet"
EUR = 1000  # accounting-identity tolerance (parses are whole euros)


# ---- 1. pure-function unit tests (run in CI) --------------------------------
@pytest.mark.parametrize(
    ("url", "expected"),
    [
        # %20 URL-encoding must be decoded — else '...%202022' → phantom '2020' (the real bug)
        ("https://x/Audited%20Annual%20Financial%20Statements%202022.pdf", 2022),
        ("https://x/Statement%202018.pdf", 2018),
        ("https://x/afs-2024-audited.pdf", 2024),
        ("https://x/AnnualFinancialStatement2025Unaudited-1.pdf", 2025),
        ("https://x/no-year-here.pdf", 0),
    ],
)
def test_title_year(url: str, expected: int):
    assert title_year(url) == expected


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Gross Expenditure 2024 2024 2024 2023 Notes", 2024),  # modal = current year
        ("2019\n2019\n2019\n2018", 2019),
        ("no years on this page", None),
    ],
)
def test_statement_year(text: str, expected: int | None):
    assert statement_year(text) == expected


def test_select_afs_prefers_audited_over_newer_unaudited():
    urls = ["https://x/unaudited-afs-2025.pdf", "https://x/audited-afs-2024.pdf"]
    assert select_afs(urls) == "https://x/audited-afs-2024.pdf"


def test_select_afs_latest_among_audited():
    urls = ["https://x/audited-afs-2022.pdf", "https://x/audited-afs-2024.pdf"]
    assert select_afs(urls) == "https://x/audited-afs-2024.pdf"


# ---- 2. golden-fact invariants ---------------------------------------------
@pytest.fixture(scope="module")
def golden() -> pl.DataFrame:
    if not FX.exists():
        pytest.skip(f"golden fixture not committed yet: {FX}")
    return pl.read_parquet(FX)


def test_all_rows_reconcile(golden: pl.DataFrame):
    """The safety gate: every council in the fact reconciled to its own printed total."""
    assert golden["reconciled"].all()


def test_accounting_identity(golden: pl.DataFrame):
    resid = golden.with_columns(
        (pl.col("gross_expenditure") - pl.col("income") - pl.col("net_expenditure")).abs().alias("r")
    )
    assert resid["r"].max() <= EUR, "net != gross - income"


def test_eight_divisions_per_council_year(golden: pl.DataFrame):
    per = golden.group_by(["council", "year"]).agg(pl.col("division").n_unique().alias("n"))
    assert per["n"].unique().to_list() == [8]


def test_no_duplicate_keys(golden: pl.DataFrame):
    assert golden.group_by(["council", "year", "division"]).len().filter(pl.col("len") > 1).height == 0


def test_no_null_year(golden: pl.DataFrame):
    assert golden.filter(pl.col("year").is_null()).height == 0


def test_no_negative_gross_or_income(golden: pl.DataFrame):
    assert golden.filter(pl.col("gross_expenditure") < 0).height == 0
    assert golden.filter(pl.col("income") < 0).height == 0


def test_printed_total_matches_gross_sum(golden: pl.DataFrame):
    chk = golden.group_by(["council", "year"]).agg(
        pl.col("gross_expenditure").sum().alias("s"), pl.col("printed_total_eur").first().alias("p")
    )
    assert chk.with_columns((pl.col("s") - pl.col("p")).abs().alias("d")).filter(pl.col("d") > 2).height == 0


def test_taxonomy_and_provenance_tags(golden: pl.DataFrame):
    assert (golden["realisation_tier"] == "SPENT").all()
    assert (golden["value_kind"] == "net_expenditure_actual").all()
    assert golden["scope"].str.contains("single-LA").all()
    assert set(golden["parser"].unique().to_list()) <= {"fitz", "camelot"}


def test_canonical_divisions(golden: pl.DataFrame):
    expected = {
        "Housing and Building",
        "Roads, Transportation and Safety",
        "Water Services",
        "Development Management",
        "Environmental Services",
        "Recreation and Amenity",
        "Agriculture, Education, Health & Welfare",
        "Miscellaneous Services",
    }
    assert set(golden["division"].unique().to_list()) == expected
