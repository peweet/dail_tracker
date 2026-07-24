"""Contract for the AFS capital-account FUNDING SPLIT (grants vs borrowing vs other).

Added 2026-07-18. The capital appendix is a 10-column statutory template, but this extractor
historically kept only opening balance / expenditure / total income — discarding WHICH source
funded the spend. Grant-funded and debt-funded capital are different stories: a loan-funded
spike is a discretionary council borrowing decision, a grant-funded one is usually a national
scheme.

The risk this contract guards is mis-attribution. The PARSED column count varies (10 vs 9)
because a column empty in every row forms no x-cluster, so positional arithmetic can silently
shift "borrowing" onto the wrong council. Saying a council borrowed €92m when it did not is a
false statement about a real public body, so the extractor assigns names ONLY when all three
components are present AND they reconcile to that row's own Total Income; otherwise all three
are NULL. These tests pin exactly that behaviour:

  * the pure function refuses ambiguous / non-reconciling layouts (no data needed);
  * on real gold, every populated split reconciles to capital_income;
  * the ground-truth row survives: Galway City 2022 Miscellaneous Services, €45,500,000 of
    non-mortgage loans against €44,715,840 of expenditure (verified against
    data/bronze/pdfs/la_afs/galway_city/2022.pdf p37).
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from extractors.la_afs_capital_extract import RECON_TOL, _funding_split  # noqa: E402

_PARQUET = Path("data/silver/parquet/la_afs_capital_divisions.parquet")
_FUNDING = ("grants_lpt", "non_mortgage_loans", "other_income")


# --------------------------------------------------------------------------- pure function
def test_three_components_reconciling_are_assigned_in_statutory_order():
    # [open, exp, grants, loans, other, income]; 20+30+5 == 55
    vals = [1.0, 99.0, 20.0, 30.0, 5.0, 55.0]
    assert _funding_split(vals, 5) == (20.0, 30.0, 5.0)


def test_nil_component_is_tolerated_when_the_rest_reconcile():
    """A '-' cell parses to None — the Galway shape (nil grants, loan + other == income)."""
    vals = [1.0, 44715840.0, None, 45500000.0, 947068.0, 46447068.0]
    assert _funding_split(vals, 5) == (None, 45500000.0, 947068.0)


def test_two_component_layout_is_refused_not_guessed():
    """The 9-column parse (a collapsed column). Which source went missing is genuinely
    ambiguous, and mislabelling borrowing as 'other' would be a false statement."""
    vals = [1.0, 99116316.0, 66493311.0, 92311729.0, 158805040.0]
    assert _funding_split(vals, 4) == (None, None, None)


def test_non_reconciling_row_is_refused():
    vals = [1.0, 99.0, 20.0, 30.0, 5.0, 999.0]  # 55 != 999
    assert _funding_split(vals, 5) == (None, None, None)


@pytest.mark.parametrize("inc_c", [None, 99])
def test_missing_or_out_of_range_income_column_is_refused(inc_c):
    assert _funding_split([1.0, 2.0, 3.0, 4.0, 5.0, 6.0], inc_c) == (None, None, None)


def test_null_income_is_refused():
    assert _funding_split([1.0, 99.0, 20.0, 30.0, 5.0, None], 5) == (None, None, None)


# --------------------------------------------------------------------------- real gold
@pytest.fixture(scope="module")
def conn():
    if not _PARQUET.exists():
        pytest.skip("la_afs_capital_divisions not built (run extractors/la_afs_capital_extract.py)")
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture(scope="module")
def rel() -> str:
    return f"read_parquet('{_PARQUET.as_posix()}')"


@pytest.mark.sql
def test_funding_columns_exist(conn, rel):
    cols = {d[0] for d in conn.execute(f"SELECT * FROM {rel} LIMIT 0").description}
    assert set(_FUNDING) <= cols


@pytest.mark.sql
def test_every_populated_split_reconciles_to_capital_income(conn, rel):
    """The invariant that makes the columns trustworthy: where we DID assign a split, its
    components must add up to the income we recorded for that same row."""
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} WHERE (grants_lpt IS NOT NULL OR non_mortgage_loans IS NOT NULL "
        f"OR other_income IS NOT NULL) AND abs("
        f"COALESCE(grants_lpt,0) + COALESCE(non_mortgage_loans,0) + COALESCE(other_income,0) "
        f"- COALESCE(capital_income,0)) >= {RECON_TOL}"
    ).fetchone()[0]
    assert bad == 0, f"{bad} rows carry a funding split that does not sum to capital_income"


@pytest.mark.sql
def test_negative_funding_is_rare_and_still_reconciles(conn, rel):
    """Negative income components are LEGITIMATE here — do not assert non-negativity.

    An initial version of this test asserted grants/loans >= 0 and failed on 8 real rows.
    They are genuine accounting adjustments (grant clawbacks, loan corrections booked as
    negative income), and each still reconciles: e.g. Donegal 2022 Housing and Building
    grants 27,176,254 + loans -211,220 + other 875,216 = 27,840,250 = its capital_income.
    So the meaningful guard is that they stay RARE (a flood would mean a sign/parse bug),
    with correctness itself covered by the reconcile test above.
    """
    total, neg = conn.execute(
        f"SELECT count(*) FILTER (WHERE grants_lpt IS NOT NULL OR non_mortgage_loans IS NOT NULL), "
        f"count(*) FILTER (WHERE grants_lpt < 0 OR non_mortgage_loans < 0) FROM {rel}"
    ).fetchone()
    assert total > 0
    assert neg / total < 0.10, f"{neg}/{total} funding rows negative — suspect a sign/parse bug"


@pytest.mark.sql
def test_ground_truth_galway_city_2022_loan_survives(conn, rel):
    """The row that motivated the work — a debt-financed acquisition hiding inside a bland
    'Miscellaneous Services' division total."""
    row = conn.execute(
        f"SELECT capital_expenditure, grants_lpt, non_mortgage_loans, other_income, capital_income "
        f"FROM {rel} WHERE council='Galway City' AND year=2022 AND division='Miscellaneous Services'"
    ).fetchone()
    assert row is not None, "Galway City 2022 Miscellaneous Services row missing"
    exp, grants, loans, other, income = row
    assert exp == pytest.approx(44715840.0)
    assert loans == pytest.approx(45500000.0)
    assert grants is None, "grants were nil ('-') in the source appendix"
    assert other == pytest.approx(947068.0)
    assert income == pytest.approx(46447068.0)


@pytest.mark.sql
def test_split_coverage_has_not_collapsed(conn, rel):
    """Ratchet: ~45% of rows carried a reconciled split at build time (417/947). A collapse to
    near-zero means the layout assumption or the parser broke, not that councils changed."""
    total, split = conn.execute(
        f"SELECT count(*), count(*) FILTER (WHERE grants_lpt IS NOT NULL OR non_mortgage_loans "
        f"IS NOT NULL OR other_income IS NOT NULL) FROM {rel}"
    ).fetchone()
    assert total > 0
    assert split / total > 0.25, f"funding-split coverage collapsed to {split}/{total}"
