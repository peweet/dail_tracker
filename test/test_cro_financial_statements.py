"""Unit tests for pipeline_sandbox/cro_financial_statements_extract.normalise.

Pure unit test (no marker, default CI lane). Drives the normaliser with a tiny
in-memory frame mirroring the 8-field CRO index — no network, no fetch. Locks the
contract: company_num typed, dates parsed, accounts_period_end renamed +
period_year derived, and EVENT GRAIN preserved (a company filing twice is kept,
not deduped).
"""

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "pipeline_sandbox"))
from cro_financial_statements_extract import normalise  # noqa: E402


def _src(rows):
    cols = ["file_name", "company_num", "company_name", "submission_num",
            "submission_rec_date", "submission_eff_date", "submission_reg_date",
            "submissions_accounts_to_date"]
    return pl.DataFrame(rows, schema=cols, orient="row")


def test_normalise_types_renames_and_derives():
    df = normalise([_src([
        ["126127218.pdf", "507913", "BLL MANAGEMENT SERVICES LIMITED", "SR1970603",
         "2023-09-21T00:00:00", "2023-09-19T00:00:00", "2023-10-02T00:00:00",
         "2022-12-31T00:00:00"],
    ])])
    assert df.schema["company_num"] == pl.Int64
    assert df.schema["accounts_period_end"] == pl.Date
    assert "submissions_accounts_to_date" not in df.columns  # renamed
    rec = df.row(0, named=True)
    assert rec["company_num"] == 507913
    assert str(rec["accounts_period_end"]) == "2022-12-31"
    assert rec["period_year"] == 2022
    assert rec["file_name"] == "126127218.pdf"


def test_normalise_preserves_event_grain():
    # same company, two filings for the same period (an amendment) — both kept
    df = normalise([_src([
        ["a.pdf", "100", "ACME LTD", "SR1", "2023-01-01T00:00:00",
         "2023-01-01T00:00:00", "2023-01-02T00:00:00", "2022-12-31T00:00:00"],
        ["b.pdf", "100", "ACME LTD", "SR2", "2023-06-01T00:00:00",
         "2023-06-01T00:00:00", "2023-06-02T00:00:00", "2022-12-31T00:00:00"],
    ])])
    assert df.height == 2
    assert df["company_num"].n_unique() == 1


def test_normalise_concats_multiple_years():
    y2022 = _src([["a.pdf", "1", "A LTD", "SR1", "2023-09-21T00:00:00",
                   "2023-09-19T00:00:00", "2023-10-02T00:00:00", "2022-12-31T00:00:00"]])
    y2023 = _src([["b.pdf", "2", "B LTD", "SR2", "2024-09-21T00:00:00",
                   "2024-09-19T00:00:00", "2024-10-02T00:00:00", "2023-12-31T00:00:00"]])
    df = normalise([y2022, y2023])
    assert df.height == 2
    assert set(df["period_year"].to_list()) == {2022, 2023}


def test_normalise_bad_dates_become_null_not_crash():
    df = normalise([_src([
        ["x.pdf", "5", "X LTD", "SR9", "not-a-date", "", "2023-10-02T00:00:00",
         "2022-12-31T00:00:00"],
    ])])
    rec = df.row(0, named=True)
    assert rec["submission_rec_date"] is None
    assert rec["submission_eff_date"] is None
    assert str(rec["accounts_period_end"]) == "2022-12-31"
