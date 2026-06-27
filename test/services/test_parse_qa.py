"""Tests for services.parse_qa — the collapsed-cell parse-quality scanner."""

from __future__ import annotations

import polars as pl
import pytest

from services.parse_qa import (
    ParseQAError,
    assert_clean,
    count_huge,
    diagnose_cell,
    scan_frame,
    suggest_split,
)


def test_clean_frame_is_not_flagged():
    df = pl.DataFrame({"name": ["Alice", "Bob"], "note": ["short", "also short"]})
    assert scan_frame(df) == []
    assert_clean(df)  # does not raise


def test_collapsed_cell_is_flagged():
    # 99 short subjects + 1 cell that swallowed a whole page.
    df = pl.DataFrame({"subject": ["Meeting with officials"] * 99 + ["x" * 5000]})
    reports = scan_frame(df)
    assert len(reports) == 1
    r = reports[0]
    assert r.column == "subject"
    assert r.max_len == 5000
    assert r.ratio > 10
    assert r.n_outliers == 1
    with pytest.raises(ParseQAError):
        assert_clean(df)


def test_legit_free_text_is_not_flagged():
    # Every row is long: this is a free-text column, not a parse failure.
    df = pl.DataFrame({"body": ["paragraph " * 100] * 50})
    assert scan_frame(df) == []


def test_allow_exempts_a_column():
    df = pl.DataFrame({"raw_text": ["short"] * 99 + ["x" * 5000]})
    assert scan_frame(df, allow={"raw_text"}) == []
    assert_clean(df, allow={"raw_text"})  # does not raise


def test_tolerate_budget_passes_known_residual_but_trips_on_growth():
    # 5 collapsed cells out of 1000 — a known, sparse residual the ratio catches.
    df = pl.DataFrame({"subject": ["Meeting"] * 995 + ["x" * 600] * 5})
    assert scan_frame(df)[0].n_outliers == 5
    assert_clean(df, tolerate=5, hard_len=10_000)  # within budget -> ok
    with pytest.raises(ParseQAError):
        assert_clean(df, tolerate=4, hard_len=10_000)  # over budget -> raises


def test_hard_len_tripwire_catches_mass_collapse_that_hides_from_ratio():
    # Half the rows collapsed: p99 is now huge, so the max/p99 ratio is ~1 and
    # scan_frame flags nothing — but the absolute hard_len backstop still fires.
    df = pl.DataFrame({"cell": ["x" * 5000] * 50 + ["short"] * 50})
    assert scan_frame(df) == []  # ratio signal is blind here
    assert count_huge(df, hard_len=2000) == 50
    with pytest.raises(ParseQAError):
        assert_clean(df, tolerate=5)


def test_count_huge_respects_allow():
    df = pl.DataFrame({"raw_text": ["x" * 5000] * 10})
    assert count_huge(df, hard_len=2000) == 10
    assert count_huge(df, hard_len=2000, allow={"raw_text"}) == 0


def test_floor_skips_small_columns():
    # 10x outlier but the max is below the absolute floor -> ignored.
    df = pl.DataFrame({"code": ["AB"] * 99 + ["A" * 40]})
    assert scan_frame(df, floor=120) == []


def test_diagnose_merged_records():
    ledger = (
        "Supply of chairs 27355 BARLOW LTD 93,100.00 services 04/11/2021 "
        "27356 BIDEAU LTD 177,557.40 services 05/11/2021 "
        "27349 BRIMWOOD LTD 830,680.00 services 06/11/2021"
    )
    assert diagnose_cell(ledger) == "MERGED_RECORDS"


def test_diagnose_multi_value_delimited():
    suppliers = " | ".join(f"Supplier {i} Ltd" for i in range(10))
    assert diagnose_cell(suppliers) == "MULTI_VALUE_DELIMITED"


def test_suggest_split_pipes():
    text = "Acme Ltd | Beta Ltd | Gamma Ltd"
    assert suggest_split(text) == ["Acme Ltd", "Beta Ltd", "Gamma Ltd"]


def test_suggest_split_leaves_freetext_unchanged():
    text = "a single sentence with no record boundary at all"
    assert suggest_split(text) == [text]
