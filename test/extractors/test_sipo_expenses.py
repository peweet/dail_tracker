"""Data-quality unit tests for the SIPO GE2024 candidate-expenses gold fact
(data/gold/parquet/sipo_expenses_fact.parquet), produced by
extractors/sipo_expenses_paddle_etl.py.

These guard the invariants the data-quality exploration established, so the page
can trust the VERIFIED (flag='ok') rows and the flag system genuinely quarantines
every OCR anomaly. The Green Party total is a hard checksum: its summed
expenditure must equal the figure printed on the form's TOTAL row (€36,729.60).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
FACT = ROOT / "data/gold/parquet/sipo_expenses_fact.parquet"
CONSTIT = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"

STATUTORY_LIMITS = {38900, 48600, 58350}
KNOWN_FLAGS = {
    "ok",
    "no_amount",
    "low_confidence_verify",
    "over_limit_verify",
    "spend_gt_assigned_verify",
    "assigned_over_limit_verify",
}
EXPECTED_COLS = {
    "party",
    "candidate_name_raw",
    "constituency",
    "constituency_match_score",
    "amount_assigned_eur",
    "expenditure_eur",
    "expenditure_confidence",
    "row_min_confidence",
    "statutory_limit_eur",
    "flag",
    "source_pdf",
    "source_page",
}
GREEN_TOTAL = 36_729.60  # printed on the Green Party return's TOTAL row


@pytest.fixture(scope="module")
def df() -> pl.DataFrame:
    if not FACT.exists():
        pytest.skip(f"fact not built: {FACT}")
    return pl.read_parquet(FACT)


@pytest.fixture(scope="module")
def valid_constituencies() -> set[str]:
    return set(pl.read_parquet(CONSTIT)["constituency_name"].to_list())


@pytest.fixture(scope="module")
def verified(df) -> pl.DataFrame:
    return df.filter(pl.col("flag") == "ok")


# ---------------------------------------------------------------- structure ----
def test_schema(df):
    assert EXPECTED_COLS.issubset(set(df.columns)), set(df.columns) ^ EXPECTED_COLS


def test_non_empty(df):
    assert df.height > 200, f"only {df.height} rows — extraction likely incomplete"


def test_flags_known(df):
    bad = set(df["flag"].unique()) - KNOWN_FLAGS
    assert not bad, f"unknown flags: {bad}"


def test_party_non_null(df):
    assert df.filter(pl.col("party").is_null()).height == 0


# --------------------------------------------------------------- anchoring ----
def test_constituency_in_closed_set(df, valid_constituencies):
    bad = df.filter(~pl.col("constituency").is_in(list(valid_constituencies)))
    assert bad.height == 0, bad.select("party", "constituency").to_dicts()


def test_no_duplicate_candidate_rows(df):
    dupes = df.group_by("party", "candidate_name_raw", "constituency").len().filter(pl.col("len") > 1)
    assert dupes.height == 0, dupes.to_dicts()


def test_no_candidate_in_two_parties(df):
    # A person stands for exactly one party in a given constituency at a general election.
    # Key on (name, constituency), NOT name alone: two *different* people can share a name
    # across constituencies (e.g. GE2024 has a Fine Gael "Barry Ward" in Dún Laoghaire and a
    # separate Irish Freedom Party "Barry Ward" in Dublin South-Central). Both are legitimate;
    # only the SAME (name, constituency) appearing under two parties signals a real mismatch.
    named = df.filter(pl.col("candidate_name_raw").str.strip_chars() != "")
    multi = (
        named.group_by("candidate_name_raw", "constituency")
        .agg(pl.col("party").n_unique().alias("n"))
        .filter(pl.col("n") > 1)
    )
    assert multi.height == 0, multi.to_dicts()


def test_constituency_overcount_bounded(df):
    # no party realistically runs >6 candidates in one constituency; a higher count
    # signals constituency mis-matching (a known soft spot — keep it bounded)
    oc = df.group_by("party", "constituency").len().filter(pl.col("len") > 6)
    assert oc.height == 0, oc.sort("len", descending=True).to_dicts()


# ----------------------------------------------------------------- amounts ----
def test_statutory_limit_values(df):
    vals = set(df.filter(pl.col("statutory_limit_eur").is_not_null())["statutory_limit_eur"].to_list())
    assert vals.issubset({float(x) for x in STATUTORY_LIMITS}), vals


def test_expenditure_non_negative(df):
    neg = df.filter(pl.col("expenditure_eur") < 0)
    assert neg.height == 0


def test_confidence_bounds(df):
    for col in ("expenditure_confidence", "row_min_confidence", "constituency_match_score"):
        bad = df.filter((pl.col(col) < 0) | (pl.col(col) > 1))
        assert bad.height == 0, f"{col} out of [0,1]"


# ----- the core guarantee: VERIFIED rows violate NO financial invariant -----
def test_verified_expenditure_within_limit(verified):
    bad = verified.filter(pl.col("expenditure_eur") > pl.col("statutory_limit_eur"))
    assert bad.height == 0, bad.select("party", "candidate_name_raw", "expenditure_eur").to_dicts()


def test_verified_expenditure_within_assigned(verified):
    bad = verified.filter(
        pl.col("amount_assigned_eur").is_not_null() & (pl.col("expenditure_eur") > pl.col("amount_assigned_eur") * 1.02)
    )
    assert bad.height == 0, bad.select(
        "party", "candidate_name_raw", "amount_assigned_eur", "expenditure_eur"
    ).to_dicts()


def test_verified_assigned_within_limit(verified):
    bad = verified.filter(
        pl.col("amount_assigned_eur").is_not_null()
        & (pl.col("amount_assigned_eur") > pl.col("statutory_limit_eur") * 1.02)
    )
    assert bad.height == 0, bad.select("party", "candidate_name_raw", "amount_assigned_eur").to_dicts()


def test_verified_has_amount(verified):
    assert verified.filter(pl.col("expenditure_eur").is_null()).height == 0


def test_flags_actually_flag_violations(df):
    # consistency: every over_limit_verify row really does exceed the limit, etc.
    ol = df.filter(pl.col("flag") == "over_limit_verify")
    assert ol.filter(pl.col("expenditure_eur") <= pl.col("statutory_limit_eur")).height == 0
    sa = df.filter(pl.col("flag") == "spend_gt_assigned_verify")
    assert sa.filter(pl.col("expenditure_eur") <= pl.col("amount_assigned_eur") * 1.02).height == 0


# ------------------------------------------------- checksum & name quality ----
def test_green_party_reconciles(df):
    g = df.filter(pl.col("party") == "Green Party")["expenditure_eur"].sum()
    assert abs(g - GREEN_TOTAL) <= 1.0, f"Green Σ €{g:,.2f} != printed TOTAL €{GREEN_TOTAL:,.2f}"


def test_name_quality(df):
    # a candidate name must not be MERGED (two candidates conflated) or a header —
    # signalled by embedded digits, absurd length, or a header word. An EMPTY name is
    # "missing" not "wrong" (the page shows constituency+amount+verify), allowed below.
    bad = df.filter(
        pl.col("candidate_name_raw").str.contains(r"\d")
        | (pl.col("candidate_name_raw").str.len_chars() > 40)
        | pl.col("candidate_name_raw").str.to_lowercase().is_in(["total", "candidate name", "constituency", "name"])
    )
    assert bad.height == 0, bad.select("party", "candidate_name_raw").to_dicts()


def test_missing_names_are_rare(df):
    # blank names are tolerated (missing, not wrong) but must stay rare (<2%)
    blank = df.filter(pl.col("candidate_name_raw").str.strip_chars() == "").height
    assert blank <= max(1, round(df.height * 0.02)), f"{blank} blank names of {df.height}"
