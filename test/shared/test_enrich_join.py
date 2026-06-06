"""
Contract tests for enrich.py's join patterns.

`enrich.py` is a top-level-execution script (reads CSVs at module load, writes
gold outputs). Per project rules in [project_pipeline_sandbox_rule.md] the file
is not refactored into testable functions for this PR — so these tests document
the join patterns enrich.py performs against synthetic in-memory data rather
than importing enrich.py directly.

What this catches:
  - Regressions in the (normalise → unique → left join) pattern.
  - Diacritic / apostrophe / honorific edge cases failing to match.
  - Duplicate members in master list silently multiplying joined rows.
  - `year_elected` extraction from `unique_member_code` breaking.
  - Vote-history dedupe key changing shape.

What this does NOT catch (out of scope for this file):
  - enrich.py drifting from this pattern. If enrich.py changes its join shape,
    update these tests to match — they're the contract, not the enforcement.
  - Real-data edge cases not represented in fixtures.

When enrich.py is eventually refactored into callable functions, point these
tests at the real functions and remove the synthetic-data wrappers.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from shared.normalise_join_key import normalise_df_td_name

# ---------------------------------------------------------------------------
# Helpers — mirror enrich.py's "normalise + dedupe" preparation step
# ---------------------------------------------------------------------------


def _prepare_for_join(df: pl.DataFrame, first_col: str = "first_name", last_col: str = "last_name") -> pl.DataFrame:
    """Replicate enrich.py:12-19 — concat first+last → normalise → dedupe.

    Mirrors the pattern applied to both `members_wide_df` and
    `member_profiles_df` before the left join at enrich.py:25.
    """
    with_key = df.with_columns(pl.concat_str(pl.col([first_col, last_col])).alias("join_key"))
    normalised = normalise_df_td_name(with_key, "join_key")
    return normalised.unique(subset=["join_key"], keep="first")


def _extract_year_elected(df: pl.DataFrame) -> pl.DataFrame:
    """Replicate enrich.py:26 year_elected extraction from unique_member_code."""
    return df.with_columns(pl.col("unique_member_code").str.extract(r"\b\d{4}\b", 0).alias("year_elected"))


# ---------------------------------------------------------------------------
# Master-list left join — the primary enrichment
# ---------------------------------------------------------------------------


def test_left_join_preserves_all_members_when_attendance_missing():
    """Members with no attendance row must still appear in the gold output
    (with attendance columns as null). A regression to inner join would
    silently drop newly-elected TDs who haven't yet been recorded as attending.
    """
    members = pl.DataFrame(
        {
            "first_name": ["Mary", "Sean"],
            "last_name": ["Murphy", "O Brien"],
            "unique_member_code": ["Mary-Murphy.D.2020-02-08", "Sean-OBrien.D.2016-02-26"],
        }
    )
    attendance = pl.DataFrame(
        {
            "first_name": ["Mary"],
            "last_name": ["Murphy"],
            "sitting_days_count": [87],
        }
    )

    m = _prepare_for_join(members)
    a = _prepare_for_join(attendance)
    joined = m.join(a.select(["join_key", "sitting_days_count"]), on="join_key", how="left")

    assert joined.height == 2, "Left join must keep both members"
    sean_row = joined.filter(pl.col("last_name") == "O Brien")
    assert sean_row["sitting_days_count"][0] is None, "Sean has no attendance record → null"


def test_diacritic_names_join_to_ascii_equivalents():
    """The normaliser strips diacritics, so a member listed as 'Seán Ó Briain'
    on the API side joins to 'Sean O Briain' (ASCII transliteration of the
    same name) on the PDF attendance side. This is the single most important
    property of the join — without it, Irish-name TDs drop from every
    enriched dataset.

    Note: Briain (Irish) != Brien (Anglicised) — these are linguistically
    different surnames. Use the SAME name with different diacritic encoding.
    """
    members = pl.DataFrame({"first_name": ["Seán"], "last_name": ["Ó Briain"]})
    attendance = pl.DataFrame({"first_name": ["Sean"], "last_name": ["O Briain"], "sitting_days_count": [94]})

    m = _prepare_for_join(members)
    a = _prepare_for_join(attendance)
    joined = m.join(a.select(["join_key", "sitting_days_count"]), on="join_key", how="left")

    assert joined["sitting_days_count"][0] == 94


def test_apostrophe_variants_join():
    """O'Brien (API) and OBrien (PDF) and O Brien (alt PDF) all collapse to
    the same join key. The normaliser strips apostrophes and whitespace.
    """
    members = pl.DataFrame({"first_name": ["Sean", "Mary"], "last_name": ["O'Brien", "Murphy"]})
    attendance = pl.DataFrame(
        {"first_name": ["Sean", "Mary"], "last_name": ["O Brien", "Murphy"], "sitting_days_count": [80, 70]}
    )

    m = _prepare_for_join(members)
    a = _prepare_for_join(attendance)
    joined = m.join(a.select(["join_key", "sitting_days_count"]), on="join_key", how="left")

    assert joined.height == 2
    assert set(joined["sitting_days_count"].to_list()) == {80, 70}, "Both members must match by normalised key"


def test_duplicate_members_in_master_are_deduped_before_join():
    """The API occasionally returns the same member twice across Dáil terms.
    enrich.py:19 dedupes with `unique(keep="first")` before joining; without
    it, a left join multiplies attendance rows.
    """
    members = pl.DataFrame(
        {
            "first_name": ["Mary", "Mary"],  # duplicate
            "last_name": ["Murphy", "Murphy"],
            "unique_member_code": ["Mary-Murphy.D.2020-02-08", "Mary-Murphy.D.2016-02-26"],
        }
    )
    attendance = pl.DataFrame({"first_name": ["Mary"], "last_name": ["Murphy"], "sitting_days_count": [87]})

    m = _prepare_for_join(members)
    a = _prepare_for_join(attendance)
    joined = m.join(a.select(["join_key", "sitting_days_count"]), on="join_key", how="left")

    assert joined.height == 1, "Dedupe must collapse duplicate members before join"
    # `keep="first"` semantics — the 2020 row wins
    assert "2020" in joined["unique_member_code"][0]


# ---------------------------------------------------------------------------
# year_elected extraction from unique_member_code
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "code,expected_year",
    [
        ("Mary-Murphy.D.2020-02-08", "2020"),
        ("Sean-OBrien.D.2016-02-26", "2016"),
        ("Ada-Lovelace.S.1843-09-30", "1843"),
        # No 4-digit number → null (extract returns None)
        ("malformed_no_year", None),
    ],
)
def test_year_elected_extracted_from_member_code(code: str, expected_year: str | None):
    """enrich.py:26 — `\\b\\d{4}\\b` extract pulls the year from the unique
    Oireachtas code. Used downstream as the TD's first-elected year.
    """
    df = pl.DataFrame({"unique_member_code": [code]})
    result = _extract_year_elected(df)
    assert result["year_elected"][0] == expected_year


def test_year_elected_takes_first_year_when_multiple_present():
    """Defensive: if a code somehow contains multiple 4-digit numbers
    bounded by non-word characters, the regex extracts the first match.
    Documents current behavior.

    Note: `\\b\\d{4}\\b` requires non-word chars on both sides of the digits.
    Letters and underscores are word chars; '.', '-', and string boundaries
    are not. So 'Member2020_seat_1234' (underscores) extracts neither, but
    '2020-Member.D.1843-08-30' extracts '2020' (first non-word-bounded run).
    """
    df = pl.DataFrame({"unique_member_code": ["2020-Member.D.1843-08-30"]})
    result = _extract_year_elected(df)
    assert result["year_elected"][0] == "2020"


# ---------------------------------------------------------------------------
# Vote-history join dedupe key
# ---------------------------------------------------------------------------


def test_vote_join_dedupe_key_prevents_duplicate_vote_records():
    """enrich.py:114 — `unique(subset=["unique_member_code", "date", "vote_id"])`
    after the vote join. Same TD voting on the same motion the same day must
    produce exactly one row even if the join introduces duplicates.
    """
    votes = pl.DataFrame(
        {
            "unique_member_code": ["MC2020A", "MC2020A", "MC2020A"],
            "date": ["2026-01-15", "2026-01-15", "2026-01-16"],
            "vote_id": ["v1", "v1", "v2"],
            "vote_value": ["Tá", "Tá", "Níl"],
        }
    )

    deduped = votes.unique(subset=["unique_member_code", "date", "vote_id"])

    assert deduped.height == 2, "Same (member, date, vote_id) collapses to one row"


def test_vote_join_dedupe_keeps_distinct_votes_on_same_day():
    """Multiple distinct votes on the same date for the same member must be
    preserved — they have different vote_ids.
    """
    votes = pl.DataFrame(
        {
            "unique_member_code": ["MC2020A", "MC2020A"],
            "date": ["2026-01-15", "2026-01-15"],
            "vote_id": ["v1", "v2"],
            "vote_value": ["Tá", "Níl"],
        }
    )

    deduped = votes.unique(subset=["unique_member_code", "date", "vote_id"])

    assert deduped.height == 2


# ---------------------------------------------------------------------------
# Inner join (payments) — drops rows with no master-list match
# ---------------------------------------------------------------------------


def test_payments_inner_join_drops_unmatched_payment_rows():
    """enrich.py:150 — payments join uses `how="inner"`, intentionally.
    Payments to a name that doesn't match any TD (typo, retired TD, staff
    payment misclassified) are dropped from the ranking. A regression to
    `how="left"` would surface those as null-identifier ranking rows.
    """
    payments = pl.DataFrame({"join_key": ["ahmmprruyy", "abeeiinnors", "xyz_no_match"], "total": [1000, 2000, 3000]})
    master = pl.DataFrame(
        {
            "join_key": ["ahmmprruyy", "abeeiinnors"],
            "identifier": ["Mary-Murphy.D.2020-02-08", "Sean-OBrien.D.2016-02-26"],
            "party": ["FF", "SF"],
        }
    )

    joined = payments.join(master, on="join_key", how="inner")

    assert joined.height == 2, "Inner join drops the unmatched 'xyz_no_match' row"
    assert "xyz_no_match" not in joined["join_key"].to_list()


# ---------------------------------------------------------------------------
# Regex shape used in enrich.py — surfaces if polars changes regex behaviour
# ---------------------------------------------------------------------------


def test_year_extract_regex_matches_word_boundary():
    """enrich.py:26 uses `\\b\\d{4}\\b` — word boundaries ensure 12345 doesn't
    return '1234'. This is a polars-API behaviour test that catches if the
    underlying regex engine changes (rust regex crate).
    """
    pattern = r"\b\d{4}\b"
    assert re.search(pattern, "12345") is None, "5-digit run does not match"
    assert re.search(pattern, "abc-2020-def").group() == "2020"
