"""
Tests for the pure helper functions in member_interests.py.

The full parser (`extract_raw_lines` → `split_embedded_names` → `group_lines`
→ `parse_members` → `clean_interests`) operates on PDF-extracted text. The
PDF stage requires a real file fixture (deferred — interests PDFs are
large), but the downstream string/list/DataFrame helpers are pure
functions covered here.

What this catches:
  - Regression in the embedded-name split regex (the case where PyMuPDF
    glues a name onto the previous category's line).
  - Continuation-line grouping (multi-line category text getting joined
    to the wrong parent).
  - Member-block partitioning (one dict per TD).
  - The Níl / Nil / Tada → 'No interests declared' normalisation chain.
  - Interest-code → interest-category mapping.
  - is_landlord / is_property_owner / interest_flag derivation logic.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from members.member_interests import (
    CATEGORIES_PATTERN,
    INTEREST_CODE_MAP,
    MEMBER_NAME_PATTERN,
    QUALITY_MATCH_THRESHOLD,
    clean_interests,
    group_lines,
    parse_members,
    passes_quality_gate,
    split_embedded_names,
)

# ---------------------------------------------------------------------------
# split_embedded_names — pre-grouping cleanup
# ---------------------------------------------------------------------------


def test_split_embedded_names_separates_embedded_name():
    """Per the docstring example: 'Nil  SMITH, John' is one PyMuPDF line
    but should become two — the category content followed by the new name.
    The regex matches 2+ spaces followed by an all-caps name pattern.
    """
    lines = ["Nil  SMITH, John"]
    result = split_embedded_names(lines)
    assert result == ["Nil", "SMITH, John"]


def test_split_embedded_names_leaves_normal_lines_alone():
    """A line with no embedded name pattern passes through unchanged."""
    lines = ["No interests declared", "MURPHY, Mary"]
    result = split_embedded_names(lines)
    assert result == ["No interests declared", "MURPHY, Mary"]


def test_split_embedded_names_skips_empty_fragments():
    """If splitting produces empty fragments (e.g. leading whitespace), they
    are filtered out so group_lines doesn't see junk rows.
    """
    lines = ["  Nil  SMITH, John  "]
    result = split_embedded_names(lines)
    assert all(line.strip() for line in result)
    assert "" not in result


def test_split_embedded_names_handles_irish_apostrophe_in_name():
    """Names like O'DONOVAN are common; the regex must allow apostrophes
    in the all-caps name pattern.
    """
    lines = ["Nil  O'DONOVAN, Denis"]
    result = split_embedded_names(lines)
    assert "O'DONOVAN, Denis" in result


# ---------------------------------------------------------------------------
# group_lines — concatenate continuation lines onto their parent block
# ---------------------------------------------------------------------------


def test_group_lines_concatenates_continuation_into_parent_block():
    """Lines that don't match a category or name pattern are continuation
    text — they get appended to the most recent block.
    """
    lines = [
        "MURPHY, Mary",
        "1. Occupations",
        "Solicitor at ABC Law",  # continuation
        "Dublin office",  # continuation
        "2. Shares",
    ]
    grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)

    # 3 blocks: the name, category 1 (with both continuations merged), category 2
    assert len(grouped) == 3
    assert grouped[0] == "MURPHY, Mary"
    assert "Solicitor at ABC Law" in grouped[1]
    assert "Dublin office" in grouped[1]
    assert grouped[2] == "2. Shares"


def test_group_lines_starts_a_new_block_at_each_member_name():
    """A member-name line always starts a new block — even if the previous
    block hasn't ended with a category-12 marker.
    """
    lines = ["MURPHY, Mary", "1. Occupations", "Solicitor", "O'BRIEN, Sean", "1. Occupations", "Teacher"]
    grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)
    # Both member names produce their own block
    names_in_groups = [g for g in grouped if MEMBER_NAME_PATTERN.match(g)]
    assert len(names_in_groups) == 2


def test_group_lines_strips_trailing_whitespace():
    """Output blocks are stripped — the joining logic adds spaces that
    could result in trailing whitespace.
    """
    lines = ["MURPHY, Mary  ", "1. Occupations ", " Solicitor "]
    grouped = group_lines(lines, CATEGORIES_PATTERN, MEMBER_NAME_PATTERN)
    for g in grouped:
        assert g == g.strip()


# ---------------------------------------------------------------------------
# parse_members — walk grouped blocks into per-member dicts
# ---------------------------------------------------------------------------


def test_parse_members_groups_interests_under_each_member():
    """Each member-name block opens a new dict; subsequent non-name blocks
    accumulate into its `interests` list.
    """
    grouped = [
        "MURPHY, Mary  (Dublin South)",
        "1. Occupations Solicitor",
        "2. Shares Acme Corp",
        "O'BRIEN, Sean  (Cork North)",
        "1. Occupations Teacher",
    ]
    members = parse_members(grouped, MEMBER_NAME_PATTERN)

    assert len(members) == 2
    assert members[0]["name"].startswith("MURPHY, Mary")
    assert len(members[0]["interests"]) == 2
    assert members[1]["name"].startswith("O'BRIEN, Sean")
    assert len(members[1]["interests"]) == 1


def test_parse_members_emits_no_dict_when_no_names_found():
    """If the input has no member-name lines at all (e.g. footer-only
    pages from extract_raw_lines), the function returns an empty list
    rather than orphan interest blocks.
    """
    grouped = ["1. Occupations Random", "Some footer text"]
    members = parse_members(grouped, MEMBER_NAME_PATTERN)
    assert members == []


def test_parse_members_emits_final_member_on_eof():
    """The last member in the input is closed when the loop ends, not
    when the next name appears. Without this, the last TD silently
    drops out of every parse.
    """
    grouped = ["MURPHY, Mary", "1. Occupations Solicitor"]
    members = parse_members(grouped, MEMBER_NAME_PATTERN)
    assert len(members) == 1
    assert len(members[0]["interests"]) == 1


# ---------------------------------------------------------------------------
# INTEREST_CODE_MAP — sanity check the map covers known codes
# ---------------------------------------------------------------------------


def test_interest_code_map_has_known_categories():
    """The category names are used throughout the codebase (silver schema,
    UI display). A missing key here would surface as 'Unknown' in the UI.
    """
    expected = {"1", "2", "3", "4"}
    missing = expected - set(INTEREST_CODE_MAP.keys())
    assert not missing, f"INTEREST_CODE_MAP missing codes: {missing}"
    assert INTEREST_CODE_MAP["1"] == "Occupations"


# ---------------------------------------------------------------------------
# clean_interests — DataFrame transformation smoke tests
# ---------------------------------------------------------------------------


def _interests_input_df() -> pl.DataFrame:
    """Build a minimal input DataFrame in the shape parse_members would emit
    (after `pl.read_json` of the JSON intermediate). Names follow the
    'LAST, First  (Constituency)' convention with 2+ space separation.
    """
    return pl.DataFrame(
        {
            "name": [
                "MURPHY, Mary  (Dublin South)",
                "O'BRIEN, Sean  (Cork North-West)",
                "WALSH, John  (Dublin Rathdown)",
            ],
            "interests": [
                ["1. Solicitor at ABC Law", "4. Apartment in Dublin (let)"],
                # '2. Shares' needs a colon to survive the fragment filter
                # (only categories 2/3/9 with ':' OR 'no interests declared' are kept).
                ["1. Teacher", "2. Acme Corp: 100 shares"],
                ["1. Níl", "4. No interests declared"],
            ],
        }
    )


def test_clean_interests_explodes_one_row_per_interest():
    """Each member's interests list is exploded so the gold table has
    one row per (member, interest_code) pair.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    # 3 members × 2 interests each = at least 6 rows (may be more after splits)
    assert df.height >= 6


def test_clean_interests_extracts_constituency_from_double_space():
    """Constituency is split off the name on the 2+ space boundary and
    stripped of surrounding parens.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    mary_constituency = df.filter(pl.col("last_name") == "MURPHY")["constituency"][0]
    assert mary_constituency == "Dublin South"


def test_clean_interests_splits_name_into_last_and_first():
    """The 'LAST, First' convention is split on comma into separate columns."""
    df = clean_interests(_interests_input_df(), year=2026)
    mary = df.filter(pl.col("last_name") == "MURPHY").row(0, named=True)
    assert mary["last_name"] == "MURPHY"
    assert mary["first_name"].strip() == "Mary"


def test_clean_interests_maps_interest_code_to_category_name():
    """Interest codes (1, 2, 3, 4, …) are mapped to human-readable category
    names via INTEREST_CODE_MAP. Downstream UI relies on the category column.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    occupation_rows = df.filter(pl.col("interest_code") == "1")
    assert occupation_rows.height > 0
    assert occupation_rows["interest_category"][0] == "Occupations"


def test_clean_interests_normalises_nil_to_no_interests_declared():
    """The Irish-language 'Níl' (and 'Nil') variants are normalised to
    the canonical 'No interests declared' string. This is the single
    most consequential normalisation — without it, Irish-named TDs
    appear to have undeclared interests in the dashboard.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    obrien_rows = df.filter(pl.col("last_name") == "WALSH")
    # WALSH has '1. Níl' as their first interest — should become 'No interests declared'.
    declared_values = obrien_rows["interest_description_cleaned"].to_list()
    assert any("No interests declared" in v for v in declared_values)


def test_clean_interests_flags_landlord_status_from_rental_keywords():
    """is_landlord is derived heuristically from rental-related words
    in the description. 'Apartment in Dublin (let)' contains 'let' → landlord.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    mary_landlord_rows = df.filter((pl.col("last_name") == "MURPHY") & (pl.col("is_landlord") == "true"))
    assert mary_landlord_rows.height > 0


def test_clean_interests_sets_interest_flag_when_not_no_interests_declared():
    """interest_flag = 1 means 'declared something'; 0 means 'No interests declared'.
    Used downstream for declared-interest counts.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    # WALSH '4. No interests declared' → flag 0
    walsh_no_int = df.filter(
        (pl.col("last_name") == "WALSH") & (pl.col("interest_description_cleaned") == "No interests declared")
    )
    assert walsh_no_int.height > 0
    assert walsh_no_int["interest_flag"][0] == 0


def test_clean_interests_emits_join_key():
    """join_key column is computed as concat(first_name, last_name) — used
    by downstream enrichment to match against the TD master list.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    assert "join_key" in df.columns
    assert df["join_key"].null_count() == 0


# ---------------------------------------------------------------------------
# passes_quality_gate — the scanned/OCR'd-register guard
# ---------------------------------------------------------------------------


def test_quality_gate_passes_clean_born_digital_year():
    """A born-digital register matches the roster ~96% — must pass."""
    assert passes_quality_gate(n_registered=159, n_parsed=166) is True


def test_quality_gate_rejects_scanned_garbage_year():
    """A scanned/OCR'd register mangles names → ~0% match — must be skipped.
    Mirrors the real 2016 register (150 parsed, 0 matched).
    """
    assert passes_quality_gate(n_registered=0, n_parsed=150) is False


def test_quality_gate_threshold_boundary():
    """Exactly at the threshold passes; just below fails."""
    n = 100
    assert passes_quality_gate(int(n * QUALITY_MATCH_THRESHOLD), n) is True
    assert passes_quality_gate(int(n * QUALITY_MATCH_THRESHOLD) - 1, n) is False


def test_quality_gate_handles_zero_parsed():
    """A PDF that parsed nothing (e.g. a scanned image with no text layer, the
    real 2012 case) must not divide-by-zero and must fail the gate.
    """
    assert passes_quality_gate(n_registered=0, n_parsed=0) is False


@pytest.mark.parametrize(
    "expected_col",
    [
        "name",
        "last_name",
        "first_name",
        "constituency",
        "interest_code",
        "interest_category",
        "interest_description_raw",
        "interest_description_cleaned",
        "join_key",
        "is_landlord",
        "is_property_owner",
        "interest_flag",
    ],
)
def test_clean_interests_output_schema_contains_required_columns(expected_col: str):
    """Silver schema contract — all these columns are consumed downstream.
    A rename or removal breaks the SQL views and Streamlit pages silently.
    """
    df = clean_interests(_interests_input_df(), year=2026)
    assert expected_col in df.columns, f"Missing required column: {expected_col}"
