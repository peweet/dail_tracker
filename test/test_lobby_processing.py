"""
Tests for pure functions in lobby_processing.py.

`lobby_processing.py` is 324 lines, 0% covered before this. It transforms
the messy lobbying.ie CSV exports into silver/gold via a chain of pure
polars functions (and one CSV-line parser). These tests target the
in-memory transformations — file I/O wrappers are not covered here.

What this catches:
  - Regression in the CSV line-level parser (`parse_line`) that handles
    embedded commas and inconsistent quoting.
  - Title-prefix / suffix stripping in `clean_dpo_name`.
  - The explode chain for DPO names and activities — if the split shape
    changes, the silver schema silently shifts.
  - Pipe-delimited client / DPO field splitting.
  - Lobbying-period date parsing (custom dd MMM, yyyy format).
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from lobby_processing import (
    clean_dpo_name,
    explode_activities,
    explode_politicians,
    parse_clients,
    parse_current_or_former_dpos,
    parse_line,
    parse_lobbying_period,
)

# ---------------------------------------------------------------------------
# parse_line — line-level CSV parser
# ---------------------------------------------------------------------------


def test_parse_line_splits_on_field_boundary():
    """The raw CSV uses '","' as the field boundary, not bare commas."""
    line = '"field1","field2","field3"'
    assert parse_line(line) == ["field1", "field2", "field3"]


def test_parse_line_strips_outer_quotes():
    """Leading quote on first field and trailing quote on last field are stripped."""
    line = '"single_field"'
    assert parse_line(line) == ["single_field"]


def test_parse_line_preserves_commas_inside_fields():
    """Embedded commas inside fields (the bug-creating case for naive parsers)
    must be preserved. The whole reason this manual parser exists.
    """
    line = '"Smith, John","Dublin, Ireland","contact"'
    assert parse_line(line) == ["Smith, John", "Dublin, Ireland", "contact"]


def test_parse_line_normalises_apostrophe_quote_sequence():
    """The raw export sometimes contains "' (quote-apostrophe) artifacts
    from re-encoding. parse_line collapses these to bare apostrophes.
    """
    line = '"O"\'Brien","client"'
    parsed = parse_line(line)
    # The quote-apostrophe should have been replaced with just apostrophe.
    assert "O'Brien" in parsed[0]


# ---------------------------------------------------------------------------
# clean_dpo_name — title strip + canonical mapping
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("Minister Mary Murphy", "Mary Murphy"),
        ("Senator Sean OBrien", "Sean OBrien"),
        ("Deputy Aoife Smith", "Aoife Smith"),
        ("TD John Walsh", "John Walsh"),
        ("Dr Mary Connolly", "Mary Connolly"),
        ("Dr. Mary Connolly", "Mary Connolly"),
        ("Prof. James Doe", "James Doe"),
        ("An Taoiseach Leo Varadkar", "Leo Varadkar"),
        ("Tánaiste Simon Harris", "Simon Harris"),
        # Suffix-stripping
        ("Mary Murphy, TD", "Mary Murphy"),
        ("Mary Murphy TD", "Mary Murphy"),
        ("Joe Bloggs, Senator", "Joe Bloggs"),
    ],
)
def test_clean_dpo_name_strips_titles(raw: str, expected: str):
    """Lobbying CSVs carry inconsistent honorifics. The cleaner must
    collapse them so the same person joins to one TD across rows.
    """
    assert clean_dpo_name(raw) == expected


def test_clean_dpo_name_applies_canonical_mapping():
    """The canonical map fixes known spelling variants — Michael Martin
    (ASCII) → Micheál Martin (canonical). Without this, the same person
    counts as two TDs in downstream lobbying tallies.
    """
    assert clean_dpo_name("Michael Martin") == "Micheál Martin"
    assert clean_dpo_name("michael martin") == "Micheál Martin"  # case-insensitive


def test_clean_dpo_name_strips_whitespace_and_punctuation():
    """Leading/trailing whitespace and stray commas must be removed before
    title detection runs — otherwise 'Minister Mary,' wouldn't match
    'Minister ' prefix.
    """
    assert clean_dpo_name("  Mary Murphy ,  ") == "Mary Murphy"


def test_clean_dpo_name_returns_clean_name_unchanged():
    """Already-clean names pass through untouched."""
    assert clean_dpo_name("Mary Murphy") == "Mary Murphy"


# ---------------------------------------------------------------------------
# explode_politicians — DPO list → one-row-per-politician
# ---------------------------------------------------------------------------


def test_explode_politicians_one_row_per_politician():
    """A row with two politicians lobbied (`::`-separated) becomes two rows."""
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "dpo_lobbied": ["Mary Murphy|TD|Dáil::Sean OBrien|TD|Dáil"],
            "lobby_enterprise_uri": ["uri-1"],
        }
    )
    result = explode_politicians(df)
    assert result.height == 2
    assert set(result["full_name"].to_list()) == {"Mary Murphy", "Sean OBrien"}


def test_explode_politicians_parses_pipe_delimited_fields():
    """Each '::'-separated lobbyist is itself '|'-delimited:
    name|position|chamber. All three fields land in named columns.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "dpo_lobbied": ["Mary Murphy|TD|Dáil Éireann"],
            "lobby_enterprise_uri": ["uri-1"],
        }
    )
    result = explode_politicians(df)
    row = result.row(0, named=True)
    assert row["full_name"] == "Mary Murphy"
    assert row["position"] == "TD"
    assert row["chamber"] == "Dáil Éireann"


def test_explode_politicians_applies_name_cleaning():
    """clean_dpo_name is map_elements'd over full_name during explode.
    Verify the title-stripping fires end-to-end through the pipeline.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "dpo_lobbied": ["Minister Mary Murphy|TD|Dáil"],
            "lobby_enterprise_uri": ["uri-1"],
        }
    )
    result = explode_politicians(df)
    assert result["full_name"][0] == "Mary Murphy"


def test_explode_politicians_drops_collective_dpo_entries():
    """Rows where the politician is a collective ('Dáil Éireann (all TDs)')
    are filtered out — they're not real per-person lobbying contacts and
    would skew the most-lobbied-politicians ranking.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1, 2],
            "dpo_lobbied": [
                "Mary Murphy|TD|Dáil",
                "Dáil Éireann (all TDs)|N/A|Dáil",
            ],
            "lobby_enterprise_uri": ["uri-1", "uri-2"],
        }
    )
    result = explode_politicians(df)
    assert result.height == 1
    assert result["full_name"][0] == "Mary Murphy"


def test_explode_politicians_drops_empty_names_after_title_strip():
    """A name that becomes empty after clean_dpo_name (e.g. just whitespace
    and punctuation from a malformed upstream row) is filtered out as junk.

    Note: clean_dpo_name only strips a prefix if it has a trailing space
    (per TITLE_PREFIXES). So 'Minister' alone with no surname does NOT
    strip to empty — it leaks through as the literal string 'Minister'.
    The empty-filter catches genuinely-empty rows from broken parsing.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1, 2],
            "dpo_lobbied": [
                "Mary Murphy|TD|Dáil",
                " , |N/A|Dáil",  # whitespace + comma → strips to empty
            ],
            "lobby_enterprise_uri": ["uri-1", "uri-2"],
        }
    )
    result = explode_politicians(df)
    assert result.height == 1
    assert result["full_name"][0] == "Mary Murphy"


# ---------------------------------------------------------------------------
# explode_activities — activity list → one-row-per-activity
# ---------------------------------------------------------------------------


def test_explode_activities_one_row_per_activity():
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "lobbying_activities": ["Meeting|in-person|TDs::Email|email|Senators"],
            "date_published_timestamp": ["01/03/2026 10:00"],
        }
    )
    result = explode_activities(df)
    assert result.height == 2
    assert set(result["action"].to_list()) == {"Meeting", "Email"}


def test_explode_activities_parses_date_to_datetime():
    """The timestamp column is split via the dd/mm/yyyy HH:MM format. A
    silent format change upstream would land here.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "lobbying_activities": ["Meeting|in-person|TDs"],
            "date_published_timestamp": ["15/03/2026 14:30"],
        }
    )
    result = explode_activities(df)
    ts = result["date_published_timestamp_dt"][0]
    assert ts.year == 2026
    assert ts.month == 3
    assert ts.day == 15
    assert ts.hour == 14


def test_explode_activities_keeps_three_named_columns():
    """action / delivery / members_targeted — the silver schema relies on
    these column names. A rename breaks downstream views silently.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "lobbying_activities": ["Meeting|in-person|TDs"],
            "date_published_timestamp": ["01/03/2026 10:00"],
        }
    )
    result = explode_activities(df)
    for col in ("action", "delivery", "members_targeted"):
        assert col in result.columns


# ---------------------------------------------------------------------------
# parse_clients — pipe-delimited client splitter
# ---------------------------------------------------------------------------


def test_parse_clients_splits_into_four_columns():
    """clients string is name|address|email|telephone. Each goes to its own column."""
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "clients": ["Acme Corp|1 Main St, Dublin|info@acme.ie|+353 1 234 5678"],
        }
    )
    result = parse_clients(df)
    row = result.row(0, named=True)
    assert row["client_name"] == "Acme Corp"
    assert row["client_address"] == "1 Main St, Dublin"
    assert row["email"] == "info@acme.ie"
    assert row["telephone"] == "+353 1 234 5678"


def test_parse_clients_handles_all_null_column():
    """If every row has null clients (common — some returns have no client),
    the function short-circuits to all-null columns rather than trying to
    split nulls. Without this, the explode would crash or produce an
    enormous all-null table.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1, 2],
            "clients": [None, None],
        },
        schema={"primary_key": pl.Int64, "clients": pl.Utf8},
    )
    result = parse_clients(df)
    assert result.height == 2
    assert result["client_name"].null_count() == 2


def test_parse_clients_drops_original_clients_column():
    """After splitting, the pipe-delimited source column must be removed —
    otherwise it gets carried into silver and confuses analyses.
    """
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "clients": ["Acme|Address|email|phone"],
        }
    )
    result = parse_clients(df)
    assert "clients" not in result.columns
    assert "clients_list" not in result.columns


# ---------------------------------------------------------------------------
# parse_current_or_former_dpos — pipe-delimited DPO splitter
# ---------------------------------------------------------------------------


def test_parse_current_or_former_dpos_splits_and_renames():
    """Three sub-fields: name|position|chamber. The name field is renamed
    to `dpos_or_former_dpos_who_carried_out_lobbying_name` (the silver
    schema column name).
    """
    df = pl.DataFrame(
        {
            "primary_key": [1],
            "current_or_former_dpos": ["John Walsh|Former Minister|Dáil"],
        }
    )
    result = parse_current_or_former_dpos(df)
    row = result.row(0, named=True)
    assert row["dpos_or_former_dpos_who_carried_out_lobbying_name"] == "John Walsh"
    assert row["current_or_former_dpos_position"] == "Former Minister"
    assert row["current_or_former_dpos_chamber"] == "Dáil"


# ---------------------------------------------------------------------------
# parse_lobbying_period — 'DD MMM, YYYY to DD MMM, YYYY' date parser
# ---------------------------------------------------------------------------


def test_parse_lobbying_period_splits_into_typed_dates():
    """The lobbying.ie 'period' string format is human-formatted. This
    function turns it into start/end Datetimes. A regression on the format
    string (`%e %b, %Y`) would silently null both columns.
    """
    df = pl.DataFrame({"lobbying_period": ["1 Jan, 2026 to 30 Apr, 2026"]})
    result = parse_lobbying_period(df)
    start = result["lobbying_period_start_date"][0]
    end = result["lobbying_period_end_date"][0]
    assert start.year == 2026
    assert start.month == 1
    assert start.day == 1
    assert end.month == 4
    assert end.day == 30


def test_parse_lobbying_period_drops_source_column():
    """The original `lobbying_period` string column must be dropped after
    splitting — silver schema doesn't include it.
    """
    df = pl.DataFrame({"lobbying_period": ["1 Jan, 2026 to 30 Apr, 2026"]})
    result = parse_lobbying_period(df)
    assert "lobbying_period" not in result.columns
