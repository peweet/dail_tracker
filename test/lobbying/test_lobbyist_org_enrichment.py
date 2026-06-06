"""Gold-builder contract tests for lobbyist organisation-register enrichment.

sql_queries/top_lobbyist_organisations.sql joins the lobbying.ie organisation
register (split_lobbyists) onto the org leaderboard so gold carries each
lobbyist's website / CRO number / registered name / main activities / org-page
URL. These are unit tests of that SQL against tiny in-memory fixtures — they run
in CI without any pipeline output.

What they guard:
  - the five register columns are present in the gold output
  - register values land on the matching lobbyist (website wiring is real)
  - LEFT JOIN keeps organisations with no register row (enrichment NULL, not dropped)
  - the register dedup keeps the table one-row-per-organisation even when the
    register lists an org under several issue URIs (no aggregate fan-out)
"""

import sys
from pathlib import Path

import duckdb
import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

SQL = (PROJECT_ROOT / "sql_queries" / "top_lobbyist_organisations.sql").read_text(encoding="utf-8")

REGISTER_COLS = {
    "main_activities_of_organisation",
    "website",
    "company_registration_number",
    "company_registered_name",
    "lobby_org_link",
}

# politician_returns_detail grain: one row per return × politician.
# IBEC: 2 returns (10,11) targeting 2 politicians; UNMATCHED: 1 return, no register row.
_POL_RETURNS = pl.DataFrame(
    {
        "primary_key": [10, 10, 11, 20],
        "full_name": ["TD A", "TD B", "TD A", "TD C"],
        "lobbyist_name": ["IBEC", "IBEC", "IBEC", "Unmatched Org"],
        "public_policy_area": ["Health", "Health", "Tax", "Tax"],
    }
)

# split_lobbyists: IBEC appears twice (two issue URIs) — the richer-website row
# must win the dedup. Cork Chamber has a single clean row.
_SPLIT = pl.DataFrame(
    {
        "lobbyist_name": ["IBEC", "IBEC", "Cork Chamber"],
        "main_activities_of_organisation": ["Business lobbying", "", "Chamber"],
        "website": ["www.ibec.ie", "", "www.corkchamber.ie"],
        "company_registration_number": ["8706", "8706", "13918"],
        "company_registered_name": ["IBEC CLG", "IBEC CLG", "CORK CHAMBER"],
        "lobby_org_link": [
            "https://www.lobbying.ie/organisation/1/ibec",
            "https://www.lobbying.ie/organisation/2/ibec",
            "https://www.lobbying.ie/organisation/3/cork-chamber",
        ],
    }
)


def _run(pol_returns: pl.DataFrame, split: pl.DataFrame) -> pl.DataFrame:
    con = duckdb.connect()
    con.register("politician_returns_detail", pol_returns.to_arrow())
    con.register("split_lobbyists", split.to_arrow())
    out = pl.from_arrow(con.execute(SQL).arrow())
    con.close()
    return out


def test_register_columns_present():
    out = _run(_POL_RETURNS, _SPLIT)
    assert REGISTER_COLS.issubset(set(out.columns)), f"missing register columns: {REGISTER_COLS - set(out.columns)}"


def test_website_lands_on_matching_lobbyist():
    out = _run(_POL_RETURNS, _SPLIT)
    ibec = out.filter(pl.col("lobbyist_name") == "IBEC").row(0, named=True)
    assert ibec["website"] == "www.ibec.ie"
    assert ibec["company_registration_number"] == "8706"
    # returns_filed counts distinct primary_key (10, 11) — enrichment must not inflate it
    assert ibec["returns_filed"] == 2


def test_left_join_keeps_unmatched_org():
    out = _run(_POL_RETURNS, _SPLIT)
    names = set(out["lobbyist_name"].to_list())
    assert "Unmatched Org" in names, "LEFT JOIN dropped an org with no register row"
    unmatched = out.filter(pl.col("lobbyist_name") == "Unmatched Org").row(0, named=True)
    assert unmatched["website"] is None


def test_no_row_fanout_from_duplicate_register():
    out = _run(_POL_RETURNS, _SPLIT)
    assert out.height == out["lobbyist_name"].n_unique(), "register dedup failed — gold fanned out"
    # IBEC's duplicate register rows must collapse to one gold row carrying the richer website
    assert out.filter(pl.col("lobbyist_name") == "IBEC").height == 1


@pytest.mark.integration
def test_real_gold_carries_register_columns():
    """The shipped gold file (when present) exposes the register columns."""
    from config import GOLD_PARQUET_DIR

    path = GOLD_PARQUET_DIR / "top_lobbyist_organisations.parquet"
    if not path.exists():
        pytest.skip("gold not built — run pipeline.py / lobby_processing.py first")
    df = pl.read_parquet(path)
    assert REGISTER_COLS.issubset(set(df.columns))
    populated = df.filter(pl.col("website").is_not_null() & (pl.col("website") != "")).height
    assert populated > 0, "no website values populated in shipped gold"
