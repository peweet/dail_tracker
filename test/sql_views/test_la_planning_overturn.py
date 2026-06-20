"""Tripwire for v_la_planning_overturn (council planning-quality signal).

Reads the real silver parquet (data/silver/parquet/planning_appeal_outcomes.parquet)
and the CE roster. The parquet is gitignored, so these tests SKIP cleanly in CI
where it is absent and run on a dev box / integration where it is present.

Guards: the authority->local_authority normalisation keeps joining the CE roster
(a vendor renaming PlanningAuthority would orphan a council and silently drop it
from the page), rates stay in [0,100], and Cork County — recovered via the extractor's
spatial_temporal fallback since it publishes no AppealRefNumber — stays present.
"""

from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
APPEALS = PROJECT_ROOT / "data" / "silver" / "parquet" / "planning_appeal_outcomes.parquet"
OVERTURN_SQL = PROJECT_ROOT / "sql_views" / "constituency" / "constituency_la_planning_overturn.sql"
CE_SQL = PROJECT_ROOT / "sql_views" / "constituency" / "constituency_la_chief_executives.sql"

pytestmark = pytest.mark.skipif(
    not APPEALS.exists(), reason=f"silver source absent (CI): {APPEALS.name}"
)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    ce = CE_SQL.read_text(encoding="utf-8").replace(
        "data/_meta/la_chief_executives.csv",
        str(PROJECT_ROOT / "data" / "_meta" / "la_chief_executives.csv").replace("\\", "/"),
    )
    ov = OVERTURN_SQL.read_text(encoding="utf-8").replace(
        "data/silver/parquet/planning_appeal_outcomes.parquet", str(APPEALS).replace("\\", "/")
    )
    c.execute(ce)
    c.execute(ov)
    return c


def test_view_builds_with_councils(con):
    n = con.execute("SELECT count(*) FROM v_la_planning_overturn").fetchone()[0]
    assert n >= 25, f"expected most councils, got {n}"


def test_every_council_joins_ce_roster(con):
    orphans = con.execute(
        """
        SELECT local_authority FROM v_la_planning_overturn
        WHERE local_authority NOT IN (SELECT local_authority FROM v_la_chief_executives)
        """
    ).fetchall()
    assert not orphans, f"PlanningAuthority normalisation broke — orphans: {orphans}"


def test_rates_in_range(con):
    bad = con.execute(
        """
        SELECT local_authority, overturn_rate_pct FROM v_la_planning_overturn
        WHERE overturn_rate_pct < 0 OR overturn_rate_pct > 100
           OR national_overturn_rate_pct < 0 OR national_overturn_rate_pct > 100
        """
    ).fetchall()
    assert not bad, f"rate out of [0,100]: {bad}"


def test_national_benchmark_is_constant(con):
    """The window benchmark must be identical on every row."""
    n = con.execute(
        "SELECT count(DISTINCT national_overturn_rate_pct) FROM v_la_planning_overturn"
    ).fetchone()[0]
    assert n == 1, "national_overturn_rate_pct should be the same on all rows"


def test_cork_county_recovered(con):
    """Cork County publishes no AppealRefNumber, so the extractor recovers its appeals via the
    validated spatial_temporal fallback — it must now appear with a plausible rate. Also guards
    the view's name mapping ('Cork County Council' -> 'Cork County', the easily-missed split)."""
    row = con.execute(
        "SELECT n_appeals, overturn_rate_pct FROM v_la_planning_overturn WHERE local_authority = 'Cork County'"
    ).fetchone()
    assert row is not None, "Cork County dropped out — check the view CASE mapping / extractor fallback"
    assert row[0] >= 25 and 10 <= row[1] <= 45
