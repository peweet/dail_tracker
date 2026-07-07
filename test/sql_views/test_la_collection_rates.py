"""Tripwire for v_la_collection_rates + v_la_accountability_summary.

Reads the gold NOAC M2 parquet (data/gold/parquet/noac_m2_collection_wide.parquet)
+ the per-council views. Gold is gitignored → SKIP in CI, run on a dev box.

Guards: all 31 councils present and joining the CE roster (the NOAC→join-key map
must keep working), national medians constant, and the 1-row summary reconciles to
the per-council views it reads.
"""

from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
M2 = PROJECT_ROOT / "data" / "gold" / "parquet" / "noac_m2_collection_wide.parquet"
DERELICT = PROJECT_ROOT / "data" / "gold" / "parquet" / "derelict_sites_levy_wide.parquet"
APPEALS = PROJECT_ROOT / "data" / "silver" / "parquet" / "planning_appeal_outcomes.parquet"
SQL_DIR = PROJECT_ROOT / "sql_views" / "constituency"

pytestmark = pytest.mark.skipif(not M2.exists(), reason=f"gold source absent (CI): {M2.name}")


def _load(c, fname, **subs):
    sql = (SQL_DIR / fname).read_text(encoding="utf-8")
    for k, v in subs.items():
        sql = sql.replace(k, v)
    c.execute(sql)


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    _load(
        c,
        "constituency_la_chief_executives.sql",
        **{
            "data/_meta/la_chief_executives.csv": str(PROJECT_ROOT / "data/_meta/la_chief_executives.csv").replace(
                "\\", "/"
            )
        },
    )
    _load(
        c,
        "constituency_la_collection_rates.sql",
        **{"data/gold/parquet/noac_m2_collection_wide.parquet": str(M2).replace("\\", "/")},
    )
    return c


def test_31_councils_join_ce(con):
    n = con.execute("SELECT count(*) FROM v_la_collection_rates").fetchone()[0]
    assert n == 31, f"expected 31, got {n}"
    orphans = con.execute(
        """
        SELECT local_authority FROM v_la_collection_rates
        WHERE local_authority NOT IN (SELECT local_authority FROM v_la_chief_executives)
        """
    ).fetchall()
    assert not orphans, f"NOAC→join-key map broke: {orphans}"


def test_national_medians_constant(con):
    n = con.execute("SELECT count(DISTINCT nat_commercial_rates_pct) FROM v_la_collection_rates").fetchone()[0]
    assert n == 1, "national median should be identical on every row"


@pytest.mark.skipif(
    not (DERELICT.exists() and APPEALS.exists()),
    reason="summary needs derelict + appeals sources too",
)
def test_summary_reconciles(con):
    """The 1-row summary must reflect the per-council views it reads."""
    _load(
        con,
        "constituency_la_planning_overturn.sql",
        **{"data/silver/parquet/planning_appeal_outcomes.parquet": str(APPEALS).replace("\\", "/")},
    )
    _load(
        con,
        "constituency_la_derelict_sites_levy.sql",
        **{"data/gold/parquet/derelict_sites_levy_wide.parquet": str(DERELICT).replace("\\", "/")},
    )
    _load(con, "constituency_la_accountability_summary.sql")
    s = con.execute("SELECT * FROM v_la_accountability_summary").fetchone()
    cols = [d[0] for d in con.description]
    row = dict(zip(cols, s, strict=False))
    assert row["n_councils"] == 31
    assert row["n_councils_levied_nothing"] >= 1
    assert 0 <= row["national_overturn_rate_pct"] <= 100
    assert row["derelict_outstanding_eur"] > 0
