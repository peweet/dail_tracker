"""Query-layer tests for the 'Who runs your county' data path.

Exercises dail_tracker_core.queries.local_government against the registered council
views — the same retrieval the Streamlit page uses, minus Streamlit. Registers the
5 v_la_* views (paths made absolute) and asserts each query returns a QueryResult
with the expected shape.

Skips in CI: the views read gold/silver parquets that are gitignored. Runs on a dev
box / integration where the pipeline output is present.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from dail_tracker_core.queries import local_government as q  # noqa: E402

SQL_DIR = ROOT / "sql_views" / "constituency"
CSV = ROOT / "data" / "_meta" / "la_chief_executives.csv"
M2 = ROOT / "data" / "gold" / "parquet" / "noac_m2_collection_wide.parquet"
DERELICT = ROOT / "data" / "gold" / "parquet" / "derelict_sites_levy_wide.parquet"
APPEALS = ROOT / "data" / "silver" / "parquet" / "planning_appeal_outcomes.parquet"
SCORECARD = ROOT / "data" / "gold" / "parquet" / "noac_scorecard_wide.parquet"
HISTORY = ROOT / "data" / "gold" / "parquet" / "noac_scorecard_history.parquet"
INDICATORS = ROOT / "data" / "gold" / "parquet" / "noac_indicators_long.parquet"

pytestmark = pytest.mark.skipif(
    not (
        CSV.exists()
        and M2.exists()
        and DERELICT.exists()
        and APPEALS.exists()
        and SCORECARD.exists()
        and HISTORY.exists()
        and INDICATORS.exists()
    ),
    reason="council source data absent (CI)",
)

_SUBS = {
    "data/_meta/la_chief_executives.csv": str(CSV).replace("\\", "/"),
    "data/gold/parquet/noac_m2_collection_wide.parquet": str(M2).replace("\\", "/"),
    "data/gold/parquet/derelict_sites_levy_wide.parquet": str(DERELICT).replace("\\", "/"),
    "data/silver/parquet/planning_appeal_outcomes.parquet": str(APPEALS).replace("\\", "/"),
    "data/gold/parquet/noac_scorecard_wide.parquet": str(SCORECARD).replace("\\", "/"),
    "data/gold/parquet/noac_scorecard_history.parquet": str(HISTORY).replace("\\", "/"),
    "data/gold/parquet/noac_indicators_long.parquet": str(INDICATORS).replace("\\", "/"),
}

_VIEWS = [
    "constituency_la_chief_executives.sql",
    "constituency_la_planning_overturn.sql",
    "constituency_la_derelict_sites_levy.sql",
    "constituency_la_collection_rates.sql",
    "constituency_la_noac_scorecard.sql",
    "constituency_la_noac_scorecard_history.sql",
    "constituency_la_noac_indicators.sql",
    "constituency_la_accountability_summary.sql",
    "constituency_la_cash_signals.sql",  # JOINs scorecard + collection_rates + derelict
]


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect()
    for fname in _VIEWS:
        sql = (SQL_DIR / fname).read_text(encoding="utf-8")
        for k, v in _SUBS.items():
            sql = sql.replace(k, v)
        c.execute(sql)
    return c


def test_index_lists_31(conn):
    res = q.chief_executives(conn)
    assert res.ok and len(res.data) == 31


def test_national_summary_one_row(conn):
    res = q.national_summary(conn)
    assert res.ok and len(res.data) == 1
    row = res.data.iloc[0]
    assert row["n_councils"] == 31
    assert row["derelict_outstanding_eur"] > 0


def test_derelict_levy_ranking_all_councils(conn):
    """Cross-council derelict-levy ranking: 31 rows, national totals present, and the
    arrears-aware collection rate is NULL exactly where a council levied nothing."""
    res = q.derelict_levy_ranking(conn)
    assert res.ok and len(res.data) == 31
    row = res.data.iloc[0]
    assert row["national_outstanding_eur"] > 0
    # levied_nothing councils carry a NULL collection rate, never 0-vs-0 nonsense
    nothing = res.data[res.data["levied_nothing"]]
    assert nothing["collection_rate_pct"].isna().all()
    # ordered worst-outstanding first
    assert res.data["cumulative_outstanding_eur"].iloc[0] >= res.data["cumulative_outstanding_eur"].iloc[-1]


def test_dossier_signals_for_donegal(conn):
    """Donegal resolves on every signal (the cross-signal example used in the UI)."""
    assert q.chief_executive(conn, "Donegal").data.iloc[0]["chief_executive"]
    assert len(q.collection_rates(conn, "Donegal").data) == 1
    assert len(q.planning_overturn(conn, "Donegal").data) == 1
    assert len(q.derelict_sites_levy(conn, "Donegal").data) == 1


def test_cork_county_planning_recovered(conn):
    """Cork County publishes no AppealRefNumber, so it has no exact-ref matches; the
    extractor's validated spatial_temporal fallback recovers it, so the query now returns
    exactly one row with a plausible overturn rate (was a documented gap before)."""
    res = q.planning_overturn(conn, "Cork County")
    assert res.ok and len(res.data) == 1
    assert 10 <= float(res.data.iloc[0]["overturn_rate_pct"]) <= 45
    assert len(q.chief_executive(conn, "Cork County").data) == 1


def test_noac_scorecard_sligo(conn):
    """Sligo carries the standout structural-deficit value; all five metrics + medians present."""
    res = q.noac_scorecard(conn, "Sligo")
    assert res.ok and len(res.data) == 1
    row = res.data.iloc[0]
    assert round(float(row["revenue_balance_pct"]), 1) == -10.6  # cumulative deficit, % of income
    for col in (
        "nat_revenue_balance_pct",
        "nat_sickness_absence_pct",
        "nat_roads_poor_pct",
        "nat_fire_within_10min_pct",
        "nat_litter_problem_pct",
    ):
        assert row[col] is not None


def test_noac_scorecard_fire_service_null(conn):
    """Authorities with no own brigade (Dublin Fire Brigade / Galway County) are n/a on fire,
    never 0 — so they are not ranked worst for a service they don't run."""
    import pandas as pd

    dlr = q.noac_scorecard(conn, "Dun Laoghaire-Rathdown").data.iloc[0]
    assert pd.isna(dlr["fire_within_10min_pct"])
    assert dlr["roads_poor_pct"] is not None  # other metrics still present


def test_noac_scorecard_m3_m4_promoted(conn):
    """M3 (settled-claims €/capita) and M4 (management-overhead %) are carried with medians.
    Cork County is the standout insurance-claims outlier (€38.19/person)."""
    row = q.noac_scorecard(conn, "Cork County").data.iloc[0]
    assert round(float(row["insurance_claims_per_capita_eur"]), 2) == 38.19
    assert 10 <= float(row["mgmt_overhead_pct"]) <= 17
    assert row["nat_insurance_claims_per_capita_eur"] is not None
    assert row["nat_mgmt_overhead_pct"] is not None


def test_noac_scorecard_history_trend(conn):
    """Multi-year trend (2022-2024) for the sparklines: Sligo has >=2 years of revenue
    balance and shows the deficit easing (2022 worse than 2024)."""
    res = q.noac_scorecard_history(conn, "Sligo")
    assert res.ok and res.data["year"].nunique() >= 2
    bal = res.data.dropna(subset=["revenue_balance_pct"]).sort_values("year")
    assert len(bal) >= 2
    assert float(bal.iloc[0]["revenue_balance_pct"]) < float(bal.iloc[-1]["revenue_balance_pct"])  # 2022 -21 < 2024 -10


def test_noac_indicators_full_set(conn):
    """The All-indicators drill-down returns the council's full published set across many
    families, with raw values + source links."""
    res = q.noac_indicators(conn, "Sligo")
    assert res.ok and len(res.data) > 80
    assert res.data["family"].nunique() >= 8
    assert res.data["raw_value"].notna().all()
    assert res.data["deep_link"].str.contains("noac.ie").all()


def test_cash_signals_co_locates_three(conn):
    """v_la_cash_signals carries the three published finance figures + medians for all 31
    councils; Sligo (the standout deficit) resolves with its rates and benchmarks. The view
    asserts NO relationship between the three — it only co-locates published values."""
    res = q.cash_signals(conn, "Sligo")
    assert res.ok and len(res.data) == 1
    row = res.data.iloc[0]
    assert round(float(row["revenue_balance_pct"]), 1) == -10.6
    assert row["commercial_rates_pct"] is not None
    for col in ("nat_revenue_balance_pct", "nat_commercial_rates_pct", "nat_derelict_collection_pct"):
        assert row[col] is not None


def test_unknown_council_returns_empty(conn):
    res = q.chief_executive(conn, "Atlantis")
    assert res.ok and res.data.empty
