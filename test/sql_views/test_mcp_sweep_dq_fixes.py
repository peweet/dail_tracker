"""Guards for the 2026-07-13 view-level DQ fixes (MCP association sweep 07-11).

Covers, at the registered-view level (integration-style — each test skips when
the real pipeline output is absent on the box):

  1. PRIVACY — v_accommodation_spend_providers must never surface a provider
     name the payments fact quarantines as personal data (public_display gate
     on the fact branch + anti-join gate on the DCEDIY legacy branch).
  2. ATTENDANCE — v_attendance_participation_turnout: votes count toward the
     DIVISION's house (from its vote_url), denominators are service-window
     scoped (member_terms), turnout can never exceed 100%; office flags are
     date-bounded on both the participation and TAA (member-year-summary)
     chains.
  3. CORPORATE — v_corporate_cbi_notice_match / v_corporate_cbi_repeat_distress
     exclude fragment entity names and misfiled non-corporate notices, and
     trustee-capacity receiverships do not count as distress of the trustee.

Run with:  pytest test/sql_views/test_mcp_sweep_dq_fixes.py -v -m sql
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import duckdb
import pytest

PROJECT_ROOT = Path(__file__).parents[2]
SQL_VIEWS_DIR = PROJECT_ROOT / "sql_views"
GOLD = PROJECT_ROOT / "data" / "gold" / "parquet"
SILVER_PQ = PROJECT_ROOT / "data" / "silver" / "parquet"


def _con() -> duckdb.DuckDBPyConnection:
    return duckdb.connect()


def _load(con: duckdb.DuckDBPyConnection, filename: str) -> None:
    """Load one sql_views file, rewriting 'data/...' literals to this repo root
    (mirrors production absolutize_data_paths, CWD-independent)."""
    matches = sorted(SQL_VIEWS_DIR.glob(f"**/{filename}"))
    assert matches, f"no view file {filename!r} under {SQL_VIEWS_DIR}"
    sql = matches[0].read_text(encoding="utf-8")
    con.execute(sql.replace("'data/", f"'{PROJECT_ROOT.as_posix()}/data/"))


def _skip_missing(*paths: Path) -> None:
    for p in paths:
        if not p.exists():
            pytest.skip(f"source not on this box: {p.name}")


# ---------------------------------------------------------------------------
# 1. Privacy gate — accommodation providers
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_accommodation_providers_never_surface_quarantined_names():
    _skip_missing(
        GOLD / "procurement_payments_fact.parquet",
        GOLD / "dceidy_ipas_legacy_spend.parquet",
    )
    con = _con()
    _load(con, "housing_accommodation_spend_providers.sql")
    fact = (GOLD / "procurement_payments_fact.parquet").as_posix()

    leaks = con.execute(
        f"""
        SELECT count(*) FROM v_accommodation_spend_providers
        WHERE provider IN (
            SELECT DISTINCT supplier_normalised
            FROM read_parquet('{fact}')
            WHERE privacy_status = 'review_personal_data'
              AND supplier_normalised IS NOT NULL
        )
        """
    ).fetchone()[0]
    assert leaks == 0, f"{leaks} quarantined personal-data names surfaced in the providers view"

    # The gate must not hollow the view out — the named-provider ranking stays real.
    n = con.execute("SELECT count(*) FROM v_accommodation_spend_providers").fetchone()[0]
    assert n > 1000


@pytest.mark.sql
def test_accommodation_by_year_totals_keep_anonymous_aggregates():
    """Only NAMES are gated: the per-year aggregate view keeps the full total,
    so its sum must be >= the (gated) providers view sum."""
    _skip_missing(
        GOLD / "procurement_payments_fact.parquet",
        GOLD / "dceidy_ipas_legacy_spend.parquet",
    )
    con = _con()
    _load(con, "housing_accommodation_spend_by_year.sql")
    _load(con, "housing_accommodation_spend_providers.sql")
    yr_total = con.execute("SELECT SUM(total_eur) FROM v_accommodation_spend_by_year").fetchone()[0]
    prov_total = con.execute("SELECT SUM(total_eur) FROM v_accommodation_spend_providers").fetchone()[0]
    assert yr_total >= prov_total > 0


# ---------------------------------------------------------------------------
# 2. Attendance — cross-house, service windows, date-bounded office flags
# ---------------------------------------------------------------------------

_VOTE_SOURCES = (
    GOLD / "current_dail_vote_history.parquet",
    GOLD / "current_seanad_vote_history.parquet",
)


def _turnout_con() -> duckdb.DuckDBPyConnection:
    con = _con()
    _load(con, "attendance_participation_turnout.sql")
    return con


@pytest.mark.sql
def test_turnout_votes_scored_against_division_house():
    """THE cross-house guard: per (member, house, year), voted_in must equal the
    count of distinct divisions whose OWN vote_url house matches — recomputed
    here independently of which source file a row sat in. A recurrence of the
    Kyne defect (Seanad votes scored in the Dáil cut) fails this exactly."""
    _skip_missing(*_VOTE_SOURCES, SILVER_PQ / "member_terms.parquet", GOLD / "participation_member_year.parquet")
    con = _turnout_con()
    dail, seanad = (p.as_posix() for p in _VOTE_SOURCES)
    mismatches = con.execute(
        f"""
        WITH votes AS (
            SELECT unique_member_code, vote_id, CAST(date AS DATE) AS d,
                   CASE WHEN vote_url LIKE '%/debates/vote/dail/%' THEN 'Dáil'
                        WHEN vote_url LIKE '%/debates/vote/seanad/%' THEN 'Seanad' END AS house
            FROM (SELECT * FROM read_parquet('{dail}') UNION ALL BY NAME SELECT * FROM read_parquet('{seanad}'))
            WHERE unique_member_code IS NOT NULL AND date IS NOT NULL
        ),
        expected AS (
            SELECT unique_member_code, house, YEAR(d) AS year, COUNT(DISTINCT vote_id) AS n
            FROM votes WHERE house IS NOT NULL AND YEAR(d) >= 2025
            GROUP BY 1, 2, 3
        ),
        actual AS (
            SELECT unique_member_code, house, year, SUM(voted_in) AS n
            FROM v_attendance_participation_turnout
            GROUP BY 1, 2, 3
        )
        SELECT count(*)
        FROM expected e
        FULL JOIN actual a USING (unique_member_code, house, year)
        WHERE COALESCE(e.n, -1) <> COALESCE(a.n, -1)
        """
    ).fetchone()[0]
    assert mismatches == 0, f"{mismatches} member-house-year rows disagree with division-house-derived counts"


@pytest.mark.sql
def test_turnout_denominator_is_service_windowed():
    _skip_missing(*_VOTE_SOURCES, SILVER_PQ / "member_terms.parquet", GOLD / "participation_member_year.parquet")
    con = _turnout_con()
    dail = _VOTE_SOURCES[0].as_posix()
    terms = (SILVER_PQ / "member_terms.parquet").as_posix()

    # never > 100% and denominator always >= voted_in
    bad = con.execute(
        "SELECT count(*) FROM v_attendance_participation_turnout"
        " WHERE turnout_pct > 100 OR voted_in > total_divisions OR missed < 0"
    ).fetchone()[0]
    assert bad == 0

    # every mid-year arrival in a house is scored only against divisions held
    # since their membership start (single-term members make the expectation
    # unambiguous). Daniel Ennis (TD from 2026-05-25) is the sweep's archetype
    # but the assertion is generic over whoever is mid-year in the current data.
    rows = con.execute(
        f"""
        WITH one_term AS (   -- members with exactly one Dáil term, starting mid-year
            SELECT unique_member_code,
                   MIN(CAST(membership_start_date AS DATE)) AS t_start
            FROM read_parquet('{terms}')
            WHERE lower(house) = 'dail'
            GROUP BY 1
            HAVING count(*) = 1
               AND MONTH(MIN(CAST(membership_start_date AS DATE))) > 1
               AND MAX(membership_end_date) IS NULL
        ),
        div_since AS (
            SELECT o.unique_member_code, YEAR(CAST(v.date AS DATE)) AS year,
                   COUNT(DISTINCT v.vote_id) AS n_expected
            FROM one_term o
            JOIN read_parquet('{dail}') v
              ON CAST(v.date AS DATE) >= o.t_start
             AND YEAR(CAST(v.date AS DATE)) = YEAR(o.t_start)
            GROUP BY 1, 2
        )
        SELECT count(*)
        FROM div_since d
        JOIN v_attendance_participation_turnout t
          ON t.unique_member_code = d.unique_member_code
         AND t.house = 'Dáil' AND t.year = d.year
        WHERE t.total_divisions <> GREATEST(t.voted_in, d.n_expected)
        """
    ).fetchone()[0]
    assert rows == 0, f"{rows} mid-year arrivals scored against a non-window denominator"


@pytest.mark.sql
def test_office_flags_date_bounded_participation():
    """Anchors on published, closed facts (stable for past years): Paschal
    Donohoe was Minister for Finance to 2025-11-18 → his 2025 participation row
    is is_minister=TRUE; Michael Healy-Rae's MoS post ran 2025-01-29→2026-04-14
    → TRUE on 2025."""
    _skip_missing(
        *_VOTE_SOURCES,
        SILVER_PQ / "member_terms.parquet",
        GOLD / "participation_member_year.parquet",
        PROJECT_ROOT / "data" / "silver" / "ministerial_tenure.parquet",
        SILVER_PQ / "flattened_members.parquet",
    )
    con = _turnout_con()
    got = dict(
        con.execute(
            "SELECT member_name, bool_or(is_minister) FROM v_attendance_participation_turnout"
            " WHERE year = 2025 AND member_name IN ('Paschal Donohoe', 'Michael Healy-Rae')"
            " GROUP BY 1"
        ).fetchall()
    )
    assert got.get("Paschal Donohoe") is True, "Donohoe 2025 must be flagged minister (in post to 2025-11-18)"
    assert got.get("Michael Healy-Rae") is True, "Healy-Rae 2025 must be flagged (MoS from 2025-01-29)"


@pytest.mark.sql
def test_office_flags_date_bounded_taa_chain():
    """Michael Healy-Rae's pre-office years must NOT carry the flag on the TAA
    (member-year-summary → year-rank) chain."""
    _skip_missing(
        GOLD / "attendance_by_td_year.parquet",
        GOLD / "seanad_attendance_by_year.parquet",
        PROJECT_ROOT / "data" / "silver" / "ministerial_tenure.parquet",
        SILVER_PQ / "flattened_members.parquet",
    )
    con = _con()
    _load(con, "attendance_member_year_summary.sql")
    rows = dict(
        con.execute(
            "SELECT year, bool_or(is_minister) FROM v_attendance_member_year_summary"
            " WHERE member_name = 'Michael Healy-Rae' AND year IN (2023, 2024, 2025)"
            " GROUP BY 1"
        ).fetchall()
    )
    if not rows:
        pytest.skip("Michael Healy-Rae not in the TAA record on this box")
    assert rows.get(2023) is not True, "pre-office 2023 row retroactively flagged minister"
    assert rows.get(2024) is not True, "pre-office 2024 row retroactively flagged minister"
    if 2025 in rows:
        assert rows[2025] is True, "2025 row (MoS from 2025-01-29) must be flagged"


# ---------------------------------------------------------------------------
# 3. Corporate — repeat-distress party-role / parse-quality gates
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_corporate_distress_gates():
    _skip_missing(GOLD / "cbi_xref_corporate_notices.parquet")
    con = _con()
    _load(con, "corporate_cbi_distress.sql")

    # no fragment entity names ("Limited and Allied Irish Banks … and") survive
    frags = con.execute(
        "SELECT count(*) FROM v_corporate_cbi_notice_match"
        " WHERE regexp_matches(entity_name, '^(and|Limited and|limited and) ')"
        "    OR regexp_matches(entity_name, ' and[,.]?$')"
    ).fetchone()[0]
    assert frags == 0

    # trustee-capacity receiverships never count as distress of the trustee:
    # distress + routine + capacity-flagged can never exceed the notice total,
    # and any entity with capacity rows has n_distress strictly below its raw
    # distress-subtype sum.
    bad = con.execute(
        """
        SELECT count(*) FROM v_corporate_cbi_repeat_distress
        WHERE n_distress > n_notices_total - n_trustee_capacity
           OR (n_trustee_capacity > 0
               AND n_distress >= n_receivership + n_court_winding_up + n_examinership
                                 + n_scarp + n_creditors_vl)
        """
    ).fetchone()[0]
    assert bad == 0

    # anchor: the historical Iris corpus contains trustee-capacity receiverships
    # (Independent Trustee / Wealth Options) — the flag must be populated.
    n_cap = con.execute("SELECT COALESCE(SUM(n_trustee_capacity), 0) FROM v_corporate_cbi_repeat_distress").fetchone()[
        0
    ]
    assert n_cap >= 1


# ---------------------------------------------------------------------------
# 4. Housing staleness signals
# ---------------------------------------------------------------------------


@pytest.mark.sql
def test_housing_supply_and_hap_carry_staleness_signals():
    _skip_missing(
        GOLD / "cso_vac14.parquet",
        GOLD / "cso_f2023b.parquet",
        GOLD / "cso_hap01.parquet",
        GOLD / "cso_hap17.parquet",
        GOLD / "cso_hap20.parquet",
        GOLD / "cso_hap32.parquet",
    )
    con = _con()
    _load(con, "housing_supply_national.sql")
    _load(con, "housing_hap_national.sql")
    this_year = dt.date.today().year

    sup = con.execute("SELECT * FROM v_housing_supply_national").fetchdf().iloc[0]
    for prefix in ("rent", "hap"):
        age = sup[f"{prefix}_period_age_years"]
        assert age == this_year - int(sup[f"{prefix}_period"])
        assert bool(sup[f"{prefix}_stale"]) == (age >= 2)

    hap = con.execute("SELECT * FROM v_housing_hap_national").fetchdf().iloc[0]
    assert hap["hap_period_age_years"] == this_year - int(hap["hap_period"])
    assert bool(hap["hap_stale"]) == (hap["hap_period_age_years"] >= 2)
