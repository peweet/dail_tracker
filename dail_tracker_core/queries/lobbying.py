"""Lobbying retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/lobbying_data.py``. Retrieval-only
SELECTs against the ``lobbying_*`` views (plus the charity-financials series view
that the org panel's "Charity finances" tile joins to by rcn). Every join,
aggregate, ranking and flag lives in the registered ``sql_views/*.sql`` — these
functions only SELECT / WHERE / ORDER BY / LIMIT.

Each takes an explicit ``conn`` and returns a ``QueryResult``. This is the
deliberate replacement for the old ``_safe()`` helper, which caught *every*
exception and returned a bare empty DataFrame — silently conflating "source
unavailable" with "no rows". The thin Streamlit wrapper flattens ``.data`` back
to the DataFrame/list contracts the page already expects (so the per-query
no-crash behaviour is preserved), but the 3-state result is now available for
any future interface that wants to distinguish the two.

Connection build (in the wrapper): ``lobbying_*.sql`` registers loud
(``swallow_errors=False`` — a missing core lobbying view is a real break), then
``charity_financials_by_year.sql`` registers soft (its tile degrades gracefully).
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.warning("lobbying query failed: %s | %s", sql[:120], exc)
        return QueryResult.unavailable(f"lobbying query failed: {exc}")


# Full enrichment column set for the lobbying page (leaderboard + org profile).
# Drawn from v_experimental_lobbying_org_index_enriched — every charity-side
# column is null for orgs with no register match.
_ORG_INDEX_COLS = (
    "lobbyist_name, sector_label AS sector, return_count, politicians_targeted,"
    " distinct_policy_areas, first_period, last_period,"
    " website, main_activities, lobbying_profile_url,"
    " register_company_registration_number, register_company_name,"
    " rcn, company_num, status, match_method, entity_age_years,"
    " latest_accounts_period_end, filing_periods_count,"
    " newly_incorporated_flag, state_adjacent_flag, country_established,"
    " trustee_count,"
    " funding_profile, gov_funded_share_latest, dominant_income_source,"
    " share_government, share_other_public, share_philanthropic,"
    " share_donations, share_trading, share_other, share_bequests,"
    " gross_income_latest_eur, gross_expenditure_latest_eur,"
    " employees_band_latest, employees_ft_latest, employees_pt_latest,"
    " volunteers_band_latest,"
    " surplus_deficit_latest, net_assets_latest_eur, cash_at_hand_latest_eur,"
    " total_assets_latest_eur, total_liabilities_latest_eur,"
    " reserves_months, reserves_band,"
    " income_trend, income_change_pct, years_filed,"
    " first_period_year, last_period_year, deficit_years_count,"
    " beneficiary_tags, report_activity_latest, flags"
)


def _topic_where(keywords: list[str]) -> tuple[str, list[str]]:
    """Build an OR-of-LIKE WHERE fragment for a list of keyword phrases."""
    cleaned = [k.strip().lower() for k in keywords if k and k.strip()]
    if not cleaned:
        return "", []
    clauses = " OR ".join(["searchable_text LIKE ?"] * len(cleaned))
    params = [f"%{k}%" for k in cleaned]
    return f"({clauses})", params


# ── Summary ─────────────────────────────────────────────────────────────────


def summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(conn, "SELECT * FROM v_lobbying_summary LIMIT 1")


# ── Return documents (embedded third-party PDFs) ─────────────────────────────


def return_documents_for_org(conn: duckdb.DuckDBPyConnection, org_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT return_id, lobby_url, source_field, pdf_url, host,"
        " date_published_timestamp, public_policy_area"
        " FROM v_lobbying_return_documents"
        " WHERE lobbyist_name = ?"
        " ORDER BY date_published_timestamp DESC NULLS LAST, return_id",
        [org_name],
    )


# ── Politician index ──────────────────────────────────────────────────────────


def politician_index(conn: duckdb.DuckDBPyConnection, year: int | None = None) -> QueryResult:
    if year is not None:
        return _run(
            conn,
            "SELECT rank, member_name, unique_member_code, chamber, position, return_count,"
            " distinct_orgs, distinct_policy_areas, first_period, last_period"
            " FROM v_lobbying_index"
            " WHERE EXTRACT(YEAR FROM CAST(last_period AS DATE)) = ?"
            " ORDER BY return_count DESC",
            [year],
        )
    return _run(
        conn,
        "SELECT rank, member_name, unique_member_code, chamber, position, return_count,"
        " distinct_orgs, distinct_policy_areas, first_period, last_period"
        " FROM v_lobbying_index ORDER BY return_count DESC",
    )


def all_politician_names(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(conn, "SELECT member_name FROM v_lobbying_index ORDER BY member_name")


# ── Org index ─────────────────────────────────────────────────────────────────


def org_index(conn: duckdb.DuckDBPyConnection, exclude_state_adjacent: bool = False) -> QueryResult:
    where = " WHERE (state_adjacent_flag IS DISTINCT FROM TRUE)" if exclude_state_adjacent else ""
    return _run(
        conn,
        f"SELECT {_ORG_INDEX_COLS} FROM v_experimental_lobbying_org_index_enriched{where} ORDER BY return_count DESC",
    )


def all_org_names(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(conn, "SELECT lobbyist_name FROM v_lobbying_org_index ORDER BY lobbyist_name")


def charity_financial_series(conn: duckdb.DuckDBPyConnection, rcn: int) -> QueryResult:
    return _run(
        conn,
        "SELECT period_year, gross_income, gross_expenditure, gov_share, income_trend"
        " FROM v_charity_financials_by_year WHERE rcn = ? ORDER BY period_year",
        [int(rcn)],
    )


# ── Procurement footprint (eTenders cross-reference) ──────────────────────────


def org_procurement(conn: duckdb.DuckDBPyConnection, org_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT lobbyist_name, supplier, n_awards, n_authorities, awarded_value_safe_eur"
        " FROM v_lobbying_org_procurement WHERE lobbyist_name = ? LIMIT 1",
        [org_name],
    )


# ── Contact detail ────────────────────────────────────────────────────────────


def contact_detail(
    conn: duckdb.DuckDBPyConnection, member_name: str, start: str | None = None, end: str | None = None
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
            " period_start_date, source_url, person_primarily_responsible"
            " FROM v_lobbying_contact_detail"
            " WHERE member_name = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [member_name, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
        " period_start_date, source_url, person_primarily_responsible"
        " FROM v_lobbying_contact_detail"
        " WHERE member_name = ?"
        " ORDER BY period_start_date DESC",
        [member_name],
    )


def org_contact_detail(
    conn: duckdb.DuckDBPyConnection, org_name: str, start: str | None = None, end: str | None = None
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
            " period_start_date, source_url, intended_results,"
            " person_primarily_responsible"
            " FROM v_lobbying_contact_detail"
            " WHERE lobbyist_name = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [org_name, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
        " period_start_date, source_url, intended_results,"
        " person_primarily_responsible"
        " FROM v_lobbying_contact_detail"
        " WHERE lobbyist_name = ?"
        " ORDER BY period_start_date DESC",
        [org_name],
    )


def politician_area_returns(
    conn: duckdb.DuckDBPyConnection, member_name: str, area: str, start: str | None = None, end: str | None = None
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
            " period_start_date, source_url, person_primarily_responsible"
            " FROM v_lobbying_contact_detail"
            " WHERE member_name = ? AND public_policy_area = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [member_name, area, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
        " period_start_date, source_url, person_primarily_responsible"
        " FROM v_lobbying_contact_detail"
        " WHERE member_name = ? AND public_policy_area = ?"
        " ORDER BY period_start_date DESC",
        [member_name, area],
    )


def dpo_return_map(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(conn, "SELECT return_id, individual_name FROM v_lobbying_dpo_returns")


def org_politician_returns(
    conn: duckdb.DuckDBPyConnection,
    org_name: str,
    member_name: str,
    start: str | None = None,
    end: str | None = None,
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
            " period_start_date, source_url, intended_results,"
            " person_primarily_responsible"
            " FROM v_lobbying_contact_detail"
            " WHERE lobbyist_name = ? AND member_name = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [org_name, member_name, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
        " period_start_date, source_url, intended_results,"
        " person_primarily_responsible"
        " FROM v_lobbying_contact_detail"
        " WHERE lobbyist_name = ? AND member_name = ?"
        " ORDER BY period_start_date DESC",
        [org_name, member_name],
    )


def dpo_politician_returns(
    conn: duckdb.DuckDBPyConnection,
    individual_name: str,
    member_name: str,
    start: str | None = None,
    end: str | None = None,
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, client_name,"
            " public_policy_area, period_start_date, source_url"
            " FROM v_lobbying_dpo_politician_returns"
            " WHERE individual_name = ? AND member_name = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [individual_name, member_name, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, client_name,"
        " public_policy_area, period_start_date, source_url"
        " FROM v_lobbying_dpo_politician_returns"
        " WHERE individual_name = ? AND member_name = ?"
        " ORDER BY period_start_date DESC",
        [individual_name, member_name],
    )


def politician_area_returns_with_dpo(
    conn: duckdb.DuckDBPyConnection, member_name: str, area: str, start: str | None = None, end: str | None = None
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
            " period_start_date, source_url, dpo_individuals, dpo_count"
            " FROM v_lobbying_contact_detail_with_dpo"
            " WHERE member_name = ? AND public_policy_area = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [member_name, area, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
        " period_start_date, source_url, dpo_individuals, dpo_count"
        " FROM v_lobbying_contact_detail_with_dpo"
        " WHERE member_name = ? AND public_policy_area = ?"
        " ORDER BY period_start_date DESC",
        [member_name, area],
    )


def area_contact_detail(
    conn: duckdb.DuckDBPyConnection, area: str, start: str | None = None, end: str | None = None
) -> QueryResult:
    if start and end:
        return _run(
            conn,
            "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
            " period_start_date, source_url, person_primarily_responsible"
            " FROM v_lobbying_contact_detail"
            " WHERE public_policy_area = ?"
            " AND period_start_date BETWEEN ? AND ?"
            " ORDER BY period_start_date DESC",
            [area, start, end],
        )
    return _run(
        conn,
        "SELECT return_id, member_name, lobbyist_name, public_policy_area,"
        " period_start_date, source_url, person_primarily_responsible"
        " FROM v_lobbying_contact_detail"
        " WHERE public_policy_area = ?"
        " ORDER BY period_start_date DESC",
        [area],
    )


# ── Policy area summary ───────────────────────────────────────────────────────


def policy_area_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(
        conn,
        "SELECT public_policy_area, return_count, distinct_orgs, distinct_politicians"
        " FROM v_lobbying_policy_area_summary ORDER BY return_count DESC",
    )


# ── Topic keyword search ──────────────────────────────────────────────────────


def topic_returns(
    conn: duckdb.DuckDBPyConnection, keywords: tuple[str, ...], start: str | None = None, end: str | None = None
) -> QueryResult:
    where_kw, params = _topic_where(list(keywords))
    if not where_kw:
        return QueryResult.success(pd.DataFrame())
    sql = (
        "SELECT return_id, lobbyist_name, public_policy_area,"
        " relevant_matter, specific_details, intended_results,"
        " period_start_date, source_url, person_primarily_responsible"
        " FROM v_lobbying_topic_search"
        f" WHERE {where_kw}"
    )
    if start and end:
        sql += " AND period_start_date BETWEEN ? AND ?"
        params += [start, end]
    sql += " ORDER BY period_start_date DESC"
    return _run(conn, sql, params)


def topic_summary(conn: duckdb.DuckDBPyConnection, keywords: tuple[str, ...]) -> QueryResult:
    where_kw, params = _topic_where(list(keywords))
    if not where_kw:
        return QueryResult.success(pd.DataFrame())
    return _run(
        conn,
        "SELECT COUNT(*) AS total_returns,"
        " COUNT(DISTINCT lobbyist_name) AS distinct_orgs,"
        " COUNT(DISTINCT public_policy_area) AS distinct_areas,"
        " MIN(period_start_date) AS first_period,"
        " MAX(period_start_date) AS last_period"
        " FROM v_lobbying_topic_search"
        f" WHERE {where_kw}",
        params,
    )


# ── Recent returns ────────────────────────────────────────────────────────────


def recent_returns(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(
        conn,
        "SELECT period_start_date, lobbyist_name, public_policy_area,"
        " relevant_matter, source_url"
        " FROM v_lobbying_recent_returns ORDER BY period_start_date DESC LIMIT 20",
    )


# ── Revolving door ────────────────────────────────────────────────────────────


def revolving_door(conn: duckdb.DuckDBPyConnection, limit: int | None = 50) -> QueryResult:
    if limit is None:
        return _run(
            conn,
            "SELECT individual_name, former_position, former_chamber, chamber_display,"
            " return_count, distinct_firms, distinct_policy_areas,"
            " distinct_politicians_targeted"
            " FROM v_lobbying_revolving_door ORDER BY return_count DESC",
        )
    return _run(
        conn,
        "SELECT individual_name, former_position, former_chamber, chamber_display,"
        " return_count, distinct_firms, distinct_policy_areas,"
        " distinct_politicians_targeted"
        " FROM v_lobbying_revolving_door ORDER BY return_count DESC LIMIT ?",
        [int(limit)],
    )


def revolving_door_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(
        conn,
        "SELECT COUNT(*) AS individuals, SUM(return_count) AS total_returns,"
        " SUM(distinct_politicians_targeted) AS politicians_targeted_sum"
        " FROM v_lobbying_revolving_door",
    )


def dpo_one(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT individual_name, former_position, former_chamber, chamber_display,"
        " return_count, distinct_firms, distinct_policy_areas,"
        " distinct_politicians_targeted"
        " FROM v_lobbying_revolving_door WHERE individual_name = ? LIMIT 1",
        [individual_name],
    )


def dpo_firms(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT lobbyist_name, return_count, first_period, last_period"
        " FROM v_lobbying_dpo_firms"
        " WHERE individual_name = ?"
        " ORDER BY return_count DESC",
        [individual_name],
    )


def dpo_client_breakdown(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT client_name, return_count, first_period, last_period"
        " FROM v_lobbying_dpo_clients"
        " WHERE individual_name = ?"
        " ORDER BY return_count DESC",
        [individual_name],
    )


def dpo_politicians_targeted(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT member_name, chamber, return_count"
        " FROM v_lobbying_dpo_politicians"
        " WHERE individual_name = ?"
        " ORDER BY return_count DESC",
        [individual_name],
    )


# ── DPO individual profile ────────────────────────────────────────────────────


def dpo_returns(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT return_id, member_name, public_policy_area,"
        " period_start_date, source_url"
        " FROM v_lobbying_contact_detail"
        " WHERE lobbyist_name = ?"
        " ORDER BY period_start_date DESC",
        [individual_name],
    )


def dpo_clients(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT client_name, period_start_date, policy_areas,"
        " politicians_count, source_url"
        " FROM v_lobbying_clients"
        " WHERE lobbying_firm = ?"
        " ORDER BY period_start_date DESC",
        [individual_name],
    )


def dpo_returns_detail(conn: duckdb.DuckDBPyConnection, individual_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT return_id, period_start_date, lobbyist_name, client_name,"
        " public_policy_area, source_url"
        " FROM v_lobbying_dpo_returns"
        " WHERE individual_name = ?"
        " ORDER BY period_start_date DESC",
        [individual_name],
    )


# ── Source links ──────────────────────────────────────────────────────────────


def sources_for_politician(conn: duckdb.DuckDBPyConnection, member_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT return_id, source_url, official_pdf_url, oireachtas_url"
        " FROM v_lobbying_sources WHERE member_name = ?"
        " ORDER BY return_id LIMIT 20",
        [member_name],
    )


def sources_for_org(conn: duckdb.DuckDBPyConnection, org_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT return_id, source_url, official_pdf_url, oireachtas_url"
        " FROM v_lobbying_sources WHERE lobbyist_name = ?"
        " ORDER BY return_id LIMIT 20",
        [org_name],
    )


# ── Org intensity (bilateral relationships) ───────────────────────────────────


def orgs_for_politician(conn: duckdb.DuckDBPyConnection, member_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT lobbyist_name, returns_in_relationship, distinct_policy_areas,"
        " distinct_periods, first_contact, last_contact"
        " FROM v_lobbying_org_intensity"
        " WHERE member_name = ?"
        " ORDER BY returns_in_relationship DESC LIMIT 20",
        [member_name],
    )


def politicians_for_org(conn: duckdb.DuckDBPyConnection, org_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT member_name, unique_member_code, chamber, returns_in_relationship,"
        " distinct_policy_areas, distinct_periods, first_contact, last_contact"
        " FROM v_lobbying_org_intensity"
        " WHERE lobbyist_name = ?"
        " ORDER BY returns_in_relationship DESC LIMIT 20",
        [org_name],
    )


# ── Persistence ───────────────────────────────────────────────────────────────


def org_persistence(conn: duckdb.DuckDBPyConnection, org_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT first_return_date, last_return_date, total_returns,"
        " distinct_periods_filed, active_span_days"
        " FROM v_lobbying_persistence WHERE lobbyist_name = ? LIMIT 1",
        [org_name],
    )


# ── Policy exposure ───────────────────────────────────────────────────────────


def policy_exposure_for_politician(conn: duckdb.DuckDBPyConnection, member_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT public_policy_area, returns_targeting, distinct_lobbyists"
        " FROM v_lobbying_policy_exposure"
        " WHERE member_name = ?"
        " ORDER BY returns_targeting DESC",
        [member_name],
    )


def politicians_for_area(conn: duckdb.DuckDBPyConnection, area: str) -> QueryResult:
    return _run(
        conn,
        "SELECT member_name, unique_member_code, chamber, returns_targeting, distinct_lobbyists"
        " FROM v_lobbying_policy_exposure"
        " WHERE public_policy_area = ?"
        " ORDER BY returns_targeting DESC LIMIT 20",
        [area],
    )


# ── Clients ───────────────────────────────────────────────────────────────────


def clients_for_org(conn: duckdb.DuckDBPyConnection, org_name: str) -> QueryResult:
    return _run(
        conn,
        "SELECT client_name, period_start_date, policy_areas,"
        " politicians_count, source_url"
        " FROM v_lobbying_clients"
        " WHERE lobbying_firm = ?"
        " ORDER BY period_start_date DESC LIMIT 50",
        [org_name],
    )
