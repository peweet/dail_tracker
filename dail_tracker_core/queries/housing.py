"""National Housing screen retrieval — Streamlit-free.

Retrieval-only SQL against the registered ``v_ssha_waiting_list_*`` views (built by
``dail_tracker_core.connections.housing_conn``). All aggregation / unpivot / rollup /
per-capita lives in ``sql_views/housing/*`` — this layer only SELECTs and filters by
grain/area, returning a ``QueryResult`` so the page can tell "source unavailable" from
"no rows".
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="housing", log=_log)


def waiting_list_totals(conn: duckdb.DuckDBPyConnection, grain: str) -> QueryResult:
    """League-table headline per area at one grain ('county' | 'la' | 'national'):
    waiting total, YoY, %4yr+/%7yr+, population, waiters-per-1,000. Ordered by size."""
    return _run(
        conn,
        "SELECT * FROM v_ssha_waiting_list_totals WHERE grain = ? ORDER BY waiting_total DESC",
        [grain],
    )


def supply_national(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National supply & affordability headline (vacancy, avg private rent, HAP) — the
    counterpart to the demand-side waiting list. Single row; each metric self-periods."""
    return _run(conn, "SELECT * FROM v_housing_supply_national")


# ── International-protection accommodation ────────────────────────────────────
# Retrieval only. Every figure here is AUDIT/REGULATOR NARRATIVE grain
# (value_safe_to_sum=False) — never sum it, never union it with the payments facts.


def ipas_la_profile(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """IP applicants by local authority + per-1,000 population + the C&AG's own band.
    The council-map contract. 31 LAs; the counts sum to the source's published total."""
    return _run(conn, "SELECT * FROM v_ipas_la_profile")


def ipas_operators(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Named centre operators, their HIQA compliance record, and the public money they
    received. Identity-gated: only exactly-resolved operators are present.
    NEVER CAUSAL — the compliance and payment windows differ (see the view's caveat)."""
    return _run(conn, "SELECT * FROM v_ipas_operators")


def ipas_centre_compliance(
    conn: duckdb.DuckDBPyConnection, county: str | None = None
) -> QueryResult:
    """Per-centre, per-standard HIQA judgments with the standard's binding statement.
    Optionally filtered to one county (the map drill-down)."""
    if county:
        return _run(
            conn,
            "SELECT * FROM v_ipas_centre_compliance WHERE county = ? "
            "ORDER BY centre_name, standard_ref",
            [county],
        )
    return _run(conn, "SELECT * FROM v_ipas_centre_compliance ORDER BY centre_name, standard_ref")


def ipas_property_rates(
    conn: duckdb.DuckDBPyConnection, county: str | None = None
) -> QueryResult:
    """What a bed actually costs per person per night (C&AG Annex 10A sample of 20).
    Rates the auditor recorded as 'Unclear' stay NULL — they are not imputed."""
    if county:
        return _run(conn, "SELECT * FROM v_ipas_property_rates WHERE county = ?", [county])
    return _run(conn, "SELECT * FROM v_ipas_property_rates")


def ipas_entitlements(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """What an applicant is entitled to in law, beside what the auditor/inspector found."""
    return _run(conn, "SELECT * FROM v_ipas_entitlements")


def ipas_citations(conn: duckdb.DuckDBPyConnection, doc_key: str | None = None) -> QueryResult:
    """The citation backing store — for the provenance footer. Read this to SOURCE a
    figure, never to aggregate one (it is an archive, not a serving table)."""
    if doc_key:
        return _run(conn, "SELECT * FROM v_ipas_facts WHERE doc_key = ?", [doc_key])
    return _run(conn, "SELECT * FROM v_ipas_facts")


def accommodation_spend_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """State asylum (international-protection) + Ukraine accommodation spend per year,
    split by stream — from the published over-€20k purchase-order registers."""
    return _run(conn, "SELECT * FROM v_accommodation_spend_by_year ORDER BY year")


def accommodation_spend_providers(conn: duckdb.DuckDBPyConnection, limit: int = 40) -> QueryResult:
    """Providers ranked by total committed asylum/Ukraine accommodation spend."""
    return _run(
        conn,
        "SELECT * FROM v_accommodation_spend_providers ORDER BY total_eur DESC LIMIT ?",
        [limit],
    )


def hap_national(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National HAP profile: households, % working, rent burden, years to social housing."""
    return _run(conn, "SELECT * FROM v_housing_hap_national")


def completions_trend(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """National new-dwelling completions per complete year (CSO NDQ09 'State' row)."""
    return _run(conn, "SELECT year, completions FROM v_housing_completions_trend ORDER BY year")


def construction_pipeline(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Social-housing build programme per LA — not-yet-completed pipeline units, the
    on-site subset, completed-to-date, and each LA's share of the national pipeline.
    Ordered largest-pipeline first (the view applies the rank)."""
    return _run(conn, "SELECT * FROM v_housing_construction_pipeline")


def rent_by_county(conn: duckdb.DuckDBPyConnection, county: str) -> QueryResult:
    """Average weekly private rent for one county (Census 2022). Empty for Dublin /
    Galway (F2023B splits them with no single total)."""
    return _run(
        conn,
        "SELECT county, avg_weekly_private_rent, rent_period FROM v_housing_rent_by_county WHERE county = ?",
        [county],
    )


def waiting_list_composition(conn: duckdb.DuckDBPyConnection, grain: str, area: str) -> QueryResult:
    """The five demographic breakdowns for one area (the distribution stripes).
    ord-then-count ordering is applied in the view."""
    return _run(
        conn,
        "SELECT dimension, category, ord, count, pct FROM v_ssha_waiting_list_composition "
        "WHERE grain = ? AND area = ? AND year = 2025",
        [grain, area],
    )
