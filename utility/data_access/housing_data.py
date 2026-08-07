"""National Housing screen data access — thin framework-neutral cached wrapper over core.

Owns only framework-neutral caching (``cache_resource`` for the connection,
``cache_data`` for per-query memoisation). All retrieval SQL + QueryResult
state live in ``dail_tracker_core.queries.housing``; all aggregation /
unpivot / rollup / per-capita live in ``sql_views/housing/*``.

Forbidden here (same contract as the other data-access modules): JOIN / GROUP BY
/ WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions.
"""

from __future__ import annotations

import duckdb
from data_access._cache import cache_data, cache_resource

from dail_tracker_core.connections import housing_conn
from dail_tracker_core.queries import housing as _q
from dail_tracker_core.results import QueryResult


@cache_resource
def get_housing_conn() -> duckdb.DuckDBPyConnection:
    """One connection per session: the SSHA waiting-list composition + totals views."""
    return housing_conn()


@cache_data(ttl=600)
def fetch_waiting_list_totals_result(grain: str) -> QueryResult:
    return _q.waiting_list_totals(get_housing_conn(), grain)


@cache_data(ttl=600)
def fetch_waiting_list_composition_result(grain: str, area: str) -> QueryResult:
    return _q.waiting_list_composition(get_housing_conn(), grain, area)


@cache_data(ttl=600)
def fetch_housing_supply_national_result() -> QueryResult:
    return _q.supply_national(get_housing_conn())


@cache_data(ttl=600)
def fetch_housing_hap_national_result() -> QueryResult:
    return _q.hap_national(get_housing_conn())


@cache_data(ttl=600)
def fetch_ipas_la_profile_result() -> QueryResult:
    """Council-map contract: IP applicants per local authority + per-1,000 population."""
    return _q.ipas_la_profile(get_housing_conn())


@cache_data(ttl=600)
def fetch_ipas_operators_result() -> QueryResult:
    """Named operators + compliance record + public money. Identity-gated; never causal."""
    return _q.ipas_operators(get_housing_conn())


@cache_data(ttl=600)
def fetch_ipas_centre_compliance_result(county: str | None = None) -> QueryResult:
    """Per-centre, per-standard HIQA judgments (optionally one county — the drill-down)."""
    return _q.ipas_centre_compliance(get_housing_conn(), county)


@cache_data(ttl=600)
def fetch_ipas_property_rates_result(county: str | None = None) -> QueryResult:
    """What a bed costs per person per night (C&AG Annex 10A)."""
    return _q.ipas_property_rates(get_housing_conn(), county)


@cache_data(ttl=600)
def fetch_ipas_entitlements_result() -> QueryResult:
    """Entitlement in law vs what the auditor and inspector found."""
    return _q.ipas_entitlements(get_housing_conn())


@cache_data(ttl=600)
def fetch_ipas_citations_result(doc_key: str | None = None) -> QueryResult:
    """Citation backing store for the provenance footer — source figures, don't aggregate."""
    return _q.ipas_citations(get_housing_conn(), doc_key)


@cache_data(ttl=600)
def fetch_accommodation_spend_by_year_result() -> QueryResult:
    return _q.accommodation_spend_by_year(get_housing_conn())


@cache_data(ttl=600)
def fetch_accommodation_spend_providers_result(limit: int = 40) -> QueryResult:
    return _q.accommodation_spend_providers(get_housing_conn(), limit)


@cache_data(ttl=600)
def fetch_housing_completions_trend_result() -> QueryResult:
    return _q.completions_trend(get_housing_conn())


@cache_data(ttl=600)
def fetch_housing_rent_by_county_result(county: str) -> QueryResult:
    return _q.rent_by_county(get_housing_conn(), county)


@cache_data(ttl=600)
def fetch_housing_construction_pipeline_result() -> QueryResult:
    """Per-LA social-housing build programme (pipeline / on-site / completed)."""
    return _q.construction_pipeline(get_housing_conn())
