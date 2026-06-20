"""Procurement (eTenders) data access — thin Streamlit wrapper over dail_tracker_core.

This module is now a THIN adapter. The retrieval SQL and the QueryResult
state-handling live in ``dail_tracker_core.queries.procurement``; this file owns
only the Streamlit caching (``st.cache_resource`` for the connection,
``st.cache_data`` for per-query memoisation) and unwraps ``QueryResult`` to the
DataFrame the page expects.

Return contract is preserved EXACTLY: on a source failure the core returns an
``unavailable`` QueryResult whose ``.data`` is an empty DataFrame, so callers see
the same empty frame the old ``_safe`` produced. The richer ok/unavailable
distinction is now available in core for a future page revision that wants to
render "source unavailable" explicitly instead of an empty state.

Forbidden here (unchanged contract): JOIN / GROUP BY / HAVING / WINDOW in SQL,
CREATE VIEW, read_parquet, pandas merge/pivot, business-metric definitions — all
of which live in sql_views/ and dail_tracker_core.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import procurement as _q
from dail_tracker_core.results import QueryResult

_ROOT = Path(__file__).resolve().parents[2]


@st.cache_resource
def get_procurement_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["procurement_*.sql"], swallow_errors=True)


@st.cache_data(ttl=600)
def fetch_coverage() -> dict:
    """The committed coverage/limitations metadata (CRO-match %, quarantined
    sole-trader count, source/licence). Static precomputed JSON — display only,
    no aggregation. Returns {} if the file is absent (page degrades gracefully)."""
    path = _ROOT / "data" / "_meta" / "procurement_coverage.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — missing/garbled metadata must not break the page
        return {}


@st.cache_data(ttl=300)
def fetch_supplier_concentration_result(top_n: int = 10) -> QueryResult:
    """Top-N share of all company-class awards (market-concentration line)."""
    return _q.supplier_concentration(get_procurement_conn(), top_n=top_n)


@st.cache_data(ttl=600)
def fetch_awards_by_year_result() -> QueryResult:
    """Company-class award counts per year (the trend mini-chart)."""
    return _q.awards_by_year(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_value_contrast_result() -> QueryResult:
    """Whole-corpus naive-vs-safe value contrast for the '€570bn that isn't' panel
    (ungated — the dataset-level literacy story, not the company-class rankings slice)."""
    return _q.value_contrast(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_coverage_stats_result() -> QueryResult:
    """Live one-row corpus summary (counts, date span, sum-safe total) for the hero
    scale anchor. Also the page's source-state gate — if this is unavailable the
    procurement views failed to load."""
    return _q.coverage_stats(get_procurement_conn())


# ── QueryResult-returning siblings ────────────────────────────────────────────
# The page boundary uses these to distinguish "source unavailable" (a missing
# view/parquet/DuckDB error) from "ran, no rows" — the ``.data`` wrappers below
# collapse both to an empty DataFrame for back-compat. Caching a frozen
# QueryResult is safe (see dail_tracker_core/results.py).
@st.cache_data(ttl=300)
def fetch_supplier_summary_result(
    limit: int | None = None, order_by: str = "awards", year: int | None = None
) -> QueryResult:
    return _q.supplier_summary(get_procurement_conn(), limit=limit, order_by=order_by, year=year)


@st.cache_data(ttl=300)
def fetch_supplier_year_trend_result(supplier_norm: str) -> QueryResult:
    """One firm's public-sector work secured per year (award count + sum-safe awarded value) —
    the 'how is this firm's public order book trending' panel. Public register only, not turnover."""
    return _q.supplier_year_trend(get_procurement_conn(), supplier_norm)


@st.cache_data(ttl=300)
def fetch_supplier_payments_by_year_result(supplier_norm: str) -> QueryResult:
    """One firm's public-body payments received per year (paid + ordered as separate tiers) — the
    supplier-side mirror of the council spend-over-time spine. Different grain from awards; never summed."""
    return _q.supplier_payments_by_year(get_procurement_conn(), supplier_norm)


@st.cache_data(ttl=300)
def fetch_epa_compliance_result(company_num: int) -> QueryResult:
    """One CRO company's EPA environmental-licence + enforcement record for the dossier's EPA panel
    (matched on CRO company_num). A separate public register — licences + compliance counts only, never
    summed with the firm's award/payment figures."""
    return _q.epa_compliance_for_supplier(get_procurement_conn(), company_num)


@st.cache_data(ttl=300)
def fetch_epa_supplier_index_result() -> QueryResult:
    """Every CRO company holding an EPA licence (company_num + licence/enforcement counts) —
    the index behind the Companies landing's EPA badge + filter. Display-only membership lookup."""
    return _q.epa_supplier_index(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_authority_summary_result(
    limit: int | None = 50, order_by: str = "awards", year: int | None = None
) -> QueryResult:
    return _q.authority_summary(get_procurement_conn(), limit=limit, order_by=order_by, year=year)


@st.cache_data(ttl=300)
def fetch_cpv_summary_result(limit: int | None = 50, order_by: str = "awards", year: int | None = None) -> QueryResult:
    return _q.cpv_summary(get_procurement_conn(), limit=limit, order_by=order_by, year=year)


@st.cache_data(ttl=600)
def fetch_available_years() -> list[int]:
    """Award years present (newest first) for the year-pill option list. Empty list
    if the source is unavailable (the page then simply omits the pills)."""
    res = _q.available_years(get_procurement_conn())
    if not res.ok or res.data.empty:
        return []
    return [int(y) for y in res.data["year"].tolist()]


@st.cache_data(ttl=300)
def fetch_lobbying_overlap_result() -> QueryResult:
    return _q.lobbying_overlap(get_procurement_conn())


# ── Public-body PAYMENTS (SPENT / COMMITTED) — different grain from awards ─────
@st.cache_data(ttl=300)
def fetch_payments_corpus_stats_result() -> QueryResult:
    return _q.payments_corpus_stats(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_payments_publisher_summary_result(tier: str = "SPENT", limit: int | None = 60) -> QueryResult:
    return _q.payments_publisher_summary(get_procurement_conn(), tier=tier, limit=limit)


@st.cache_data(ttl=300)
def fetch_payments_supplier_summary_result(tier: str = "SPENT", limit: int | None = 60) -> QueryResult:
    return _q.payments_supplier_summary(get_procurement_conn(), tier=tier, limit=limit)


@st.cache_data(ttl=300)
def fetch_payments_for_publisher_result(publisher_name: str, tier: str = "SPENT") -> QueryResult:
    return _q.payments_for_publisher(get_procurement_conn(), publisher_name, tier=tier)


@st.cache_data(ttl=300)
def fetch_council_summary_result() -> QueryResult:
    """Publishing local authorities for the 'Your council' index — pre-grouped by province."""
    return _q.council_summary(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_payments_publisher_profile_result(publisher_name: str) -> QueryResult:
    return _q.payments_publisher_profile(get_procurement_conn(), publisher_name)


@st.cache_data(ttl=300)
def fetch_payments_by_year_result(publisher_name: str, tier: str = "SPENT") -> QueryResult:
    """One body's sum-safe spend per year, one tier (the dossier spend-over-time chart)."""
    return _q.payments_by_year(get_procurement_conn(), publisher_name, tier=tier)


@st.cache_data(ttl=300)
def fetch_payments_for_supplier_result(supplier_norm: str) -> QueryResult:
    return _q.payments_for_supplier(get_procurement_conn(), supplier_norm)


@st.cache_data(ttl=300)
def fetch_payments_supplier_header_result(supplier_norm: str) -> QueryResult:
    """Single-row header for the paid-supplier drill-down (display name + both tier totals)."""
    return _q.payments_supplier_header(get_procurement_conn(), supplier_norm)


@st.cache_data(ttl=300)
def fetch_payments_publishers_for_supplier_result(supplier_norm: str, tier: str = "SPENT") -> QueryResult:
    """The public bodies that paid/ordered from one supplier (the drill-down line items)."""
    return _q.payments_publishers_for_supplier(get_procurement_conn(), supplier_norm, tier=tier)


@st.cache_data(ttl=300)
def fetch_payment_lines_for_pair_result(supplier_norm: str, publisher_name: str, tier: str = "SPENT") -> QueryResult:
    """The published payment line items for one (supplier × public body × tier) pair — the leaf
    of the payments drill-down (breaks the supplier↔body card loop by finally showing records)."""
    return _q.payment_lines_for_pair(get_procurement_conn(), supplier_norm, publisher_name, tier=tier)


@st.cache_data(ttl=300)
def fetch_payment_group_header_result(group_slug: str) -> QueryResult:
    """Single-row header for a corporate-group node (Follow-the-money): the group's structure
    (entity / PPP-SPV / JV / no-CRO counts) and both lifecycle-tier sum-safe totals, never summed."""
    return _q.payment_group_header(get_procurement_conn(), group_slug)


@st.cache_data(ttl=300)
def fetch_payment_group_members_result(group_slug: str, tier: str = "SPENT") -> QueryResult:
    """The member entities of a corporate group in one tier, biggest first — each drills into its
    own paid-supplier profile."""
    return _q.payment_group_members(get_procurement_conn(), group_slug, tier=tier)


@st.cache_data(ttl=300)
def fetch_entity_chain_for_company_result(company_num: str) -> QueryResult:
    """One CRO-matched firm's cross-register footprint (eTenders / TED / payments), each
    register's own headline number side by side — never summed (different grains)."""
    return _q.entity_chain_for_company(get_procurement_conn(), company_num)


# ── AFS (per-LA audited accounts) — BUDGET grain; the council-spend denominator ───
# Sibling context fact for the local-authority dossier, never summed with PO/award euros.
@st.cache_data(ttl=300)
def fetch_afs_total_by_year_result(council: str) -> QueryResult:
    """One council's audited revenue-account spend per year (the 'all spending' chart)."""
    return _q.afs_total_by_year(get_procurement_conn(), council)


@st.cache_data(ttl=300)
def fetch_afs_by_division_result(council: str, year: int) -> QueryResult:
    """One council-year's revenue spending by service division (the by-function panel)."""
    return _q.afs_by_division(get_procurement_conn(), council, year)


@st.cache_data(ttl=300)
def fetch_afs_vs_po_coverage_result(council: str, year: int | None = None) -> QueryResult:
    """Audited spend vs the named-supplier (PO) traceable slice, per year (the traceability line)."""
    return _q.afs_vs_po_coverage(get_procurement_conn(), council, year=year)


@st.cache_data(ttl=300)
def fetch_afs_capital_by_year_result(council: str) -> QueryResult:
    """One council's audited capital-account investment per year (the BUILDING-lane spine).
    A distinct grain — never summed with the revenue or PO/award euros."""
    return _q.afs_capital_by_year(get_procurement_conn(), council)


@st.cache_data(ttl=300)
def fetch_afs_capital_by_division_result(council: str, year: int) -> QueryResult:
    """One council-year's capital investment by service division (the build/acquire breakdown)."""
    return _q.afs_capital_by_division(get_procurement_conn(), council, year)


# ── TED (EU-journal awards) — separate register ───────────────────────────────
@st.cache_data(ttl=300)
def fetch_ted_corpus_stats_result() -> QueryResult:
    return _q.ted_corpus_stats(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_ted_competition_stats_result() -> QueryResult:
    """Notice-level competition-intensity summary for the TED tab (single-bid / uncompetitive
    procedure / price-only counts) — factual signals, never a verdict."""
    return _q.ted_competition_stats(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_ted_awards_by_year_result() -> QueryResult:
    """TED award notices per year (2016-2026, pan-EU excluded) — the TED 'over time' trend."""
    return _q.ted_awards_by_year(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_ted_supplier_summary_result(limit: int | None = 60, order_by: str = "awards") -> QueryResult:
    return _q.ted_supplier_summary(get_procurement_conn(), limit=limit, order_by=order_by)


@st.cache_data(ttl=300)
def fetch_ted_for_supplier_result(join_norm: str) -> QueryResult:
    return _q.ted_for_supplier(get_procurement_conn(), join_norm)


@st.cache_data(ttl=300)
def fetch_ted_notices_for_supplier_result(join_norm: str) -> QueryResult:
    """One winner's individual TED award notices + the source notice_url (the conduit to the
    authoritative EU Official Journal notice). One row per notice, newest first."""
    return _q.ted_notices_for_supplier(get_procurement_conn(), join_norm)


@st.cache_data(ttl=300)
def fetch_ted_tenders_stats_result() -> QueryResult:
    """TED tender-pipeline (cn-standard) corpus summary — notice count, span, still-open count."""
    return _q.ted_tenders_stats(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_ted_tenders_result(only_open: bool = False, limit: int | None = 60, sector: str | None = None) -> QueryResult:
    """TED tender-pipeline listing (pre-award competition notices), most recent first. ``sector``
    narrows to one CPV division (the TED feed's sector facet)."""
    return _q.ted_tenders(get_procurement_conn(), limit=limit, only_open=only_open, sector=sector)


@st.cache_data(ttl=300)
def fetch_ted_tender_sectors_result(only_open: bool = False) -> QueryResult:
    """Distinct CPV divisions in the TED tender pipeline with per-division counts — the sector
    facet's option list. ``only_open`` keeps the counts in step with the listing's open toggle."""
    return _q.ted_tender_sectors(get_procurement_conn(), only_open=only_open)


@st.cache_data(ttl=300)
def fetch_expiring_contracts_stats_result() -> QueryResult:
    """Advertised-term corpus summary (estimate counts, 12-month due count, basis mix)."""
    return _q.expiring_contracts_stats(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_expiring_contracts_result(months_ahead: int = 12, limit: int | None = 60) -> QueryResult:
    """Contracts whose advertised term ends within the window, soonest first."""
    return _q.expiring_contracts(get_procurement_conn(), months_ahead=months_ahead, limit=limit)


@st.cache_data(ttl=300)
def fetch_live_tenders_result(
    limit: int | None = 80, within_days: int | None = None, sector: str | None = None
) -> QueryResult:
    """Open NATIONAL tenders (etenders.gov.ie) accepting bids now, soonest-closing first.
    PLANNED tier — estimated_value_eur is a buyer estimate, never summed. ``within_days`` narrows
    by closing date; ``sector`` narrows to one CPV division (available once the snapshot carries
    an enriched CPV — referenced only when a sector is passed, so un-enriched snapshots are unaffected)."""
    return _q.live_tenders(get_procurement_conn(), limit=limit, within_days=within_days, sector=sector)


@st.cache_data(ttl=300)
def fetch_live_tender_sectors_result(within_days: int | None = None) -> QueryResult:
    """Distinct CPV divisions in the open national pipeline + counts (the sector facet's option
    list). Unavailable — so the page omits the facet — until the snapshot is CPV-enriched."""
    return _q.live_tender_sectors(get_procurement_conn(), within_days=within_days)


@st.cache_data(ttl=300)
def fetch_live_tenders_stats_result() -> QueryResult:
    """Open national pipeline summary — open count, distinct buyers, closing-within-14d,
    next closing date, and the snapshot timestamp (retrieved_utc) for the freshness line."""
    return _q.live_tenders_stats(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_expiring_etenders_result(months_ahead: int = 24, limit: int | None = 60) -> QueryResult:
    """NATIONAL (eTenders) contracts whose advertised term ends within the window, soonest first.
    Advertised term, not a verified event; award_value_eur is display-only, never summed."""
    return _q.expiring_contracts_etenders(get_procurement_conn(), months_ahead=months_ahead, limit=limit)


@st.cache_data(ttl=300)
def fetch_awards_for_authority(contracting_authority: str, year: int | None = None) -> pd.DataFrame:
    """Every award made by one contracting authority (drill-down), most recent first."""
    return _q.awards_for_authority(get_procurement_conn(), contracting_authority, year=year).data


@st.cache_data(ttl=300)
def fetch_awards_for_cpv(cpv_code: str, year: int | None = None) -> pd.DataFrame:
    """Every award in one CPV category (drill-down), most recent first."""
    return _q.awards_for_cpv(get_procurement_conn(), cpv_code, year=year).data


@st.cache_data(ttl=300)
def fetch_supplier_summary(limit: int | None = None) -> pd.DataFrame:
    """Supplier ranking — one row per distinct supplier (company-class), ordered by
    contract count (the trustworthy metric). Carries CRO match + lobbying flags."""
    return _q.supplier_summary(get_procurement_conn(), limit=limit).data


@st.cache_data(ttl=300)
def fetch_awards_for_supplier(supplier_norm: str) -> pd.DataFrame:
    """Every award row for one supplier (detail view), most recent first."""
    return _q.awards_for_supplier(get_procurement_conn(), supplier_norm).data


@st.cache_data(ttl=300)
def fetch_authority_summary(limit: int = 50) -> pd.DataFrame:
    """Contracting authorities ranked by number of awards."""
    return _q.authority_summary(get_procurement_conn(), limit=limit).data


@st.cache_data(ttl=300)
def fetch_cpv_summary(limit: int = 50) -> pd.DataFrame:
    """CPV categories ranked by number of awards."""
    return _q.cpv_summary(get_procurement_conn(), limit=limit).data


@st.cache_data(ttl=300)
def fetch_lobbying_overlap() -> pd.DataFrame:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _q.lobbying_overlap(get_procurement_conn()).data


@st.cache_data(ttl=300)
def fetch_charity_overlap_result() -> QueryResult:
    """Registered charities that also win public contracts (shared CRO company
    number — co-occurrence disclosure only, never causation; see the view header)."""
    return _q.charity_overlap(get_procurement_conn())


# ── Entity search + derived-signal panels (doc/PROCUREMENT_NUGGETS.md, 2026-06-11) ────
# Factual structure signals; the page carries each one's no-inference caveat.
@st.cache_data(ttl=600)
def fetch_entity_search_result() -> QueryResult:
    """Unified typeahead corpus (suppliers + authorities + CPV) for the search-first hero.
    The page filters by name display-only, same pattern as the supplier search."""
    return _q.entity_search(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_supplier_single_bid_result(join_norm: str) -> QueryResult:
    """One firm's lot-level single-bid context (TED 2024+, sole-winner notices only)."""
    return _q.supplier_single_bid(get_procurement_conn(), join_norm)


@st.cache_data(ttl=600)
def fetch_single_bid_baseline_result() -> QueryResult:
    """The national lot-level single-bid baseline (one row) for the competition blocks."""
    return _q.single_bid_baseline(get_procurement_conn())


@st.cache_data(ttl=600)
def fetch_competition_by_cpv_result(min_lots: int = 100) -> QueryResult:
    """Lot-level single-bid rate per CPV division (the market-spread panel)."""
    return _q.competition_by_cpv(get_procurement_conn(), min_lots=min_lots)


@st.cache_data(ttl=600)
def fetch_single_bid_notices_for_cpv_result(cpv_division: str) -> QueryResult:
    """The individual single-bid award notices within one CPV division (the drill-down behind a
    single-bid market card), each linking to its authoritative EU Official Journal notice."""
    return _q.single_bid_notices_for_cpv(get_procurement_conn(), cpv_division)


@st.cache_data(ttl=600)
def fetch_new_entrants_result() -> QueryResult:
    """First-time-winner share of awards per year (left-censored years flagged)."""
    return _q.new_entrants_by_year(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_incumbency_for_supplier_result(supplier_norm: str) -> QueryResult:
    """One firm's top buyers with distinct-years spans (the relationships block)."""
    return _q.incumbency_for_supplier(get_procurement_conn(), supplier_norm)


@st.cache_data(ttl=600)
def fetch_incumbency_top_result(min_years: int = 6, limit: int = 24) -> QueryResult:
    """Longest-running supplier×buyer award relationships (the patterns panel)."""
    return _q.incumbency_top(get_procurement_conn(), min_years=min_years, limit=limit)


@st.cache_data(ttl=300)
def fetch_dependency_for_supplier_result(supplier_norm: str) -> QueryResult:
    """One firm's top-buyer share (present only when the firm has ≥5 awards)."""
    return _q.dependency_for_supplier(get_procurement_conn(), supplier_norm)


@st.cache_data(ttl=600)
def fetch_dependency_top_result(min_awards: int = 10, min_share_pct: float = 80.0, limit: int = 24) -> QueryResult:
    """Suppliers winning ≥80% of their awards from one buyer (central purchasing excluded)."""
    return _q.dependency_top(get_procurement_conn(), min_awards=min_awards, min_share_pct=min_share_pct, limit=limit)


@st.cache_data(ttl=600)
def fetch_quarter_totals_result() -> QueryResult:
    """Corpus-wide COMMITTED lines + sum-safe euros per quarter (the Q4-spike headline)."""
    return _q.quarter_totals(get_procurement_conn())


@st.cache_data(ttl=600)
def fetch_quarter_profile_top_result(quarter: int = 4, limit: int = 12) -> QueryResult:
    """Publishers most skewed toward one quarter, by share of their own COMMITTED lines."""
    return _q.quarter_profile_top(get_procurement_conn(), quarter=quarter, limit=limit)


@st.cache_data(ttl=600)
def fetch_sector_breadth_top_result(min_sectors: int = 5, limit: int = 12) -> QueryResult:
    """Paid suppliers reaching the most public-service sectors (collision-guarded)."""
    return _q.sector_breadth_top(get_procurement_conn(), min_sectors=min_sectors, limit=limit)


@st.cache_data(ttl=300)
def fetch_call_offs_for_supplier_result(supplier_norm: str) -> QueryResult:
    """One firm's call-off awards with the parent framework resolved where in-corpus."""
    return _q.call_offs_for_supplier(get_procurement_conn(), supplier_norm)
