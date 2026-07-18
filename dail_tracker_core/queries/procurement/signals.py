"""Procurement retrieval — cross-register + derived competition signals (single-bid, incumbency, dependency, overlaps).

Split from the single 1,6xx-line queries/procurement.py by MONEY GRAIN
(2026-07-18) so the never-sum boundaries are module boundaries. Import surface is
unchanged: ``from dail_tracker_core.queries import procurement`` re-exports every
function; grain-shared constants live in ``._shared``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("procurement", _log)

def epa_compliance_for_supplier(conn: duckdb.DuckDBPyConnection, company_num: int) -> QueryResult:
    """One CRO company's EPA environmental-licence + enforcement record for the dossier panel,
    matched on CRO ``company_num``. Returns the single summary row, or empty if the firm holds no EPA
    licence. Licence portfolio + compliance counts only (a separate public register) — carries no money
    and is never summed with the firm's award/payment figures."""
    return _run(
        conn,
        "SELECT * FROM v_procurement_epa_compliance WHERE company_num = ?",
        [company_num],
    )


def epa_supplier_index(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """All CRO companies that hold at least one EPA environmental licence, keyed on
    ``company_num`` — the discovery index behind the Companies landing's EPA badge/filter.
    Retrieval only (the n_* counts are pre-computed in the view); the page does a
    display-only membership lookup against supplier rows by company_num."""
    return _run(
        conn,
        "SELECT company_num, n_licences, n_enforcement_events FROM v_procurement_epa_compliance WHERE n_licences > 0",
    )


def eu_tam_state_aid(conn: duckdb.DuckDBPyConnection, *, limit: int | None = None) -> QueryResult:
    """EU State-Aid Transparency awards to Irish beneficiaries — the grant/subsidy register
    (IDA, Enterprise Ireland, DAFM…), one row per disclosed award, biggest first.

    Ranks on ``aid_element_value`` (the actual subsidy value — ~86% filled), NOT
    ``nominal_amount_value`` (~73% null, register-blank) — see the EU-TAM aid-element gotcha.
    These are subsidies/grants, a DIFFERENT instrument from contract awards — never summed with
    eTenders/TED values. value_safe_to_sum is False on the view (mixed bases), so the page lists
    named awards and never totals them."""
    sql = (
        "SELECT beneficiary_name, beneficiary_type, cro_company_num, aid_measure_title,"
        " granting_authority, aid_instrument, objective, sector_nace,"
        " aid_element_value, nominal_amount_value, date_granted, award_detail_url,"
        " aid_element_suspect_scheme_total"
        " FROM v_procurement_eu_tam_state_aid"
        " ORDER BY aid_element_value DESC NULLS LAST, date_granted DESC NULLS LAST"
    )
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def eu_tam_state_aid_count(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """How many real per-beneficiary State-Aid awards exist (excluding the scheme-total
    artefacts the view flags), so a top-N listing can disclose the full corpus size instead
    of silently truncating."""
    return _run(
        conn,
        "SELECT COUNT(*) AS n_awards FROM v_procurement_eu_tam_state_aid"
        " WHERE NOT COALESCE(aid_element_suspect_scheme_total, false)",
    )


def lobbying_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _run(
        conn,
        "SELECT lobby_name, lobby_side, supplier, supplier_norm, n_lobby_returns,"
        " n_award_rows, n_authorities, awarded_value_safe_eur"
        " FROM v_procurement_lobbying_overlap ORDER BY n_award_rows DESC",
    )


def charity_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Registered charities that ALSO appear on the procurement award register,
    linked by a shared CRO company number (a hard identifier — the charity's
    declared cro_number equals the supplier's CRO match). Co-occurrence disclosure
    only: the same legal entity is on both registers — NOT a claim about either.
    gov_funded_share_latest (0–1) is the charity's own latest-return figure, shown
    as context. awarded_value_safe_eur is already the money-grain-safe sum (ceiling
    notices excluded) inside the view — display only, never re-aggregated here."""
    return _run(
        conn,
        "SELECT rcn, registered_charity_name, company_num, company_status,"
        " charity_classification, state_adjacent_flag, funding_profile,"
        " gov_funded_share_latest, gross_income_latest_eur,"
        " supplier_norm, matched_supplier_name, n_awards, n_authorities,"
        " awarded_value_safe_eur, n_value_safe_awards, n_ceiling_notices"
        " FROM v_procurement_charity_overlap ORDER BY awarded_value_safe_eur DESC, n_awards DESC",
    )


# ── Entity search + derived-signal views (doc/PROCUREMENT_NUGGETS.md, 2026-06-11) ────────
# All factual structure signals — the consuming page must keep the no-inference posture
# (every view header states its caveat family). Pre-aggregated in the views; retrieval only.
def entity_search(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The unified typeahead corpus (awards suppliers/authorities/CPV categories AND realised-
    payments contractors/bodies in one list) for the page's search-first hero. The page applies a
    DISPLAY-ONLY name filter over this frame (same pattern as the supplier search). The two money
    hint columns are different grains (award ceilings vs realised payments) — shown with their own
    labels, never added. paid_tier (SPENT/COMMITTED) rides the paid_* rows so the page builds the
    tier-correct paid-dossier deep-link; it is NULL for the award kinds."""
    return _run(
        conn,
        "SELECT entity_kind, display_name, url_key, n_records, n_counterparties,"
        " awarded_value_safe_eur, paid_safe_eur, cro_company_num, on_lobbying_register, paid_tier"
        " FROM v_procurement_entity_search",
    )


def supplier_single_bid(conn: duckdb.DuckDBPyConnection, join_norm: str) -> QueryResult:
    """One firm's lot-level single-bid context (TED 2024+, sole-winner notices only) for the
    competition block on a company dossier. Factual signal, never a verdict — the page must
    carry the 'often wholly legitimate' caveat. Matched on the TED winner join norm (the same
    key as ted_for_supplier)."""
    return _run(
        conn,
        "SELECT * FROM v_procurement_supplier_single_bid WHERE winner_join_norm = ?",
        [join_norm],
    )


def single_bid_baseline(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The national lot-level single-bid baseline (one row) the per-firm/per-market rates are
    read against. Computed over the by-CPV view so the denominator matches the market table."""
    return _run(
        conn,
        "SELECT COALESCE(SUM(n_lots_with_bidcount), 0) AS n_lots_with_bidcount,"
        " COALESCE(SUM(n_single_bid_lots), 0) AS n_single_bid_lots,"
        " ROUND(100.0 * SUM(n_single_bid_lots) / NULLIF(SUM(n_lots_with_bidcount), 0), 1)"
        "   AS single_bid_lot_pct"
        " FROM v_procurement_competition_by_cpv",
    )


def competition_by_cpv(conn: duckdb.DuckDBPyConnection, *, min_lots: int = 100) -> QueryResult:
    """Lot-level single-bid rate per CPV division (market), highest first — the market-spread
    panel. ``min_lots`` drops small noisy markets."""
    return _run(
        conn,
        "SELECT cpv_division, n_notices, n_lots_with_bidcount, n_single_bid_lots,"
        " single_bid_lot_pct, n_uncompetitive_notices, n_buyers, first_year, last_year"
        " FROM v_procurement_competition_by_cpv WHERE n_lots_with_bidcount >= ?"
        " ORDER BY single_bid_lot_pct DESC NULLS LAST",
        [int(min_lots)],
    )


def single_bid_notices_for_cpv(conn: duckdb.DuckDBPyConnection, cpv_division: str, *, limit: int = 80) -> QueryResult:
    """The individual single-bid award NOTICES within one CPV division (market) — the drill-down
    behind a single-bid market card. Each row opens the authoritative EU Official Journal notice
    (notice_url). Restricted to source_lane='api' (the single-bid field only exists for 2024+
    eForms, matching the market panel's denominator) and pan-EU outliers excluded. DISTINCT by
    publication number so a notice counts once. A single bid is a recorded fact, often wholly
    legitimate — the page carries that caveat; this query only surfaces the notices, never a verdict."""
    return _run(
        conn,
        "SELECT DISTINCT publication_number, notice_url, buyer_name, winner_name,"
        " dispatch_date, year, n_tenders_received, value_kind, cpv_code"
        " FROM v_procurement_ted_winner_history"
        " WHERE cpv_division = ? AND is_single_bid AND NOT is_pan_eu_outlier AND source_lane = 'api'"
        " ORDER BY dispatch_date DESC NULLS LAST"
        " LIMIT ?",
        [cpv_division, int(limit)],
    )


def new_entrants_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Share of eTenders awards going to first-time suppliers, per year. Years before 2016
    are flagged is_left_censored (the corpus starts 2013, so 'first award' is first IN THE
    CORPUS) — the page must de-emphasise or exclude them."""
    return _run(
        conn,
        "SELECT year, n_awards, n_suppliers, n_new_suppliers, n_awards_to_new,"
        " pct_awards_to_new_entrants, is_left_censored"
        " FROM v_procurement_new_entrants ORDER BY year",
    )


def incumbency_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, limit: int = 8) -> QueryResult:
    """One firm's repeat-winner relationships (its top buyers by award count, with the
    distinct-years span) for the dossier's relationships block. Central-purchasing rows are
    flagged so the page can badge framework mechanics vs bilateral relationships."""
    return _run(
        conn,
        "SELECT contracting_authority, authority_is_central_purchasing, n_awards,"
        " n_distinct_years, first_year, last_year, awarded_value_safe_eur"
        " FROM v_procurement_incumbency WHERE supplier_norm = ?"
        " ORDER BY n_awards DESC LIMIT ?",
        [supplier_norm, int(limit)],
    )


def incumbency_top(conn: duckdb.DuckDBPyConnection, *, min_years: int = 6, limit: int = 24) -> QueryResult:
    """The longest-running supplier×buyer award relationships (≥min_years distinct years),
    for the patterns panel. A long streak is a structure fact, not a finding — the page
    carries the caveat and the central-purchasing badge."""
    return _run(
        conn,
        "SELECT supplier, supplier_norm, contracting_authority, authority_is_central_purchasing,"
        " n_awards, n_distinct_years, first_year, last_year"
        " FROM v_procurement_incumbency WHERE n_distinct_years >= ?"
        " ORDER BY n_distinct_years DESC, n_awards DESC LIMIT ?",
        [int(min_years), int(limit)],
    )


def dependency_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's top-buyer dependency share (only present when the firm has ≥5 awards) for
    the dossier's relationships block. 'X won N of its M awards from Y' — a structure fact,
    no concentration-risk language."""
    return _run(
        conn,
        "SELECT top_authority, top_authority_is_central_purchasing, awards_from_top_authority,"
        " total_awards, n_authorities, top_authority_share_pct"
        " FROM v_procurement_supplier_dependency WHERE supplier_norm = ?",
        [supplier_norm],
    )


def dependency_top(
    conn: duckdb.DuckDBPyConnection, *, min_awards: int = 10, min_share_pct: float = 80.0, limit: int = 24
) -> QueryResult:
    """Suppliers winning ≥min_share_pct of their ≥min_awards awards from one buyer, for the
    patterns panel. Central-purchasing rows excluded (an OGP top-buyer is framework
    mechanics, not a bilateral relationship — the panel would otherwise be all OGP)."""
    return _run(
        conn,
        "SELECT supplier, supplier_norm, top_authority, awards_from_top_authority,"
        " total_awards, n_authorities, top_authority_share_pct"
        " FROM v_procurement_supplier_dependency"
        " WHERE total_awards >= ? AND top_authority_share_pct >= ?"
        "   AND NOT top_authority_is_central_purchasing"
        " ORDER BY top_authority_share_pct DESC, total_awards DESC LIMIT ?",
        [int(min_awards), float(min_share_pct), int(limit)],
    )


def quarter_profile_top(conn: duckdb.DuckDBPyConnection, *, quarter: int = 4, limit: int = 12) -> QueryResult:
    """Publishers most skewed toward one quarter (default Q4 — the year-end ordering spike),
    by share of their own COMMITTED lines. Seasonality is a shape fact, never a motive."""
    return _run(
        conn,
        "SELECT publisher_name, quarter, n_lines, committed_safe_eur, pct_of_publisher_lines"
        " FROM v_procurement_quarter_profile WHERE quarter = ?"
        " ORDER BY pct_of_publisher_lines DESC LIMIT ?",
        [int(quarter), int(limit)],
    )


def quarter_totals(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Corpus-wide COMMITTED line counts + sum-safe euros per quarter (the Q4-spike headline).
    Aggregated over the per-publisher view so the denominators match the skew panel."""
    return _run(
        conn,
        "SELECT quarter, SUM(n_lines) AS n_lines,"
        " COALESCE(SUM(committed_safe_eur), 0) AS committed_safe_eur"
        " FROM v_procurement_quarter_profile GROUP BY quarter ORDER BY quarter",
    )


def sector_breadth_top(conn: duckdb.DuckDBPyConnection, *, min_sectors: int = 5, limit: int = 12) -> QueryResult:
    """Paid suppliers reaching the most public-service SECTORS, for the patterns panel.
    Name-collision guard: a short generic normalised name aggregating many distinct raw
    names (e.g. 'ELECTRIC') is many firms, not one — those buckets are excluded here
    (≥3 raw variants behind a single-token name), and n_raw_variants rides along so the
    page can disclose the grouping basis."""
    return _run(
        conn,
        "SELECT supplier_normalised, n_raw_variants, n_sectors, n_publishers, n_lines,"
        " paid_safe_eur, committed_safe_eur, vat_mixed, sectors, first_year, last_year"
        " FROM v_procurement_supplier_sector_breadth"
        " WHERE n_sectors >= ?"
        "   AND NOT (n_raw_variants >= 3 AND NOT contains(trim(supplier_normalised), ' '))"
        " ORDER BY n_sectors DESC, paid_safe_eur DESC NULLS LAST LIMIT ?",
        [int(min_sectors), int(limit)],
    )


def call_offs_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, limit: int = 25) -> QueryResult:
    """One firm's call-off awards (drawdowns under a framework/DPS) with the parent
    agreement resolved where it exists in the corpus — the framework nesting, made visible.
    An unresolved parent is itself a transparency fact ('parent not in the published
    corpus'), never hidden. The parent ceiling is context, never added to the call-off."""
    return _run(
        conn,
        "SELECT tender_id, contracting_authority, award_date, value_eur, value_kind,"
        " value_safe_to_sum, parent_agreement_id, parent_in_corpus, parent_authority,"
        " parent_value_eur, parent_value_kind, parent_n_suppliers"
        " FROM v_procurement_call_off_links WHERE supplier_norm = ?"
        " ORDER BY award_date DESC NULLS LAST LIMIT ?",
        [supplier_norm, int(limit)],
    )
