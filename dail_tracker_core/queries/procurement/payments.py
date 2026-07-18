"""Procurement retrieval — public-body PAYMENTS/PO lines — the SPENT/COMMITTED tiers (realised money; never summed with award ceilings).

Split from the single 1,6xx-line queries/procurement.py by MONEY GRAIN
(2026-07-18) so the never-sum boundaries are module boundaries. Import surface is
unchanged: ``from dail_tracker_core.queries import procurement`` re-exports every
function; grain-shared constants live in ``._shared``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.queries.procurement._shared import _PAYMENT_TIERS, _PAYMENTS_REAL_TIERS
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("procurement", _log)

def payments_real_by_year(conn: duckdb.DuckDBPyConnection, *, tier: str | None = None) -> QueryResult:
    """Annual public-spend totals, nominal vs real (the GOVERNMENT-CONSUMPTION deflator — the
    agency-standard index for public money, not CPI), from ``v_procurement_payments_real_by_year``.
    The grain is year × realisation_tier × vat_status, so SPENT and COMMITTED (and differing VAT
    bases) are NEVER summed together. ``tier`` optionally scopes to one realisation tier
    (whitelisted: an unrecognised value is ignored and all tiers are returned)."""
    sql = (
        "SELECT year, realisation_tier, vat_status, n_lines, total_nominal_eur, total_real_eur,"
        " n_real_excluded, real_base_year, deflator_index"
        " FROM v_procurement_payments_real_by_year"
    )
    params: list = []
    if tier in _PAYMENTS_REAL_TIERS:
        sql += " WHERE realisation_tier = ?"
        params.append(tier)
    sql += " ORDER BY year, realisation_tier, vat_status"
    return _run(conn, sql, params)


def payments_real_trend(conn: duckdb.DuckDBPyConnection, *, tier: str = "SPENT") -> QueryResult:
    """Per-year public-spend total, nominal vs real (government-consumption deflator), for the
    real-terms TREND chart, from ``v_procurement_payments_real_trend``. Year-level INDICATIVE
    FLOOR (VAT combined — the same basis as the corpus 'at least €X' headline), one tier only
    (SPENT/COMMITTED never blended). ``real_uplift_pct`` is the pure inflation uplift on the
    adjustable rows; ``n_unadjustable_lines`` flags years the deflator can't yet reach (2025+).
    All aggregation is in the view; this only scopes to a tier and orders chronologically."""
    t = tier if tier in _PAYMENTS_REAL_TIERS else "SPENT"  # whitelist — no raw string to SQL
    return _run(
        conn,
        "SELECT year, realisation_tier, total_nominal_eur, total_nominal_adjustable_eur,"
        " total_real_eur, real_uplift_pct, n_unadjustable_lines, real_base_year, deflator_index"
        " FROM v_procurement_payments_real_trend WHERE realisation_tier = ? ORDER BY year",
        [t],
    )


def supplier_payments_by_year(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's public-body PAYMENTS RECEIVED per year — the supplier-side mirror of
    ``payments_by_year`` (which is per publisher). Paid (SPENT) and ordered (COMMITTED) come back
    as SEPARATE sum-safe columns so the consumer charts them on their own axes.

    ⚠️ Three never-cross rules ride on the column split: paid and ordered are different lifecycle
    stages (never added to each other, never stacked — that reads as a sum), and BOTH are a
    different grain from the award totals (realised/committed spend vs an award ceiling — never
    added to the awards trend either). Indicative floor only: amounts span mixed VAT bases.
    Sum-safe euro only (public-body transfers already excluded upstream). Oldest year first."""
    return _run(
        conn,
        "SELECT year,"
        " COALESCE(SUM(amount_spent_safe_eur), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(amount_committed_safe_eur), 0)"
        "   AS ordered_safe_eur,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines"
        " FROM v_procurement_payments"
        " WHERE supplier_normalised = ? AND year IS NOT NULL"
        " GROUP BY year ORDER BY year",
        [supplier_norm],
    )


def _tier(tier: str) -> str:
    return _PAYMENT_TIERS.get((tier or "").upper(), "SPENT")


def payments_corpus_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row summary of the public-body payment corpus for the section headline + source-state
    gate: distinct publishers/suppliers, year span, and the sum-safe total for EACH tier shown
    separately (paid vs ordered are never added). Totals span mixed vat_status, so the page must
    label them indicative floors, not audited totals."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_payments,"
        "  COUNT(DISTINCT publisher_name) AS n_publishers,"
        "  COUNT(DISTINCT supplier_normalised) AS n_suppliers,"
        "  MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        "  COALESCE(SUM(amount_spent_safe_eur), 0)"
        "    AS spent_safe_eur,"
        "  COALESCE(SUM(amount_committed_safe_eur), 0)"
        "    AS committed_safe_eur"
        " FROM v_procurement_payments",
    )


def payments_publisher_summary(
    conn: duckdb.DuckDBPyConnection, *, tier: str = "SPENT", limit: int | None = 60
) -> QueryResult:
    """Public bodies ranked by sum-safe amount for one lifecycle tier (paid / ordered)."""
    sql = (
        "SELECT * FROM v_procurement_payments_publisher_summary WHERE realisation_tier = ? ORDER BY total_safe_eur DESC"
    )
    params: list = [_tier(tier)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def payments_supplier_summary(
    conn: duckdb.DuckDBPyConnection, *, tier: str = "SPENT", limit: int | None = 60
) -> QueryResult:
    """Suppliers ranked by sum-safe amount the State paid (SPENT) or ordered (COMMITTED)."""
    sql = (
        "SELECT * FROM v_procurement_payments_supplier_summary WHERE realisation_tier = ? ORDER BY total_safe_eur DESC"
    )
    params: list = [_tier(tier)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def payments_for_publisher(
    conn: duckdb.DuckDBPyConnection, publisher_name: str, *, tier: str = "SPENT", limit: int = 200
) -> QueryResult:
    """Top suppliers paid/ordered by one public body (drill-down), sum-safe within that body."""
    return _run(
        conn,
        "SELECT mode(supplier) AS supplier, supplier_normalised, supplier_class,"
        " COUNT(*) AS n_payments, MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur,"
        " mode(cro_company_num) AS cro_company_num"
        " FROM v_procurement_payments WHERE publisher_name = ? AND realisation_tier = ?"
        " GROUP BY supplier_normalised, supplier_class"
        " ORDER BY total_safe_eur DESC LIMIT ?",
        [publisher_name, _tier(tier), int(limit)],
    )


def payments_by_year(conn: duckdb.DuckDBPyConnection, publisher_name: str, *, tier: str = "SPENT") -> QueryResult:
    """One public body's sum-safe spend per calendar year, for ONE lifecycle tier (the body
    dossier's spend-over-time spine — now meaningful with the 2016–2026 council backfill).
    One tier only by design: ordered and paid are never charted on one stacked axis (that would
    read as a sum). Counts + euros pre-aggregated here; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, COUNT(*) AS n_payments,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_procurement_payments"
        " WHERE publisher_name = ? AND realisation_tier = ? AND year IS NOT NULL"
        " GROUP BY year ORDER BY year",
        [publisher_name, _tier(tier)],
    )


def council_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Publishing local authorities for the "Your council" index — one row per council,
    pre-grouped North->South by province (province_order) then by scale within province.

    Carries BOTH lifecycle totals as separate columns (ordered_safe_eur / paid_safe_eur)
    so the page renders one labelled pill per council WITHOUT pivoting or summing — paid
    (SPENT) and ordered (COMMITTED) are different stages and never added. The view already
    orders the rows; the page selects and renders, computing nothing."""
    return _run(conn, "SELECT * FROM v_procurement_council_summary")


def payments_publisher_profile(conn: duckdb.DuckDBPyConnection, publisher_name: str) -> QueryResult:
    """Single-row buyer dossier header for one public body (the per-council profile anchor).

    Carries BOTH lifecycle tiers side by side so the page can show "€X ordered" and "€Y paid"
    without ever summing them (they are different stages of public money). Also returns
    publisher_type so the page can badge a local authority, and the supplier/line/year spans.
    Sum-safe euro only (public_body transfers already excluded upstream by value_safe_to_sum)."""
    return _run(
        conn,
        "SELECT mode(publisher_name) AS publisher_name, mode(publisher_type) AS publisher_type,"
        " mode(sector) AS sector,"
        " COUNT(DISTINCT supplier_normalised) AS n_suppliers,"
        " MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines,"
        " COALESCE(SUM(amount_spent_safe_eur), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(amount_committed_safe_eur), 0)"
        "   AS ordered_safe_eur"
        " FROM v_procurement_payments WHERE publisher_name = ?",
        [publisher_name],
    )


def payments_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's public-body payment footprint for the cross-reference on an eTenders supplier
    profile: paid (SPENT) and ordered (COMMITTED) totals + publisher count. Indicative floor
    (mixed vat_status); never summed with the firm's award totals."""
    return _run(
        conn,
        "SELECT realisation_tier, COUNT(*) AS n_payments,"
        " COUNT(DISTINCT publisher_name) AS n_publishers,"
        " MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " (COUNT(DISTINCT vat_status) > 1) AS vat_mixed,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_procurement_payments WHERE supplier_normalised = ?"
        " GROUP BY realisation_tier ORDER BY total_safe_eur DESC",
        [supplier_norm],
    )


def payments_supplier_header(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """Single-row header for a paid-supplier drill-down (the mirror of
    ``payments_publisher_profile``): the firm's published display name, both lifecycle
    tiers' sum-safe totals side by side (never summed), its distinct-publisher / line / year
    spans and its CRO match. Sum-safe euro only; mixed vat_status flagged so the page can
    label the totals indicative floors."""
    return _run(
        conn,
        "SELECT mode(supplier) AS supplier, supplier_normalised, mode(supplier_class) AS supplier_class,"
        " COUNT(DISTINCT publisher_name) AS n_publishers,"
        " MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines,"
        " COALESCE(SUM(amount_spent_safe_eur), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(amount_committed_safe_eur), 0)"
        "   AS ordered_safe_eur,"
        " (COUNT(DISTINCT vat_status) > 1) AS vat_mixed,"
        " mode(cro_company_num) AS cro_company_num, mode(cro_company_status) AS cro_company_status"
        " FROM v_procurement_payments WHERE supplier_normalised = ?"
        " GROUP BY supplier_normalised",
        [supplier_norm],
    )


def payments_publishers_for_supplier(
    conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, tier: str = "SPENT", limit: int = 200
) -> QueryResult:
    """The public bodies that paid (SPENT) or ordered (COMMITTED) from one supplier — the
    drill-down line items behind a paid-supplier card, and the exact mirror of
    ``payments_for_publisher`` (which lists a body's suppliers). Sum-safe within each body;
    bodies named per their own published lists. One row per publisher, biggest first."""
    return _run(
        conn,
        "SELECT publisher_name, mode(publisher_type) AS publisher_type, mode(sector) AS sector,"
        " COUNT(*) AS n_payments, MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_procurement_payments WHERE supplier_normalised = ? AND realisation_tier = ?"
        " GROUP BY publisher_name ORDER BY total_safe_eur DESC LIMIT ?",
        [supplier_norm, _tier(tier), int(limit)],
    )


def payment_lines_for_pair(
    conn: duckdb.DuckDBPyConnection, supplier_norm: str, publisher_name: str, *, tier: str = "SPENT", limit: int = 500
) -> QueryResult:
    """The actual published payment LINE ITEMS for one (supplier × public body × tier) pair —
    the LEAF of the payments drill-down. The supplier↔body cards above are aggregates that
    link to each other; this is the terminus that finally shows the individual records a body
    published (period, description, PO number, amount), with a link to the body's own source
    file. One row per published line, biggest first. Sum-safe euro flag rides along so the page
    can mark a line that is not safe to total; never summed across vat_status. paid_status carries
    the body's own per-line payment status (Paid / Part paid / Not paid) where it published one —
    canonicalised in the view from a strict allowlist; NULL for the majority that publish none.

    ``recurring_years`` counts the distinct years in which this body published an IDENTICAL amount
    to this firm — the signature of a recurring availability / unitary charge (e.g. a PPP annual
    payment), as opposed to distinct one-off purchases. ≥2 means the same figure repeats yearly, so
    the page can flag it as not meaningful to sum. Computed over all the pair's lines (pre-LIMIT)."""
    return _run(
        conn,
        "SELECT period, year, description, po_number, amount_eur, value_kind,"
        " value_safe_to_sum, vat_status, paid_status, source_file_url,"
        " COUNT(DISTINCT year) OVER (PARTITION BY round(amount_eur, 2)) AS recurring_years"
        " FROM v_procurement_payments"
        " WHERE supplier_normalised = ? AND publisher_name = ? AND realisation_tier = ?"
        " ORDER BY amount_eur DESC NULLS LAST LIMIT ?",
        [supplier_norm, publisher_name, _tier(tier), int(limit)],
    )


def payment_lines_for_supplier(
    conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, tier: str = "SPENT", limit: int = 500
) -> QueryResult:
    """Every published payment LINE ITEM for ONE supplier across ALL public bodies in one
    lifecycle tier — the 'what comprised this figure' leaf for a corporate-group member card.
    A group member aggregates a firm over several bodies, so its headline has no single body to
    drill into; this lists the constituent records directly, each carrying its paying body
    (period, description, PO number, amount, source). One row per line, biggest first. Sum-safe
    flag rides along; never summed across vat_status. Mirrors ``payment_lines_for_pair`` but with
    no publisher filter and the body name selected so the page can label each line.

    ``recurring_years`` (see ``payment_lines_for_pair``) is partitioned by body here too — the same
    amount from the SAME body across ≥2 years marks a recurring availability/unitary charge, so the
    page can flag PPP-style repeating lines that must not be read as distinct spend."""
    return _run(
        conn,
        "SELECT publisher_name, period, year, description, po_number, amount_eur, value_kind,"
        " value_safe_to_sum, vat_status, paid_status, source_file_url,"
        " COUNT(DISTINCT year) OVER (PARTITION BY publisher_name, round(amount_eur, 2)) AS recurring_years"
        " FROM v_procurement_payments"
        " WHERE supplier_normalised = ? AND realisation_tier = ?"
        " ORDER BY amount_eur DESC NULLS LAST LIMIT ?",
        [supplier_norm, _tier(tier), int(limit)],
    )


# ── Corporate-group rollup (Follow-the-money "BAM" node) ────────────────────────────────
# A curated group (v_procurement_supplier_groups) gathers a parent's many published payment
# entities — operating companies, PPP special-purpose vehicles, joint ventures — under one node.
# Aggregation lives here (the page renders only); the join key is the uppercase supplier_normalised.
def payment_group_header(conn: duckdb.DuckDBPyConnection, group_slug: str) -> QueryResult:
    """Single-row header for a corporate-group node: the group's structure (how many legal
    entities, how many are PPP SPVs / JVs, how many carry no CRO) and BOTH lifecycle tiers'
    sum-safe totals side by side — NEVER summed across tiers, and an indicative FLOOR within a
    tier (members' euros may mix VAT bases across the bodies that paid them; vat_mixed flags it)."""
    return _run(
        conn,
        "SELECT any_value(g.group_label) AS group_label,"
        " COUNT(DISTINCT p.supplier_normalised) AS n_entities,"
        " COUNT(DISTINCT p.supplier_normalised) FILTER (WHERE g.entity_kind = 'ppp_spv') AS n_ppp_spv,"
        " COUNT(DISTINCT p.supplier_normalised) FILTER (WHERE g.entity_kind = 'jv')      AS n_jv,"
        " COUNT(DISTINCT p.supplier_normalised) FILTER (WHERE g.cro_company_num IS NULL) AS n_no_cro,"
        " COUNT(DISTINCT p.publisher_name) AS n_publishers,"
        " MIN(p.year)::INT AS min_year, MAX(p.year)::INT AS max_year,"
        " COUNT(*) FILTER (WHERE p.realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE p.realisation_tier = 'COMMITTED') AS n_ordered_lines,"
        " COALESCE(SUM(p.amount_spent_safe_eur), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(p.amount_committed_safe_eur), 0)"
        "   AS ordered_safe_eur,"
        " (COUNT(DISTINCT p.vat_status) > 1) AS vat_mixed"
        " FROM v_procurement_payments p"
        " JOIN v_procurement_supplier_groups g"
        "   ON upper(trim(p.supplier_normalised)) = g.supplier_normalised"
        " WHERE g.group_slug = ?",
        [group_slug],
    )


def payment_group_members(conn: duckdb.DuckDBPyConnection, group_slug: str, *, tier: str = "SPENT") -> QueryResult:
    """The member entities of a corporate group in one lifecycle tier, biggest first — each a
    row the Follow-the-money node renders as a card that drills into that entity's own
    paid-supplier profile (?paid_supplier=). entity_kind/note ride along so the card can badge a
    PPP SPV or JV. Sum-safe within each member; ordered/paid never blended (one tier per call).

    One row per entity: grouped on supplier_normalised over the payment feed (NOT the per-class
    supplier summary), so an entity whose lines split across supplier_class — e.g. a bundle SPV
    classed partly company, partly individual — collapses to a single card with its majority
    class (mode), the same drill key the supplier node resolves."""
    return _run(
        conn,
        "SELECT mode(p.supplier) AS supplier, p.supplier_normalised,"
        " mode(p.supplier_class) AS supplier_class,"
        " COUNT(*) AS n_payments, COUNT(DISTINCT p.publisher_name) AS n_publishers,"
        " MIN(p.year)::INT AS min_year, MAX(p.year)::INT AS max_year,"
        " COALESCE(SUM(p.amount_eur) FILTER (WHERE p.value_safe_to_sum), 0) AS total_safe_eur,"
        " (COUNT(DISTINCT p.vat_status) > 1) AS vat_mixed,"
        " mode(p.cro_company_num) AS cro_company_num, mode(p.cro_company_status) AS cro_company_status,"
        " any_value(g.entity_kind) AS entity_kind, any_value(g.note) AS note"
        " FROM v_procurement_payments p"
        " JOIN v_procurement_supplier_groups g"
        "   ON upper(trim(p.supplier_normalised)) = g.supplier_normalised"
        " WHERE g.group_slug = ? AND p.realisation_tier = ?"
        " GROUP BY p.supplier_normalised"
        " ORDER BY total_safe_eur DESC",
        [group_slug, _tier(tier)],
    )


def entity_chain_for_company(conn: duckdb.DuckDBPyConnection, company_num: str) -> QueryResult:
    """One CRO-matched firm's cross-register footprint: which of the three public-money
    registers it appears in (eTenders awards, TED awards, public-body payments) and each
    register's own headline number, side by side. Hard CRO company-number match only.

    ⚠️ The money columns are DIFFERENT GRAINS (award ceilings vs realised payments) and the
    page MUST label them separately and NEVER sum them. Absence from a register is coverage,
    not missing money (only ~7% of State spend is in the payments corpus). Single-row select
    over the pre-joined view — the page renders, never computes."""
    return _run(
        conn,
        "SELECT company_num, display_name, in_etenders, in_ted, in_payments, n_registers,"
        " etenders_award_rows, etenders_n_authorities, etenders_awarded_value_safe_eur,"
        " ted_awards, ted_n_buyers, ted_value_safe_eur,"
        " payment_lines, payments_n_publishers, paid_safe_eur, committed_safe_eur"
        " FROM v_procurement_entity_chain WHERE company_num = ?",
        [company_num],
    )
